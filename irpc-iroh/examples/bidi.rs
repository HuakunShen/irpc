//! Bidirectional RPC over a single iroh connection.
//!
//! In the usual irpc setup one peer is the client (it opens streams and sends requests) and the
//! other is the server (it accepts streams and answers them). A QUIC connection is symmetric,
//! though: either peer can open a bidirectional stream at any time. This example uses that to let
//! *both* peers act as client and server at once, over one shared connection.
//!
//! Two independent protocols run in opposite directions:
//!
//! - [`proto::ClientToServer`]: the connecting peer asks the listening peer to `Get`/`Set` entries
//!   in a key-value store.
//! - [`proto::ServerToClient`]: the listening peer periodically `Ping`s each connected peer to
//!   check liveness.
//!
//! The trick is that each peer holds *both* an [`irpc::Client`] (to open streams and send its own
//! requests) *and* a [`irpc_iroh::read_request`] loop (to accept and answer the other peer's
//! requests). Both are built from the same [`iroh::endpoint::Connection`]. Because `accept_bi`
//! only ever yields streams opened by the *remote* side, the two directions never collide: the
//! listener's accept loop only sees `ClientToServer` streams, the connector's accept loop only
//! sees `ServerToClient` streams.
//!
//! Run the listener, note its endpoint id, then drive it from a second process:
//!
//! ```text
//! cargo run --example bidi -- listen
//! cargo run --example bidi -- connect <ENDPOINT_ID> set foo bar
//! cargo run --example bidi -- connect <ENDPOINT_ID> get foo
//! ```

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    cli::run().await
}

/// The protocol definitions and the peer implementations for both roles.
mod proto {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use anyhow::Result;
    use iroh::{
        Endpoint, EndpointId,
        endpoint::{Connection, presets},
        protocol::{AcceptError, ProtocolHandler, Router},
    };
    use irpc::{Client, WithChannels, channel::oneshot, rpc_requests};
    use irpc_iroh::{IrohRemoteConnection, read_request};
    use n0_future::task::AbortOnDropHandle;
    use serde::{Deserialize, Serialize};
    use tokio::time::Instant;
    use tracing::{info, warn};

    /// ALPN identifying this example protocol on the iroh endpoint.
    ///
    /// Both peers must agree on it: the listener registers it with the [`Router`], the connector
    /// passes it to `connect`. The trailing `/1` leaves room to bump the version later.
    const ALPN: &[u8] = b"iroh-irpc/example-bidi/1";

    /// Requests the connecting peer sends to the listening peer.
    ///
    /// These operate on the listener's key-value store. Each variant carries a `tx` oneshot channel
    /// on which the handler sends its single response.
    #[rpc_requests(message = ClientToServerMsg)]
    #[derive(Debug, Serialize, Deserialize)]
    pub enum ClientToServer {
        /// Looks up a key, responding with the current value or `None` if absent.
        #[rpc(tx=oneshot::Sender<Option<String>>)]
        #[wrap(GetRequest, derive(Clone))]
        Get(String),

        /// Stores a value under a key, responding with the previous value if there was one.
        #[rpc(tx=oneshot::Sender<Option<String>>)]
        #[wrap(SetRequest)]
        Set {
            /// The key to store under.
            key: String,
            /// The value to store.
            value: String,
        },
    }

    /// Requests the listening peer sends back to a connected peer.
    ///
    /// This is the reverse direction: the listener drives it from its own [`irpc::Client`] built on
    /// top of the accepted connection. Here it is just a liveness check.
    #[rpc_requests(message = ServerToClientMsg)]
    #[derive(Debug, Serialize, Deserialize)]
    pub enum ServerToClient {
        /// Asks the peer to reply so the listener can confirm the connection is still alive.
        #[rpc(tx=oneshot::Sender<()>)]
        #[wrap(PingRequest)]
        Ping {},
    }

    /// Spawn the listening peer.
    ///
    /// Binds an endpoint, registers the [`Server`] as the [`ProtocolHandler`] for [`ALPN`], and
    /// spawns the background [`Server::ping_loop`] that pings connected peers.
    pub async fn server() -> Result<(Router, AbortOnDropHandle<()>)> {
        let endpoint = Endpoint::bind(presets::N0).await?;
        let server = Server::default();
        // The router drives `Server::accept` for every incoming connection on this ALPN.
        let router = Router::builder(endpoint)
            .accept(ALPN, server.clone())
            .spawn();
        // Printed so the connecting side can be pointed at this peer.
        println!("endpoint id: {}", router.endpoint().id());
        let ping_loop = tokio::spawn({
            let server = server.clone();
            server.ping_loop()
        });

        Ok((router, AbortOnDropHandle::new(ping_loop)))
    }

    /// The listening state: a shared key-value store plus a registry of reverse clients.
    #[derive(Debug, Clone, Default)]
    pub struct Server {
        /// The key-value store that [`ClientToServer`] requests read and write.
        state: Arc<Mutex<BTreeMap<String, String>>>,
        /// A reverse [`irpc::Client`] per connected peer, used to send [`ServerToClient`] pings.
        ///
        /// Keyed by the peer's endpoint id so the ping loop can reach every live connection and
        /// drop entries whose ping fails.
        clients: Arc<Mutex<HashMap<EndpointId, irpc::Client<ServerToClient>>>>,
    }

    impl ProtocolHandler for Server {
        /// Serves one accepted connection for its whole lifetime.
        ///
        /// This wires up both roles on the listener side: it first builds a reverse client over the
        /// same connection so the ping loop can call *back* to this peer, then runs the accept loop
        /// that answers this peer's requests.
        async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
            // Build a client on top of the incoming connection and register it under the peer's id.
            // The ping loop opens `ServerToClient` streams through this, in the reverse direction
            // to the `ClientToServer` streams we accept below.
            let client = Client::boxed(IrohRemoteConnection::new(conn.clone()));
            self.clients
                .lock()
                .unwrap()
                .insert(conn.remote_id(), client);
            // Accept and handle this peer's requests one at a time until it closes the connection.
            // `read_request` returns `None` on a clean shutdown, ending the loop.
            while let Some(msg) = read_request::<ClientToServer>(&conn).await? {
                self.handle_message(msg).await;
            }
            // The request loop ended; wait for the connection to fully close before returning. The
            // reverse client stays registered until a ping fails and the ping loop evicts it.
            conn.closed().await;
            Ok(())
        }
    }

    impl Server {
        /// Handles a single decoded [`ClientToServer`] request against the store.
        ///
        /// Each arm destructures [`WithChannels`] to split the request payload (`inner`) from its
        /// response channel (`tx`), then sends the reply. `tx.send(..).await.ok()` ignores a send
        /// error, which happens only if the requesting peer has already disconnected.
        async fn handle_message(&self, msg: ClientToServerMsg) {
            match msg {
                ClientToServerMsg::Get(msg) => {
                    let WithChannels { inner, tx, .. } = msg;
                    println!("handle request: {inner:?}");
                    let GetRequest(key) = inner;
                    let value = self.state.lock().unwrap().get(&key).cloned();
                    tx.send(value).await.ok();
                }
                ClientToServerMsg::Set(msg) => {
                    let WithChannels { inner, tx, .. } = msg;
                    println!("handle request: {inner:?}");
                    let SetRequest { key, value } = inner;
                    let prev_value = self.state.lock().unwrap().insert(key, value);
                    tx.send(prev_value).await.ok();
                }
            }
        }

        /// Pings every connected peer once a second, evicting any whose ping fails.
        ///
        /// This is the listener acting as a client: it calls out over each reverse client stored in
        /// [`Server::clients`]. Each ping runs in its own task so one slow or dead peer does not
        /// hold up the others. A failed ping means the connection is gone, so its entry is removed.
        async fn ping_loop(self) {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                // Hold the lock only for the synchronous fan-out: each iteration clones out what a
                // task needs and spawns it. The actual ping awaits happen inside those tasks, which
                // own their clones, so the std mutex is never held across an await.
                for (id, client) in self.clients.lock().unwrap().iter() {
                    let id = *id;
                    let client = client.clone();
                    let clients = self.clients.clone();
                    tokio::spawn(async move {
                        let now = Instant::now();
                        println!("ping {}...", id.fmt_short());
                        // A plain request/response RPC in the server-to-client direction. It
                        // resolves once the peer's accept loop answers the ping with a pong.
                        match client.rpc(PingRequest {}).await {
                            Ok(()) => {
                                println!("ping {}: OK ({:?})", id.fmt_short(), now.elapsed());
                            }
                            Err(err) => {
                                println!(
                                    "ping {}: FAIL {err:#} ({:?})",
                                    id.fmt_short(),
                                    now.elapsed()
                                );
                                clients.lock().unwrap().remove(&id);
                            }
                        }
                    });
                }
            }
        }
    }

    /// Connects to a listening peer and sets up both directions.
    ///
    /// Returns the endpoint (kept alive by the caller) and an [`irpc::Client`] for sending
    /// [`ClientToServer`] requests. It also spawns a background accept loop that answers the
    /// listener's [`ServerToClient`] pings, so this side serves the reverse protocol too.
    pub async fn connect(endpoint_id: EndpointId) -> Result<(Endpoint, Client<ClientToServer>)> {
        println!("connecting to {endpoint_id}");
        let endpoint = Endpoint::bind(presets::N0).await?;
        let conn = endpoint.connect(endpoint_id, ALPN).await?;
        // The outgoing client: opens `ClientToServer` streams to send get/set requests.
        let client = Client::boxed(IrohRemoteConnection::new(conn.clone()));
        // The reverse accept loop, mirroring the listener's `accept`: it answers the pings the
        // listener sends on this same connection. Detached into its own task so the caller can send
        // its own requests concurrently; the returned endpoint keeps the connection alive.
        let _accept_loop = tokio::spawn(async move {
            // Wait for the next server-initiated request. `Ok(None)` is a clean close; an error
            // means the connection dropped. Either way, stop serving.
            while let Some(msg) = read_request::<ServerToClient>(&conn).await? {
                match msg {
                    ServerToClientMsg::Ping(msg) => {
                        // Answer the ping by sending a unit on its response channel.
                        let WithChannels { tx, .. } = msg;
                        println!("Received ping from server, sending pong");
                        if let Err(err) = tx.send(()).await {
                            warn!("failed to send pong: {err:#}");
                            break;
                        }
                    }
                }
            }
            info!("ping loop closed");
            anyhow::Ok(())
        });
        Ok((endpoint, client))
    }
}

/// Command-line interface: selects the listener or connector role and runs it.
mod cli {
    use anyhow::Result;
    use clap::Parser;
    use iroh::EndpointId;

    use crate::proto::{GetRequest, SetRequest, connect, server};

    /// Runs the example as either a listening or a connecting peer.
    #[derive(Debug, Parser)]
    enum Cli {
        /// Run the listening peer and print its endpoint id.
        Listen,
        /// Connect to a listening peer and issue one request.
        Connect {
            endpoint_id: EndpointId,
            #[clap(subcommand)]
            command: Command,
        },
    }

    /// The single request a connector issues after connecting.
    #[derive(Debug, Parser)]
    enum Command {
        Get { key: String },
        Set { key: String, value: String },
    }

    /// Parses arguments and runs the chosen role to completion.
    pub async fn run() -> Result<()> {
        match Cli::parse() {
            Cli::Listen => {
                let (router, _guard) = server().await?;
                tokio::signal::ctrl_c().await.ok();
                router.shutdown().await?;
            }
            Cli::Connect {
                endpoint_id,
                command,
            } => {
                let (endpoint, client) = connect(endpoint_id).await?;
                // A single request/response RPC. `client.rpc` opens a bi stream, sends the request,
                // and awaits the one response the listener sends on the reply channel.
                match command {
                    Command::Get { key } => {
                        println!("get '{key}'");
                        let value = client.rpc(GetRequest(key)).await?;
                        println!("{value:?}");
                    }
                    Command::Set { key, value } => {
                        println!("set '{key}' to '{value}'");
                        let value = client.rpc(SetRequest { key, value }).await?;
                        println!("OK (previous: {value:?})");
                    }
                }
                // Stay connected so the spawned accept loop keeps answering the listener's pings;
                // this is what makes the ping output visible on both sides until ctrl-c.
                println!("waiting for ctrl-c");
                tokio::signal::ctrl_c().await.ok();
                endpoint.close().await;
            }
        }
        Ok(())
    }
}
