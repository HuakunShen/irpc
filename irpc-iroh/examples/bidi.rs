//! Demonstrates the typical pattern where the server runs an actor loop that processes incoming
//! messages sequentially.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    cli::run().await
}

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
    use serde::{Deserialize, Serialize};
    use tokio::time::Instant;
    use tracing::{info, warn};

    const ALPN: &[u8] = b"iroh-irpc/example-bidi/1";

    #[rpc_requests(message = ClientToServerMsg)]
    #[derive(Debug, Serialize, Deserialize)]
    pub enum ClientToServer {
        /// This is the get request.
        #[rpc(tx=oneshot::Sender<Option<String>>)]
        #[wrap(GetRequest, derive(Clone))]
        Get(String),

        /// This is the set request.
        #[rpc(tx=oneshot::Sender<Option<String>>)]
        #[wrap(SetRequest)]
        Set {
            /// This is the key
            key: String,
            /// This is the value
            value: String,
        },
    }

    #[rpc_requests(message = ServerToClientMsg)]
    #[derive(Debug, Serialize, Deserialize)]
    pub enum ServerToClient {
        #[rpc(tx=oneshot::Sender<()>)]
        #[wrap(PingRequest)]
        Ping {},
    }

    pub async fn server() -> Result<()> {
        let endpoint = Endpoint::bind(presets::N0).await?;
        let server = Server::default();
        let router = Router::builder(endpoint)
            .accept(ALPN, server.clone())
            .spawn();
        println!("endpoint id: {}", router.endpoint().id());
        let ping_loop = tokio::spawn({
            let server = server.clone();
            server.ping_loop()
        });

        tokio::signal::ctrl_c().await?;
        ping_loop.abort();
        router.shutdown().await?;
        Ok(())
    }

    #[derive(Debug, Clone, Default)]
    pub struct Server {
        state: Arc<Mutex<BTreeMap<String, String>>>,
        clients: Arc<Mutex<HashMap<EndpointId, irpc::Client<ServerToClient>>>>,
    }

    impl ProtocolHandler for Server {
        async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
            let client = Client::boxed(IrohRemoteConnection::new(conn.clone()));
            self.clients
                .lock()
                .unwrap()
                .insert(conn.remote_id(), client);
            while let Some(msg) = read_request::<ClientToServer>(&conn).await? {
                self.handle_message(msg).await;
            }
            conn.closed().await;
            Ok(())
        }
    }

    impl Server {
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

        async fn ping_loop(self) {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let clients = self.clients.lock().unwrap();
                for (id, client) in clients.iter() {
                    let this = self.clone();
                    let client = client.clone();
                    let id = *id;
                    tokio::spawn(async move {
                        let now = Instant::now();
                        println!("ping {}...", id.fmt_short());
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
                                this.clients.lock().unwrap().remove(&id);
                            }
                        }
                    });
                }
            }
        }
    }

    pub async fn connect(endpoint_id: EndpointId) -> Result<(Endpoint, Client<ClientToServer>)> {
        println!("connecting to {endpoint_id}");
        let endpoint = Endpoint::bind(presets::N0).await?;
        let conn = endpoint.connect(endpoint_id, ALPN).await?;
        let client = Client::boxed(IrohRemoteConnection::new(conn.clone()));
        let _accept_loop = tokio::spawn(async move {
            loop {
                let msg = match read_request::<ServerToClient>(&conn).await {
                    Err(err) => {
                        warn!("connection to server closed: {err:#}");
                        break;
                    }
                    Ok(None) => {
                        info!("connection to server closed");
                        break;
                    }
                    Ok(Some(msg)) => msg,
                };
                match msg {
                    ServerToClientMsg::Ping(msg) => {
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
        });
        Ok((endpoint, client))
    }
}

mod cli {
    use anyhow::Result;
    use clap::Parser;
    use iroh::EndpointId;

    use crate::proto::{GetRequest, SetRequest, connect, server};

    #[derive(Debug, Parser)]
    enum Cli {
        Listen,
        Connect {
            endpoint_id: EndpointId,
            #[clap(subcommand)]
            command: Command,
        },
    }

    #[derive(Debug, Parser)]
    enum Command {
        Get { key: String },
        Set { key: String, value: String },
    }

    pub async fn run() -> Result<()> {
        match Cli::parse() {
            Cli::Listen => server().await?,
            Cli::Connect {
                endpoint_id,
                command,
            } => {
                let (endpoint, client) = connect(endpoint_id).await?;
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
                println!("waiting for ctrl-c");
                tokio::signal::ctrl_c().await.ok();
                endpoint.close().await;
            }
        }
        Ok(())
    }
}
