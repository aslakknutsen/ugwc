//! Debug client for ugwc ADS (`StreamAggregatedResources`).
//!
//! This is for inspecting snapshots on the wire, not full Envoy conformance (see design/TODO-best-practices.md).
//! Only **plaintext** `http://` gRPC is supported here (same as typical `UGWC_XDS_INSECURE=true` setups).

use clap::Parser;
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Request;
use xds_api::pb::envoy::config::core::v3::Node;
use xds_api::pb::envoy::service::discovery::v3::aggregated_discovery_service_client::AggregatedDiscoveryServiceClient;
use xds_api::pb::envoy::service::discovery::v3::DiscoveryRequest;

#[derive(Parser, Clone, Debug)]
pub struct XdsDumpArgs {
    /// gRPC server address (`http://host:port` plaintext only).
    #[arg(long, default_value = "http://127.0.0.1:18000")]
    pub addr: String,
    /// Sets `DiscoveryRequest.node.id` (e.g. `namespace/name` for scoped snapshots).
    #[arg(long)]
    pub node_id: Option<String>,
    /// Sets `DiscoveryRequest.type_url` (empty string selects envoy wire encoding on the server).
    #[arg(long)]
    pub type_url: Option<String>,
    /// When `--type-url` is omitted: `gwxds` sends the Praxis gwxds Resource type_url; `envoy` sends empty.
    #[arg(long, value_parser = ["envoy", "gwxds"])]
    pub wire: Option<String>,
    /// How many `DiscoveryRequest` messages to send (server emits one `DiscoveryResponse` per request today).
    #[arg(long, default_value_t = 3)]
    pub iterations: u32,
    /// After the summary line for each resource, decode `istio.gwxds.Resource` and print `Debug` (includes PEM bytes).
    #[arg(long, default_value_t = false)]
    pub decode: bool,
}

fn effective_type_url(args: &XdsDumpArgs) -> String {
    if let Some(t) = &args.type_url {
        return t.clone();
    }
    match args.wire.as_deref() {
        Some("gwxds") => ugwc_xds::GW_TYPE_URL.to_string(),
        _ => String::new(),
    }
}

async fn connect_channel(addr: &str) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    if !addr.starts_with("http://") {
        return Err(format!(
            "xds-dump: only plaintext http:// is supported (got {addr:?}); use UGWC_XDS_INSECURE with http:// bind"
        )
        .into());
    }
    let ep = tonic::transport::Endpoint::from_shared(addr.to_string())?;
    Ok(ep.connect().await?)
}

fn is_gwxds_resource_type_url(type_url: &str) -> bool {
    type_url == ugwc_xds::GW_TYPE_URL || type_url.ends_with("istio.gwxds.Resource")
}

fn print_decoded_resource(any: &xds_api::pb::google::protobuf::Any) {
    if !is_gwxds_resource_type_url(&any.type_url) {
        println!("  (decode skipped: expected istio.gwxds.Resource, got {})", any.type_url);
        return;
    }
    match ugwc_gwxds::istio::gwxds::Resource::decode(any.value.as_slice()) {
        Ok(r) => println!("{r:#?}"),
        Err(e) => eprintln!("  decode error: {e}"),
    }
}

pub async fn run(args: XdsDumpArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel(&args.addr).await?;
    let mut client = AggregatedDiscoveryServiceClient::new(channel);

    let type_url = effective_type_url(&args);
    let node = Some(Node {
        id: args.node_id.clone().unwrap_or_default(),
        ..Default::default()
    });

    let (tx, rx) = mpsc::channel::<DiscoveryRequest>(4);
    let req_stream = ReceiverStream::new(rx);

    let mut outbound = client
        .stream_aggregated_resources(Request::new(req_stream))
        .await?
        .into_inner();

    let send_n = args.iterations.max(1);
    for i in 0..send_n {
        let req = DiscoveryRequest {
            version_info: String::new(),
            node: node.clone(),
            type_url: type_url.clone(),
            response_nonce: String::new(),
            ..Default::default()
        };
        if tx.send(req).await.is_err() {
            break;
        }
        match outbound.message().await {
            Ok(Some(resp)) => {
                println!("--- response {} ---", i + 1);
                println!("version_info: {}", resp.version_info);
                println!("type_url: {:?}", resp.type_url);
                println!("resources: {}", resp.resources.len());
                for (j, any) in resp.resources.iter().enumerate() {
                    println!(
                        "  [{}] type_url={} size={}",
                        j,
                        any.type_url,
                        any.value.len()
                    );
                    if args.decode {
                        print_decoded_resource(any);
                    }
                }
            }
            Ok(None) => {
                println!("stream closed after {} response(s)", i);
                break;
            }
            Err(e) => {
                eprintln!("recv error: {e}");
                break;
            }
        }
    }
    drop(tx);
    Ok(())
}
