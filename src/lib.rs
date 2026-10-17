//! MQTT `watch_strategy` plugin (`dev.mcpg.watch.mqtt`).
//!
//! The `mqtt_topics` strategy subscribes to topic filters with manual
//! acknowledgement, through the SDK's queue helper. Each message is one
//! `mqtt.message` event; a QoS 1 or 2 message is acknowledged to the broker
//! once the gateway has stored its event. MQTT has no way to hand one
//! message back, so a message the gateway has not taken in `ack_timeout_ms`
//! makes the watch reconnect on its persistent session, and the broker sends
//! every unacknowledged message again: delivery is at-least-once. MQTT 3.1.1
//! messages carry no id, so a redelivered message is a new event.
//!
//! The client's event loop runs on its own task and is never held up by the
//! gateway, so the connection keeps alive while the window is full.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::queue::{
    Delivery, QueueOptions, QueueSource, acknowledge_queue_watch, cancel_queue_watch, defaults,
    spawn_queue_watch,
};
use rumqttc::tokio_rustls::rustls::{ClientConfig, RootCertStore};
use rumqttc::{
    AsyncClient, ConnectReturnCode, Event, MqttOptions, Packet, Publish, QoS, SubscribeFilter,
    TlsConfiguration, Transport,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::sync::{mpsc, oneshot};

pub const PLUGIN_ID: &str = "dev.mcpg.watch.mqtt";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "mqtt_topics";

/// The event type each message makes.
pub const MESSAGE_EVENT_TYPE: &str = "mqtt.message";

/// Largest `max_body_bytes`: the event must stay under the host's 256 KiB.
const MAX_BODY_BYTES: usize = 192 * 1024;

/// Largest packet the client takes: a message past it is refused by the
/// client rather than truncated.
const MAX_PACKET_BYTES: usize = 256 * 1024;

/// How long one receive waits for a message.
const RECEIVE_WAIT: Duration = Duration::from_secs(1);

/// How long a connect waits for the broker's CONNACK.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The fewest messages the client holds for the window. MQTT 3.1.1 gives a
/// client no way to lower the broker's in-flight limit (Mosquitto 20, EMQX
/// 32), and a message past a full buffer makes the watch reconnect, which
/// sends every unacknowledged message again.
const MIN_INCOMING_BUFFER: usize = 128;

fn default_qos() -> u8 {
    1
}

fn default_keep_alive_secs() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// `mqtt://host:1883` or `mqtts://host:8883`.
    url: String,
    /// The topic filters to subscribe to (`+` and `#` wildcards).
    topics: Vec<String>,
    /// The session's client id; default: one derived from the source, the
    /// same on every restart, so the broker keeps its session.
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    username: Option<String>,
    /// Resolved before the watch starts (`${env.X}`, `${secret.X}`,
    /// `vault://…`).
    #[serde(default)]
    password: Option<String>,
    /// 0, 1 or 2. At 0 the broker never sends a message again.
    #[serde(default = "default_qos")]
    qos: u8,
    /// A CA bundle, PEM, for a broker certificate a public root does not
    /// sign.
    #[serde(default)]
    ca_pem: Option<String>,
    #[serde(default = "default_keep_alive_secs")]
    keep_alive_secs: u64,
    #[serde(default = "defaults::max_in_flight")]
    max_in_flight: usize,
    #[serde(default = "defaults::ack_timeout_ms")]
    ack_timeout_ms: u64,
    #[serde(default = "defaults::max_body_bytes")]
    max_body_bytes: usize,
}

/// Where a broker URL points.
#[derive(Debug, PartialEq)]
struct Broker {
    host: String,
    port: u16,
    tls: bool,
}

fn broker_of(url: &str) -> Result<Broker, String> {
    let (rest, tls, default_port) = if let Some(rest) = url.strip_prefix("mqtts://") {
        (rest, true, 8883)
    } else if let Some(rest) = url.strip_prefix("mqtt://") {
        (rest, false, 1883)
    } else {
        return Err("url must start with mqtt:// or mqtts://".into());
    };
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.contains('@') {
        return Err("url must not carry credentials; use username and password".into());
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (
            host,
            port.parse::<u16>()
                .map_err(|_| format!("url port '{port}' is not a port"))?,
        ),
        None => (authority, default_port),
    };
    if host.is_empty() {
        return Err("url must name a host".into());
    }
    Ok(Broker {
        host: host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned(),
        port,
        tls,
    })
}

/// A client id the same for the same source on every restart.
fn derived_client_id(resource_uri: &str) -> String {
    let digest = blake3::hash(resource_uri.as_bytes()).to_hex();
    format!("mcpg-{}", &digest[..16])
}

fn qos_of(level: u8) -> QoS {
    match level {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}

/// What the event loop task hands the receiving thread.
enum Incoming {
    Publish(Publish),
    Failed(String),
}

/// One connection: its client, the task polling its event loop, and what
/// that task delivers.
struct Live {
    client: AsyncClient,
    task: tokio::task::JoinHandle<()>,
    incoming: mpsc::Receiver<Incoming>,
}

/// The broker side of one `mqtt_topics` watch.
struct MqttTopics {
    options: MqttOptions,
    filters: Vec<SubscribeFilter>,
    buffer: usize,
    max_body_bytes: usize,
    rt: tokio::runtime::Runtime,
    live: Mutex<Option<Live>>,
    /// The client to acknowledge with, apart from `live`, which the
    /// receiving thread holds while it waits.
    acker: Mutex<Option<AsyncClient>>,
    held: Mutex<HashMap<u64, Publish>>,
    next_receipt: AtomicU64,
    /// A message was released, or dropped on a full buffer: reconnect, so
    /// the broker sends what is unacknowledged again.
    redeliver: Arc<AtomicBool>,
}

impl MqttTopics {
    async fn open(&self) -> Result<Live, String> {
        let (client, mut eventloop) = AsyncClient::new(self.options.clone(), 64);
        let (tx, incoming) = mpsc::channel(self.buffer);
        let (connected, connack) = oneshot::channel::<Result<(), String>>();
        let redeliver = Arc::clone(&self.redeliver);
        let task = tokio::spawn(async move {
            let mut connected = Some(connected);
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                        let outcome = if ack.code == ConnectReturnCode::Success {
                            Ok(())
                        } else {
                            Err(format!(
                                "MQTT broker refused the connection: {:?}",
                                ack.code
                            ))
                        };
                        if let Some(connected) = connected.take() {
                            let _ = connected.send(outcome);
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        // Never wait here: the loop keeps the connection alive.
                        if tx.try_send(Incoming::Publish(publish)).is_err() {
                            redeliver.store(true, Ordering::Release);
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let error = format!("MQTT connection failed: {error}");
                        match connected.take() {
                            Some(connected) => {
                                let _ = connected.send(Err(error));
                            }
                            None => {
                                let _ = tx.send(Incoming::Failed(error)).await;
                            }
                        }
                        return;
                    }
                }
            }
        });
        let outcome = tokio::time::timeout(CONNECT_TIMEOUT, connack)
            .await
            .map_err(|_| "MQTT broker did not answer the connect".to_owned())
            .and_then(|r| r.map_err(|_| "MQTT connection closed".to_owned()))
            .and_then(|r| r);
        if let Err(error) = outcome {
            task.abort();
            return Err(error);
        }
        if let Err(error) = client.subscribe_many(self.filters.clone()).await {
            task.abort();
            return Err(format!("MQTT subscribe failed: {error}"));
        }
        Ok(Live {
            client,
            task,
            incoming,
        })
    }

    async fn close(live: Live) {
        let _ = tokio::time::timeout(Duration::from_secs(2), live.client.disconnect()).await;
        live.task.abort();
    }

    fn client(&self) -> Option<AsyncClient> {
        self.acker.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl QueueSource for MqttTopics {
    fn connect(&self) -> Result<(), String> {
        let mut live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(old) = live.take() {
            self.rt.block_on(Self::close(old));
        }
        // Messages held from the old connection are sent again on this one.
        self.redeliver.store(false, Ordering::Release);
        let opened = self.rt.block_on(self.open())?;
        *self.acker.lock().unwrap_or_else(|e| e.into_inner()) = Some(opened.client.clone());
        *live = Some(opened);
        Ok(())
    }

    fn receive(&self, max: usize, wait: Duration) -> Result<Vec<Delivery>, String> {
        if self.redeliver.load(Ordering::Acquire) {
            return Err("reconnecting, so the broker sends unacknowledged messages again".into());
        }
        let mut live = self.live.lock().unwrap_or_else(|e| e.into_inner());
        let Some(live) = live.as_mut() else {
            return Err("not connected".into());
        };
        let received = self.rt.block_on(async {
            let mut received = Vec::new();
            match tokio::time::timeout(wait, live.incoming.recv()).await {
                Err(_) => return Ok(received),
                Ok(None) => return Err("the MQTT connection ended".to_owned()),
                Ok(Some(Incoming::Failed(error))) => return Err(error),
                Ok(Some(Incoming::Publish(publish))) => received.push(publish),
            }
            while received.len() < max {
                match live.incoming.try_recv() {
                    Ok(Incoming::Publish(publish)) => received.push(publish),
                    Ok(Incoming::Failed(error)) if received.is_empty() => return Err(error),
                    _ => break,
                }
            }
            Ok(received)
        })?;
        let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        Ok(received
            .into_iter()
            .map(|publish| {
                let receipt = self.next_receipt.fetch_add(1, Ordering::Relaxed);
                let event = message_event(&publish, self.max_body_bytes);
                held.insert(receipt, publish);
                Delivery {
                    event,
                    receipt: Value::from(receipt),
                }
            })
            .collect())
    }

    fn ack(&self, receipt: &Value) -> Result<(), String> {
        let Some(publish) = receipt.as_u64().and_then(|r| {
            self.held
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&r)
        }) else {
            return Ok(());
        };
        if publish.qos == QoS::AtMostOnce {
            return Ok(());
        }
        let Some(client) = self.client() else {
            return Ok(());
        };
        // The session outlives a reconnect, so a packet id from the old
        // connection is still the broker's to settle.
        self.rt
            .block_on(client.ack(&publish))
            .map_err(|e| format!("MQTT ack failed: {e}"))
    }

    fn release(&self, receipt: &Value) -> Result<(), String> {
        let released = receipt.as_u64().and_then(|r| {
            self.held
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&r)
        });
        if released.is_some_and(|p| p.qos != QoS::AtMostOnce) {
            self.redeliver.store(true, Ordering::Release);
        }
        Ok(())
    }
}

impl Drop for MqttTopics {
    fn drop(&mut self) {
        let live = self.live.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(live) = live.take() {
            self.rt.block_on(Self::close(live));
        }
    }
}

/// The `mqtt.message` event of one message.
fn message_event(publish: &Publish, max_body_bytes: usize) -> WatchEvent {
    let mut data = Map::new();
    data.insert("topic".into(), json!(publish.topic));
    data.insert("qos".into(), json!(publish.qos as u8));
    data.insert("retain".into(), json!(publish.retain));
    data.insert("dup".into(), json!(publish.dup));
    if publish.payload.len() > max_body_bytes {
        data.insert("payload".into(), Value::Null);
        data.insert("payloadEncoding".into(), Value::Null);
        data.insert("payloadTruncated".into(), json!(true));
        data.insert("payloadSize".into(), json!(publish.payload.len()));
    } else {
        let (payload, encoding) = mcpg_plugin_protocol::event_body(&publish.payload);
        data.insert("payload".into(), payload);
        data.insert("payloadEncoding".into(), json!(encoding));
    }
    WatchEvent {
        event_type: Some(MESSAGE_EVENT_TYPE.to_owned()),
        data: Some(Value::Object(data)),
        ..WatchEvent::default()
    }
}

/// Install the crypto provider rustls builds TLS configurations from: a
/// build that links both rustls providers has no default, and this
/// cdylib's copy of rustls does not see the host's.
fn install_crypto_provider() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// `watch_strategy` entity. Stateless beyond its manifest — each watcher's
/// broker and topics arrive on the per-watch spec.
pub struct MqttWatchPlugin {
    manifest: PluginManifest,
}

impl MqttWatchPlugin {
    /// Infallible cdylib factory; the watch carries no plugin-level config.
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        install_crypto_provider();
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.watch.mqtt",
                name: "MQTT Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for MqttWatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, _) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: WatchSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid mqtt_topics watch spec: {e}"),
            })?;
        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        let broker = broker_of(&parsed.url).map_err(invalid)?;
        if parsed.topics.is_empty() || parsed.topics.iter().any(|t| t.trim().is_empty()) {
            return Err(invalid("topics must name at least one topic filter".into()));
        }
        if parsed.qos > 2 {
            return Err(invalid("qos must be 0, 1 or 2".into()));
        }
        if parsed
            .password
            .as_deref()
            .is_some_and(|p| p.starts_with("cred://"))
        {
            return Err(invalid(
                "password must not be a cred:// URI — per-caller credentials are unsupported; \
                 use ${env.X} / ${secret.X} / vault:// (resolved before the watch starts)"
                    .into(),
            ));
        }
        if parsed.password.is_some() && parsed.username.is_none() {
            return Err(invalid("a password needs a username".into()));
        }
        if !(5..=3_600).contains(&parsed.keep_alive_secs) {
            return Err(invalid("keep_alive_secs must be 5 to 3600".into()));
        }
        if !(1..=MAX_BODY_BYTES).contains(&parsed.max_body_bytes) {
            return Err(invalid(format!(
                "max_body_bytes must be 1 to {MAX_BODY_BYTES}"
            )));
        }
        let client_id = parsed
            .client_id
            .clone()
            .unwrap_or_else(|| derived_client_id(resource_uri));
        if client_id.is_empty() || client_id.len() > 128 {
            return Err(invalid("client_id must be 1 to 128 characters".into()));
        }
        let options = QueueOptions {
            source: format!("mqtt {}:{} {:?}", broker.host, broker.port, parsed.topics),
            max_in_flight: parsed.max_in_flight,
            ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
            receive_wait: RECEIVE_WAIT,
        };
        options.validate().map_err(invalid)?;

        let mut mqtt = MqttOptions::new(client_id, broker.host.clone(), broker.port);
        mqtt.set_keep_alive(Duration::from_secs(parsed.keep_alive_secs))
            // A persistent session: what is unacknowledged at a reconnect
            // comes again.
            .set_clean_session(false)
            .set_manual_acks(true)
            .set_max_packet_size(MAX_PACKET_BYTES, MAX_PACKET_BYTES);
        if let Some(username) = &parsed.username {
            mqtt.set_credentials(username, parsed.password.clone().unwrap_or_default());
        }
        if broker.tls {
            let tls = match parsed.ca_pem {
                Some(ca) => TlsConfiguration::Simple {
                    ca: ca.into_bytes(),
                    alpn: None,
                    client_auth: None,
                },
                None => {
                    let roots =
                        RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                    TlsConfiguration::Rustls(Arc::new(
                        ClientConfig::builder()
                            .with_root_certificates(roots)
                            .with_no_client_auth(),
                    ))
                }
            };
            mqtt.set_transport(Transport::tls_with_config(tls));
        }
        let filters = parsed
            .topics
            .iter()
            .map(|t| SubscribeFilter::new(t.trim().to_owned(), qos_of(parsed.qos)))
            .collect();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("mcpg-mqtt-watch")
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("mqtt_topics: tokio runtime init failed: {e}"),
            })?;
        let source = MqttTopics {
            options: mqtt,
            filters,
            buffer: parsed.max_in_flight.max(MIN_INCOMING_BUFFER),
            max_body_bytes: parsed.max_body_bytes,
            rt,
            live: Mutex::new(None),
            acker: Mutex::new(None),
            held: Mutex::new(HashMap::new()),
            next_receipt: AtomicU64::new(0),
            redeliver: Arc::new(AtomicBool::new(false)),
        };
        spawn_queue_watch(resource_uri, options, emit_event, source)
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        acknowledge_queue_watch(watch_handle, position);
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        cancel_queue_watch(watch_handle);
    }
}

mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.watch.mqtt",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        watch_strategy as watch {
            inner_name: "",
            plugin_type: MqttWatchPlugin,
            factory: |cfg: &str, host: ::mcpg_plugin_sdk::HostHandle|
                MqttWatchPlugin::from_host_config(cfg, host),
        },
    ],
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stub_host() -> HostHandle {
        // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
        // factory ignores the host entirely.
        #[allow(unsafe_code)]
        unsafe {
            HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
        }
    }

    fn plugin() -> MqttWatchPlugin {
        MqttWatchPlugin::from_host_config("", stub_host())
    }

    #[test]
    fn a_tls_configuration_can_be_built_once_the_plugin_is_made() {
        let _ = plugin();
        let _ = rustls::ClientConfig::builder();
    }

    fn refused(spec: Value) -> String {
        match plugin().watch("mcpg-event://sensors", &spec, Box::new(|_| {})) {
            Err(WatchError::InvalidSpec { message }) => message,
            Err(other) => panic!("not a spec error: {other:?}"),
            Ok(handle) => {
                plugin().cancel(handle);
                panic!("accepted {spec}");
            }
        }
    }

    #[test]
    fn manifest_and_kind_are_correct() {
        use mcpg_plugin_protocol::PluginClass;
        let p = plugin();
        assert_eq!(SyncWatchStrategyPlugin::manifest(&p).id, PLUGIN_ID);
        assert_eq!(
            SyncWatchStrategyPlugin::manifest(&p).plugin_class,
            PluginClass::WatchStrategy
        );
        assert_eq!(p.kind(), WATCH_KIND);
    }

    #[test]
    fn a_broker_url_names_host_port_and_tls() {
        assert_eq!(
            broker_of("mqtt://broker.local").unwrap(),
            Broker {
                host: "broker.local".into(),
                port: 1883,
                tls: false
            }
        );
        assert_eq!(
            broker_of("mqtts://broker.local:9883/").unwrap(),
            Broker {
                host: "broker.local".into(),
                port: 9883,
                tls: true
            }
        );
        assert!(broker_of("tcp://broker").is_err());
        assert!(
            broker_of("mqtt://u:p@broker")
                .unwrap_err()
                .contains("credentials")
        );
        assert!(broker_of("mqtt://broker:x").is_err());
        assert!(broker_of("mqtt://").is_err());
    }

    #[test]
    fn the_derived_client_id_is_the_same_for_the_same_source() {
        assert_eq!(
            derived_client_id("mcpg-event://sensors"),
            derived_client_id("mcpg-event://sensors")
        );
        assert_ne!(
            derived_client_id("mcpg-event://sensors"),
            derived_client_id("mcpg-event://other")
        );
        assert!(
            derived_client_id("x").len() <= 23,
            "within MQTT 3.1.1's minimum"
        );
    }

    #[test]
    fn a_bad_spec_is_refused_before_anything_connects() {
        let with = |extra: Value| {
            let mut spec = json!({ "url": "mqtt://broker", "topics": ["sensors/#"] });
            for (k, v) in extra.as_object().unwrap() {
                spec[k] = v.clone();
            }
            spec
        };
        assert!(refused(with(json!({ "bogus": 1 }))).contains("unknown field"));
        assert!(refused(with(json!({ "url": "http://broker" }))).contains("mqtt://"));
        assert!(refused(with(json!({ "topics": [] }))).contains("topic"));
        assert!(refused(with(json!({ "qos": 3 }))).contains("qos"));
        assert!(
            refused(with(
                json!({ "username": "u", "password": "cred://vault/mqtt" })
            ))
            .contains("cred://")
        );
        assert!(refused(with(json!({ "password": "p" }))).contains("username"));
        assert!(refused(with(json!({ "keep_alive_secs": 1 }))).contains("keep_alive_secs"));
        assert!(refused(with(json!({ "max_in_flight": 0 }))).contains("max_in_flight"));
        assert!(refused(with(json!({ "max_body_bytes": 300_000 }))).contains("max_body_bytes"));
        assert!(refused(with(json!({ "client_id": "" }))).contains("client_id"));
    }

    #[test]
    fn a_message_becomes_an_mqtt_message_event() {
        let publish = Publish::new(
            "sensors/a/temp",
            QoS::AtLeastOnce,
            br#"{"c":21.5}"#.to_vec(),
        );
        let event = message_event(&publish, 1024);
        assert_eq!(event.event_type.as_deref(), Some(MESSAGE_EVENT_TYPE));
        assert_eq!(event.event_id, None, "MQTT 3.1.1 messages carry no id");
        let data = event.data.unwrap();
        assert_eq!(data["topic"], json!("sensors/a/temp"));
        assert_eq!(data["qos"], json!(1));
        assert_eq!(data["payload"], json!({ "c": 21.5 }));
        assert_eq!(data["payloadEncoding"], json!("json"));
        let big = message_event(&publish, 4).data.unwrap();
        assert_eq!(big["payloadTruncated"], json!(true));
        assert_eq!(big["payloadSize"], json!(10));
    }
}
