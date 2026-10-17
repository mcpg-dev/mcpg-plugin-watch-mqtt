# MQTT Watch Strategy — `dev.mcpg.watch.mqtt`

> class `watch_strategy` · `native` · package `mcpg-plugin-watch-mqtt` · artifact `libmcpg_plugin_watch_mqtt.so` · Apache-2.0

Subscribes to MQTT 3.1.1 topic filters on any compliant broker (Mosquitto,
EMQX, HiveMQ, AWS IoT Core) and makes each message an MCP Event. The
`mqtt_topics` strategy keeps a **persistent session** with **manual
acknowledgement**:

- A QoS 1 or QoS 2 message is acknowledged to the broker once the gateway
  has stored its `mqtt.message` event, and only then.
- MQTT cannot hand one message back. A message the gateway has not taken in
  `ack_timeout_ms` makes the watch reconnect on its session, and the broker
  sends every unacknowledged message again (with `dup: true`): delivery is
  at-least-once.
- MQTT 3.1.1 messages carry no id, so a message sent again is a new event:
  a subscriber may see it twice.
- The session's client id is derived from the event source, the same on
  every restart, so the broker keeps the session, and what arrives while
  the watch is stopped, between restarts. Set `client_id` to choose it; two
  watches must not share one.
- QoS 0 messages are never sent again.

## Configuration

```yaml
plugins:
  - id: dev.mcpg.watch.mqtt
    class: watch_strategy
    kind: native
    source: { oci: "<registry>/plugins/watch-mqtt:protocol-1" }

mcp:
  configurations:
    events:
      enabled: true
      store: { kind: file, dir: /var/lib/mcpg/events/subscriptions }
      log_store: { kind: file, dir: /var/lib/mcpg/events/log }
  capabilities:
    events:
      - name: sensors.reading
        description: A temperature reading from a plant sensor.
        source:
          watch:
            strategy:
              type: plugin
              kind: mqtt_topics
              url: mqtts://broker.plant.example.com:8883
              topics: ["plant/+/temperature"]
              username: mcpg
              password: ${env.MQTT_PASSWORD}
            event_type: mqtt.message
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `url` | string (required) | — | `mqtt://host[:1883]` or `mqtts://host[:8883]`; no credentials in it. |
| `topics` | list of string (required) | — | Topic filters, with `+` and `#`. |
| `qos` | 0, 1, 2 | `1` | The subscription's QoS; at 0 nothing is sent again. |
| `client_id` | string | derived from the source | The persistent session's id (1 to 128 characters). |
| `username`, `password` | string | — | The password resolves before the watch starts (`${env.X}`, `${secret.X}`, `vault://…`); `cred://` is rejected. |
| `ca_pem` | string | public roots | A CA bundle, PEM, for a broker certificate a public root does not sign. |
| `keep_alive_secs` | int | `30` | 5 to 3600. |
| `max_in_flight` | int | `256` | Most messages delivered and not yet acknowledged (1 to 1000). |
| `ack_timeout_ms` | int | `60000` | How long a message waits for the gateway before the watch reconnects to have it sent again. |
| `max_body_bytes` | int | `131072` | A larger payload is left out of the event, with `payloadTruncated: true` and `payloadSize` (at most 196608). |

The client keeps polling the connection while the gateway is busy, so the
session stays alive when the window is full. A broker's own in-flight limit
(Mosquitto's `max_inflight_messages`, 20 by default) also bounds what is
outstanding. Messages that the window cannot take yet wait in the client, up
to `max_in_flight` or 128, whichever is larger. A message past that is
dropped, and the watch reconnects so that the broker sends it again.

## Events

Each `mqtt.message` event's `data` is `{topic, qos, retain, dup, payload,
payloadEncoding}`: `payload` is its JSON when it parses, else its UTF-8 text,
else standard base64 of its bytes, and `payloadEncoding` says which.

## Build / test

```bash
cargo build  -p mcpg-plugin-watch-mqtt
cargo test   -p mcpg-plugin-watch-mqtt                              # unit tests
cargo test   -p mcpg-plugin-watch-mqtt --features integration-tests # Mosquitto (docker)
cargo clippy -p mcpg-plugin-watch-mqtt --all-targets -- -D warnings
```

The integration suite runs the watch through a window of two, an
acknowledgement, a redelivery through a reconnect after the timeout, and a
message the persistent session kept while the watch was stopped.
