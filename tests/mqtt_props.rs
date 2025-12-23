use quantum::mqtt::{parse_disconnect, parse_subscribe, ProtocolVersion, PublishProperties, Qos};
use quantum::mqtt::{
    write_connack, write_disconnect, ConnAckProperties, DisconnectProperties, InternalCode,
};
use tokio::io::AsyncReadExt;

#[test]
fn subscribe_parses_subscription_identifier() {
    // SUBSCRIBE fixed header omitted; we feed variable header + payload
    // Packet ID = 10, properties len=2, property 0x0B= subscription id 7
    // One filter: "a/b" with qos1
    let mut buf = Vec::new();
    buf.extend_from_slice(&10u16.to_be_bytes());
    buf.push(2); // properties length
    buf.push(0x0B);
    buf.push(7);
    buf.extend_from_slice(&3u16.to_be_bytes());
    buf.extend_from_slice(b"a/b");
    buf.push(0x01); // qos1
    let parsed = parse_subscribe(&buf, ProtocolVersion::V5).unwrap();
    assert_eq!(parsed.packet_id, 10);
    assert_eq!(parsed.subscription_identifier, Some(7));
    assert_eq!(parsed.filters.len(), 1);
    assert_eq!(parsed.filters[0].qos, Qos::AtLeastOnce);
}

#[test]
fn disconnect_tolerates_properties() {
    // reason code 0x00, properties length=5, reason string id + "bye"
    let mut buf = Vec::new();
    buf.push(0); // reason
    buf.push(6); // properties length (id + len + data)
    buf.push(0x1F); // reason string
    buf.extend_from_slice(&3u16.to_be_bytes());
    buf.extend_from_slice(b"bye");
    let pkt = parse_disconnect(&buf, ProtocolVersion::V5).unwrap();
    assert_eq!(pkt.reason_code, 0);
}

#[test]
fn encode_publish_properties_handles_subscription_id() {
    let mut props = PublishProperties {
        subscription_identifier: Some(9),
        ..Default::default()
    };
    props.user_properties.push(("k".into(), "v".into()));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let encoded = rt.block_on(quantum::mqtt::write_publish_with_properties(
        &mut tokio::io::sink(),
        ProtocolVersion::V5,
        "topic",
        b"body",
        Qos::AtLeastOnce,
        false,
        Some(1),
        Some(&props),
    ));
    assert!(encoded.is_ok());
}

#[tokio::test]
async fn connack_encodes_server_keep_alive() {
    let (mut client, mut server) = tokio::io::duplex(64);
    let handle = tokio::spawn(async move {
        write_connack(
            &mut server,
            ProtocolVersion::V5,
            false,
            InternalCode::Success,
            ConnAckProperties {
                server_keep_alive: Some(30),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    });
    let mut buf = Vec::new();
    client.read_to_end(&mut buf).await.unwrap();
    handle.await.unwrap();
    assert!(
        buf.contains(&0x13),
        "expected server keep-alive property in CONNACK frame"
    );
}

#[tokio::test]
async fn disconnect_encodes_reason_string() {
    let (mut client, mut server) = tokio::io::duplex(64);
    let props = DisconnectProperties {
        reason_string: Some("bye".into()),
        ..Default::default()
    };
    let handle = tokio::spawn(async move {
        write_disconnect(
            &mut server,
            ProtocolVersion::V5,
            InternalCode::Success,
            Some(&props),
        )
        .await
        .unwrap();
    });
    let mut buf = Vec::new();
    client.read_to_end(&mut buf).await.unwrap();
    handle.await.unwrap();
    assert!(
        buf.contains(&0x1F),
        "expected reason string property in DISCONNECT frame"
    );
}
