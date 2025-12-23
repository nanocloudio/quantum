use crate::control::ControlPlaneError;
use crate::routing::RoutingError;
use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    V3_1,
    V3_1_1,
    V5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Qos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone)]
pub struct ConnectPacket {
    pub client_id: String,
    pub keep_alive: u16,
    pub clean_start: bool,
    pub protocol: ProtocolVersion,
    pub will: Option<Will>,
    pub properties: ConnectProperties,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct PublishPacket {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: Qos,
    pub message_id: Option<u16>,
    pub dup: bool,
    pub retain: bool,
    pub properties: PublishProperties,
}

#[derive(Debug, Clone)]
pub struct SubscriptionRequest {
    pub topic_filter: String,
    pub qos: Qos,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
}

#[derive(Debug, Clone)]
pub struct SubscribePacket {
    pub packet_id: u16,
    pub filters: Vec<SubscriptionRequest>,
    pub subscription_identifier: Option<u32>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct UnsubscribePacket {
    pub packet_id: u16,
    pub topics: Vec<String>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DisconnectPacket {
    pub reason_code: u8,
    pub reason_string: Option<String>,
    pub server_reference: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DisconnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub reason_string: Option<String>,
    pub server_reference: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub struct AuthPacket {
    pub reason_code: u8,
    pub auth_method: Option<String>,
    pub auth_data: Option<Vec<u8>>,
    pub reason_string: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub topic_alias_max: Option<u16>,
    pub user_properties: Vec<(String, String)>,
    pub auth_method: Option<String>,
    pub auth_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub struct WillProperties {
    pub delay_interval: Option<u32>,
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: Qos,
    pub retain: bool,
    pub properties: WillProperties,
}

#[derive(Debug, Clone, Default)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub subscription_identifier: Option<u32>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub struct ConnAckProperties {
    pub receive_max: Option<u16>,
    pub session_expiry: Option<u32>,
    pub server_keep_alive: Option<u16>,
    pub reason_string: Option<String>,
    pub assigned_client_identifier: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub enum ControlPacket {
    Connect(ConnectPacket),
    Publish(PublishPacket),
    PubAck(u16),
    PubRec(u16),
    PubRel(u16),
    PubComp(u16),
    Subscribe(SubscribePacket),
    Unsubscribe(UnsubscribePacket),
    PingReq,
    Disconnect(DisconnectPacket),
    Auth(AuthPacket),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InternalCode {
    Success,
    PermanentEpoch,
    PermanentDurability,
    TransientBackpressure,
    TokenBindingMismatch,
    SessionTakenOver,
    OverQuota,
    Unauthorized,
    ProtocolError,
}

impl From<&ControlPlaneError> for InternalCode {
    fn from(err: &ControlPlaneError) -> Self {
        match err {
            ControlPlaneError::StaleEpoch => InternalCode::PermanentEpoch,
            ControlPlaneError::CacheExpired
            | ControlPlaneError::NeededForReadIndex
            | ControlPlaneError::StrictFallback => InternalCode::PermanentDurability,
        }
    }
}

impl From<ControlPlaneError> for InternalCode {
    fn from(err: ControlPlaneError) -> Self {
        InternalCode::from(&err)
    }
}

impl From<&RoutingError> for InternalCode {
    fn from(err: &RoutingError) -> Self {
        match err {
            RoutingError::StaleEpoch { .. } => InternalCode::PermanentEpoch,
            RoutingError::TenantMismatch => InternalCode::Unauthorized,
        }
    }
}

impl From<RoutingError> for InternalCode {
    fn from(err: RoutingError) -> Self {
        InternalCode::from(&err)
    }
}

pub struct ReasonCodes;

impl ReasonCodes {
    pub const SUCCESS: u8 = 0x00;
    pub const NOT_AUTHORIZED: u8 = 0x87;
    pub const SERVER_UNAVAILABLE: u8 = 0x88;
    pub const SESSION_TAKEN_OVER: u8 = 0x8E;
    pub const QUOTA_EXCEEDED: u8 = 0x97;
    pub const SERVER_MOVED: u8 = 0x9D;
    pub const UNSPECIFIED: u8 = 0x80;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubAckResult {
    Granted(Qos),
    Error(InternalCode),
}

/// Map internal errors to MQTT reason codes, applying downgrade rules for v3.1/v3.1.1.
pub fn map_reason(proto: ProtocolVersion, code: InternalCode) -> u8 {
    match proto {
        ProtocolVersion::V5 => match code {
            InternalCode::Success => ReasonCodes::SUCCESS,
            InternalCode::PermanentEpoch => ReasonCodes::SERVER_MOVED,
            InternalCode::PermanentDurability => ReasonCodes::SERVER_UNAVAILABLE,
            InternalCode::TransientBackpressure => ReasonCodes::QUOTA_EXCEEDED,
            InternalCode::TokenBindingMismatch => ReasonCodes::NOT_AUTHORIZED,
            InternalCode::SessionTakenOver => ReasonCodes::SESSION_TAKEN_OVER,
            InternalCode::OverQuota => ReasonCodes::QUOTA_EXCEEDED,
            InternalCode::Unauthorized => ReasonCodes::NOT_AUTHORIZED,
            InternalCode::ProtocolError => ReasonCodes::UNSPECIFIED,
        },
        ProtocolVersion::V3_1 | ProtocolVersion::V3_1_1 => match code {
            InternalCode::Success => 0x00,
            InternalCode::TokenBindingMismatch | InternalCode::Unauthorized => 0x05, // Not authorized
            _ => 0x03, // Server unavailable / refused
        },
    }
}

/// Parse an MQTT CONNECT packet from the stream.
pub async fn read_connect<S: AsyncReadExt + Unpin>(stream: &mut S) -> Result<ConnectPacket> {
    let mut header = [0u8; 1];
    stream.read_exact(&mut header).await?;
    let packet_type = header[0] >> 4;
    if packet_type != 1 {
        return Err(anyhow!("expected CONNECT (type 1), got {packet_type}"));
    }
    let remaining_len = decode_remaining_length_stream(stream).await?;
    let mut payload = vec![0u8; remaining_len as usize];
    stream.read_exact(&mut payload).await?;
    parse_connect(&payload)
}

/// Read a control packet after CONNECT for the given protocol version.
pub async fn read_packet<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
) -> Result<ControlPacket> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).await?;
    let packet_type = first[0] >> 4;
    let flags = first[0] & 0x0F;
    let remaining_len = decode_remaining_length_stream(stream).await?;
    let mut buf = vec![0u8; remaining_len as usize];
    stream.read_exact(&mut buf).await?;
    match packet_type {
        3 => Ok(ControlPacket::Publish(parse_publish(flags, &buf, proto)?)),
        4 => Ok(ControlPacket::PubAck(parse_packet_id(&buf)?)),
        5 => Ok(ControlPacket::PubRec(parse_packet_id(&buf)?)),
        6 => Ok(ControlPacket::PubRel(parse_packet_id(&buf)?)),
        7 => Ok(ControlPacket::PubComp(parse_packet_id(&buf)?)),
        8 => Ok(ControlPacket::Subscribe(parse_subscribe(&buf, proto)?)),
        10 => Ok(ControlPacket::Unsubscribe(parse_unsubscribe(&buf, proto)?)),
        12 => Ok(ControlPacket::PingReq),
        14 => Ok(ControlPacket::Disconnect(parse_disconnect(&buf, proto)?)),
        15 => Ok(ControlPacket::Auth(parse_auth(&buf, proto)?)),
        _ => Err(anyhow!("unsupported packet type {packet_type}")),
    }
}

pub async fn write_connack<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    session_present: bool,
    code: InternalCode,
    props: ConnAckProperties,
) -> Result<()> {
    let reason = map_reason(proto, code);
    match proto {
        ProtocolVersion::V3_1 | ProtocolVersion::V3_1_1 => {
            let flags = if session_present { 0x01 } else { 0x00 };
            let mut payload = vec![flags, reason];
            let mut frame = vec![0x20];
            frame.extend(encode_remaining_length(payload.len()));
            frame.append(&mut payload);
            stream.write_all(&frame).await?;
        }
        ProtocolVersion::V5 => {
            let flags = if session_present { 0x01 } else { 0x00 };
            let mut props_buf = Vec::new();
            if let Some(rm) = props.receive_max {
                props_buf.push(0x21);
                props_buf.extend_from_slice(&rm.to_be_bytes());
            }
            if let Some(expiry) = props.session_expiry {
                props_buf.push(0x11);
                props_buf.extend_from_slice(&expiry.to_be_bytes());
            }
            if let Some(keep_alive) = props.server_keep_alive {
                props_buf.push(0x13);
                props_buf.extend_from_slice(&keep_alive.to_be_bytes());
            }
            if let Some(assigned) = props.assigned_client_identifier {
                props_buf.push(0x12);
                props_buf.extend_from_slice(&(assigned.len() as u16).to_be_bytes());
                props_buf.extend_from_slice(assigned.as_bytes());
            }
            if let Some(reason) = props.reason_string {
                props_buf.push(0x1F);
                props_buf.extend_from_slice(&(reason.len() as u16).to_be_bytes());
                props_buf.extend_from_slice(reason.as_bytes());
            }
            for (k, v) in props.user_properties {
                props_buf.push(0x26);
                props_buf.extend_from_slice(&(k.len() as u16).to_be_bytes());
                props_buf.extend_from_slice(k.as_bytes());
                props_buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
                props_buf.extend_from_slice(v.as_bytes());
            }
            let mut payload = vec![flags, reason];
            let props_len = encode_remaining_length(props_buf.len());
            payload.extend(props_len);
            payload.extend(props_buf);
            let mut frame = vec![0x20];
            frame.extend(encode_remaining_length(payload.len()));
            frame.extend(payload);
            stream.write_all(&frame).await?;
        }
    }
    Ok(())
}

pub async fn write_suback<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    packet_id: u16,
    grants: &[SubAckResult],
) -> Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet_id.to_be_bytes());
    if matches!(proto, ProtocolVersion::V5) {
        payload.push(0); // property length
    }
    for grant in grants {
        let code = match grant {
            SubAckResult::Granted(Qos::AtMostOnce) => 0x00,
            SubAckResult::Granted(Qos::AtLeastOnce) => 0x01,
            SubAckResult::Granted(Qos::ExactlyOnce) => 0x02,
            SubAckResult::Error(err) => {
                if matches!(proto, ProtocolVersion::V5) {
                    map_reason(proto, *err)
                } else {
                    0x80
                }
            }
        };
        payload.push(code);
    }
    let mut header = vec![0x90];
    header.extend(encode_remaining_length(payload.len()));
    header.extend(payload);
    stream.write_all(&header).await?;
    Ok(())
}

pub async fn write_unsuback<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    packet_id: u16,
    code: InternalCode,
) -> Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&packet_id.to_be_bytes());
    if matches!(proto, ProtocolVersion::V5) {
        payload.push(0); // property length
        payload.push(map_reason(proto, code));
    }
    let mut header = vec![0xB0];
    header.extend(encode_remaining_length(payload.len()));
    header.extend(payload);
    stream.write_all(&header).await?;
    Ok(())
}

pub async fn write_puback<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    mid: u16,
    code: InternalCode,
) -> Result<()> {
    write_puback_variant(stream, proto, 0x40, mid, code).await
}

pub async fn write_pubrec<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    mid: u16,
    code: InternalCode,
) -> Result<()> {
    write_puback_variant(stream, proto, 0x50, mid, code).await
}

pub async fn write_pubrel<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    mid: u16,
) -> Result<()> {
    write_puback_variant(stream, proto, 0x62, mid, InternalCode::Success).await
}

pub async fn write_pubcomp<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    mid: u16,
) -> Result<()> {
    write_puback_variant(stream, proto, 0x70, mid, InternalCode::Success).await
}

pub async fn write_pingresp<S: AsyncWriteExt + Unpin>(stream: &mut S) -> Result<()> {
    stream.write_all(&[0xD0, 0x00]).await?;
    Ok(())
}

pub async fn write_disconnect<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    code: InternalCode,
    props: Option<&DisconnectProperties>,
) -> Result<()> {
    match proto {
        ProtocolVersion::V5 => {
            let reason = map_reason(proto, code);
            let mut prop_buf = Vec::new();
            if let Some(props) = props {
                if let Some(expiry) = props.session_expiry_interval {
                    prop_buf.push(0x11);
                    prop_buf.extend_from_slice(&expiry.to_be_bytes());
                }
                if let Some(reason_str) = &props.reason_string {
                    prop_buf.push(0x1F);
                    prop_buf.extend_from_slice(&(reason_str.len() as u16).to_be_bytes());
                    prop_buf.extend_from_slice(reason_str.as_bytes());
                }
                if let Some(server_ref) = &props.server_reference {
                    prop_buf.push(0x1C);
                    prop_buf.extend_from_slice(&(server_ref.len() as u16).to_be_bytes());
                    prop_buf.extend_from_slice(server_ref.as_bytes());
                }
                for (k, v) in &props.user_properties {
                    prop_buf.push(0x26);
                    prop_buf.extend_from_slice(&(k.len() as u16).to_be_bytes());
                    prop_buf.extend_from_slice(k.as_bytes());
                    prop_buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
                    prop_buf.extend_from_slice(v.as_bytes());
                }
            }
            let mut frame = vec![0xE0, 0x00, reason];
            frame.extend(encode_remaining_length(prop_buf.len()));
            frame.extend(prop_buf);
            stream.write_all(&frame).await?;
        }
        ProtocolVersion::V3_1 | ProtocolVersion::V3_1_1 => {
            // MQTT 3.1/3.1.1 disconnect has no reason code.
            stream.write_all(&[0xE0, 0x00]).await?;
        }
    }
    Ok(())
}

/// Send an AUTH packet (MQTT 5 enhanced auth).
pub async fn write_auth<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    code: InternalCode,
    reason_string: Option<&str>,
) -> Result<()> {
    if !matches!(proto, ProtocolVersion::V5) {
        anyhow::bail!("AUTH is MQTT 5-only");
    }
    let reason = map_reason(proto, code);
    let mut props_buf = Vec::new();
    if let Some(reason_str) = reason_string {
        props_buf.push(0x1F);
        props_buf.extend_from_slice(&(reason_str.len() as u16).to_be_bytes());
        props_buf.extend_from_slice(reason_str.as_bytes());
    }
    let mut payload = vec![reason];
    payload.extend(encode_remaining_length(props_buf.len()));
    payload.extend(props_buf);
    let mut frame = vec![0xF0];
    frame.extend(encode_remaining_length(payload.len()));
    frame.append(&mut payload);
    stream.write_all(&frame).await?;
    Ok(())
}

/// Send a PUBLISH packet with optional QoS/retain.
pub async fn write_publish<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    topic: &str,
    payload: &[u8],
    qos: Qos,
    retain: bool,
    message_id: Option<u16>,
) -> Result<()> {
    write_publish_with_properties(stream, proto, topic, payload, qos, retain, message_id, None)
        .await
}

/// Send a PUBLISH packet with MQTT5 properties if provided.
#[allow(clippy::too_many_arguments)]
pub async fn write_publish_with_properties<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    topic: &str,
    payload: &[u8],
    qos: Qos,
    retain: bool,
    message_id: Option<u16>,
    properties: Option<&PublishProperties>,
) -> Result<()> {
    let qos_bits = match qos {
        Qos::AtMostOnce => 0u8,
        Qos::AtLeastOnce => 0b0010,
        Qos::ExactlyOnce => 0b0100,
    };
    let retain_bit = if retain { 0x01 } else { 0x00 };
    let header = 0b0011_0000 | qos_bits | retain_bit;
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if !matches!(qos, Qos::AtMostOnce) {
        let mid = message_id.ok_or_else(|| anyhow::anyhow!("message id required"))?;
        body.extend_from_slice(&mid.to_be_bytes());
    }
    if matches!(proto, ProtocolVersion::V5) {
        let props_buf = properties
            .map(encode_publish_properties)
            .unwrap_or_default();
        body.extend(encode_remaining_length(props_buf.len()));
        body.extend(props_buf);
    }
    body.extend_from_slice(payload);
    let mut frame = vec![header];
    frame.extend(encode_remaining_length(body.len()));
    frame.extend(body);
    stream.write_all(&frame).await?;
    Ok(())
}

fn parse_connect(buf: &[u8]) -> Result<ConnectPacket> {
    let mut cursor = 0usize;
    let _proto_name = read_string(buf, &mut cursor)?;
    let proto_level = read_u8(buf, &mut cursor)?;
    let connect_flags = read_u8(buf, &mut cursor)?;
    let keep_alive = read_u16(buf, &mut cursor)?;

    let protocol = match proto_level {
        3 => ProtocolVersion::V3_1,
        4 => ProtocolVersion::V3_1_1,
        5 => ProtocolVersion::V5,
        other => return Err(anyhow!("unsupported protocol level {other}")),
    };

    let mut properties = ConnectProperties::default();
    if matches!(protocol, ProtocolVersion::V5) {
        let props_len = decode_varint(buf, &mut cursor).unwrap_or(0);
        properties = parse_connect_properties(buf, &mut cursor, props_len)?;
    }

    let client_id = read_string(buf, &mut cursor)?;
    let clean_start = (connect_flags & 0x02) != 0;
    let will_flag = (connect_flags & 0x04) != 0;
    let will_qos_bits = (connect_flags >> 3) & 0x03;
    let will_retain = (connect_flags & 0x20) != 0;
    let mut will = None;
    if will_flag {
        let qos = match will_qos_bits {
            0 => Qos::AtMostOnce,
            1 => Qos::AtLeastOnce,
            2 => Qos::ExactlyOnce,
            other => return Err(anyhow!("unsupported QoS {other}")),
        };
        let will_props = if matches!(protocol, ProtocolVersion::V5) {
            parse_will_properties(buf, &mut cursor)?
        } else {
            WillProperties::default()
        };
        let topic = read_string(buf, &mut cursor)?;
        let payload = read_binary(buf, &mut cursor)?;
        will = Some(Will {
            topic,
            payload,
            qos,
            retain: will_retain,
            properties: will_props,
        });
    }
    let username_flag = (connect_flags & 0x80) != 0;
    let password_flag = (connect_flags & 0x40) != 0;
    let username = if username_flag {
        Some(read_string(buf, &mut cursor)?)
    } else {
        None
    };
    let password = if password_flag {
        Some(read_binary(buf, &mut cursor)?)
    } else {
        None
    };

    Ok(ConnectPacket {
        client_id,
        keep_alive,
        clean_start,
        protocol,
        will,
        properties,
        username,
        password,
    })
}

fn parse_publish(flags: u8, buf: &[u8], proto: ProtocolVersion) -> Result<PublishPacket> {
    let dup = (flags & 0b0000_1000) != 0;
    let retain = (flags & 0b0000_0001) != 0;
    let qos_bits = (flags & 0b0000_0110) >> 1;
    let qos = match qos_bits {
        0 => Qos::AtMostOnce,
        1 => Qos::AtLeastOnce,
        2 => Qos::ExactlyOnce,
        other => return Err(anyhow!("unsupported QoS {other}")),
    };
    if matches!(qos, Qos::AtMostOnce) && dup {
        anyhow::bail!("dup flag set for qos0 publish");
    }
    let mut cursor = 0usize;
    let topic = read_string(buf, &mut cursor)?;
    let message_id = match qos {
        Qos::AtMostOnce => None,
        _ => Some(read_u16(buf, &mut cursor)?),
    };
    let properties = if matches!(proto, ProtocolVersion::V5) {
        let props_len = decode_varint(buf, &mut cursor).unwrap_or(0);
        parse_publish_properties(buf, &mut cursor, props_len)?
    } else {
        PublishProperties::default()
    };
    let payload = buf[cursor..].to_vec();
    Ok(PublishPacket {
        topic,
        payload,
        qos,
        message_id,
        dup,
        retain,
        properties,
    })
}

pub fn parse_subscribe(buf: &[u8], proto: ProtocolVersion) -> Result<SubscribePacket> {
    let mut cursor = 0usize;
    let packet_id = read_u16(buf, &mut cursor)?;
    let mut user_properties = Vec::new();
    let mut subscription_identifier = None;
    if matches!(proto, ProtocolVersion::V5) {
        let props_len = decode_varint(buf, &mut cursor).unwrap_or(0);
        let props_end = cursor
            .checked_add(props_len as usize)
            .context("subscribe properties overflow")?;
        while cursor < props_end {
            let prop_id = read_u8(buf, &mut cursor)?;
            match prop_id {
                0x0B => {
                    if subscription_identifier.is_some() {
                        anyhow::bail!("duplicate subscription identifier");
                    }
                    subscription_identifier = decode_varint(buf, &mut cursor);
                }
                0x26 => {
                    let key = read_string(buf, &mut cursor)?;
                    let value = read_string(buf, &mut cursor)?;
                    user_properties.push((key, value));
                }
                _ => anyhow::bail!("unsupported subscribe property {prop_id}"),
            }
        }
        if cursor != props_end {
            anyhow::bail!("subscribe properties length mismatch");
        }
    }
    let mut filters = Vec::new();
    while cursor < buf.len() {
        let topic_filter = read_string(buf, &mut cursor)?;
        let opts = read_u8(buf, &mut cursor)?;
        if opts & 0b1100_0000 != 0 {
            anyhow::bail!("reserved subscription flags set");
        }
        let qos_bits = opts & 0x03;
        let qos = match qos_bits {
            0 => Qos::AtMostOnce,
            1 => Qos::AtLeastOnce,
            2 => Qos::ExactlyOnce,
            _ => return Err(anyhow!("invalid subscription QoS {qos_bits}")),
        };
        let no_local = (opts & 0b0000_0100) != 0;
        let retain_as_published = (opts & 0b0000_1000) != 0;
        let retain_handling = (opts >> 4) & 0x03;
        if retain_handling > 2 {
            anyhow::bail!("invalid retain handling {retain_handling}");
        }
        filters.push(SubscriptionRequest {
            topic_filter,
            qos,
            no_local,
            retain_as_published,
            retain_handling,
        });
    }
    Ok(SubscribePacket {
        packet_id,
        filters,
        subscription_identifier,
        user_properties,
    })
}

fn parse_unsubscribe(buf: &[u8], proto: ProtocolVersion) -> Result<UnsubscribePacket> {
    let mut cursor = 0usize;
    let packet_id = read_u16(buf, &mut cursor)?;
    let mut user_properties = Vec::new();
    if matches!(proto, ProtocolVersion::V5) {
        let props_len = decode_varint(buf, &mut cursor).unwrap_or(0);
        let props_end = cursor
            .checked_add(props_len as usize)
            .context("unsubscribe properties overflow")?;
        while cursor < props_end {
            let prop_id = read_u8(buf, &mut cursor)?;
            match prop_id {
                0x26 => {
                    let key = read_string(buf, &mut cursor)?;
                    let value = read_string(buf, &mut cursor)?;
                    user_properties.push((key, value));
                }
                _ => anyhow::bail!("unsupported unsubscribe property {prop_id}"),
            }
        }
    }
    let mut topics = Vec::new();
    while cursor < buf.len() {
        topics.push(read_string(buf, &mut cursor)?);
    }
    Ok(UnsubscribePacket {
        packet_id,
        topics,
        user_properties,
    })
}

pub fn parse_disconnect(buf: &[u8], proto: ProtocolVersion) -> Result<DisconnectPacket> {
    let mut cursor = 0usize;
    let mut reason_code = ReasonCodes::SUCCESS;
    let mut reason_string = None;
    let mut server_reference = None;
    if matches!(proto, ProtocolVersion::V5) {
        if !buf.is_empty() {
            reason_code = read_u8(buf, &mut cursor)?;
        }
        if cursor < buf.len() {
            let props_len = decode_varint(buf, &mut cursor).unwrap_or(0);
            let props_end = cursor
                .checked_add(props_len as usize)
                .context("disconnect properties overflow")?;
            while cursor < props_end {
                let prop_id = read_u8(buf, &mut cursor)?;
                match prop_id {
                    0x1F => {
                        reason_string = Some(read_string(buf, &mut cursor)?);
                    }
                    0x11 => {
                        let _ = read_u32(buf, &mut cursor)?;
                    }
                    0x1C => {
                        server_reference = Some(read_string(buf, &mut cursor)?);
                    }
                    0x26 => {
                        let _ = read_string(buf, &mut cursor)?;
                        let _ = read_string(buf, &mut cursor)?;
                    }
                    _ => anyhow::bail!("unsupported DISCONNECT property {prop_id}"),
                }
            }
            if cursor != props_end {
                anyhow::bail!("disconnect properties length mismatch");
            }
        }
    }
    Ok(DisconnectPacket {
        reason_code,
        reason_string,
        server_reference,
    })
}

fn parse_auth(buf: &[u8], proto: ProtocolVersion) -> Result<AuthPacket> {
    if !matches!(proto, ProtocolVersion::V5) {
        anyhow::bail!("AUTH is MQTT 5-only");
    }
    let mut cursor = 0usize;
    let reason_code = if !buf.is_empty() {
        read_u8(buf, &mut cursor)?
    } else {
        ReasonCodes::SUCCESS
    };
    let props_len = decode_varint(buf, &mut cursor).unwrap_or(0);
    let props_end = cursor
        .checked_add(props_len as usize)
        .context("auth properties overflow")?;
    let mut packet = AuthPacket {
        reason_code,
        ..Default::default()
    };
    while cursor < props_end {
        let prop_id = read_u8(buf, &mut cursor)?;
        match prop_id {
            0x15 => packet.auth_method = Some(read_string(buf, &mut cursor)?),
            0x16 => packet.auth_data = Some(read_binary(buf, &mut cursor)?),
            0x1F => packet.reason_string = Some(read_string(buf, &mut cursor)?),
            0x26 => {
                let key = read_string(buf, &mut cursor)?;
                let value = read_string(buf, &mut cursor)?;
                packet.user_properties.push((key, value));
            }
            _ => anyhow::bail!("unsupported AUTH property {prop_id}"),
        }
    }
    if cursor != props_end {
        anyhow::bail!("auth properties length mismatch");
    }
    Ok(packet)
}

fn parse_packet_id(buf: &[u8]) -> Result<u16> {
    let mut cursor = 0usize;
    read_u16(buf, &mut cursor)
}

fn read_u8(buf: &[u8], cursor: &mut usize) -> Result<u8> {
    if *cursor >= buf.len() {
        anyhow::bail!("unexpected end of buffer");
    }
    let v = buf[*cursor];
    *cursor += 1;
    Ok(v)
}

fn read_u16(buf: &[u8], cursor: &mut usize) -> Result<u16> {
    if *cursor + 1 >= buf.len() {
        anyhow::bail!("unexpected end of buffer");
    }
    let v = u16::from_be_bytes([buf[*cursor], buf[*cursor + 1]]);
    *cursor += 2;
    Ok(v)
}

fn read_u32(buf: &[u8], cursor: &mut usize) -> Result<u32> {
    if *cursor + 3 >= buf.len() {
        anyhow::bail!("unexpected end of buffer");
    }
    let v = u32::from_be_bytes([
        buf[*cursor],
        buf[*cursor + 1],
        buf[*cursor + 2],
        buf[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(v)
}

fn read_string(buf: &[u8], cursor: &mut usize) -> Result<String> {
    let len = read_u16(buf, cursor)? as usize;
    if *cursor + len > buf.len() {
        anyhow::bail!("unexpected end of buffer");
    }
    let s = std::str::from_utf8(&buf[*cursor..*cursor + len])
        .context("invalid utf8 in mqtt string")?
        .to_string();
    *cursor += len;
    Ok(s)
}

fn read_binary(buf: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_u16(buf, cursor)? as usize;
    if *cursor + len > buf.len() {
        anyhow::bail!("unexpected end of buffer");
    }
    let bytes = buf[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(bytes)
}

fn decode_varint(buf: &[u8], cursor: &mut usize) -> Option<u32> {
    let mut multiplier = 1u32;
    let mut value = 0u32;
    loop {
        if *cursor >= buf.len() {
            return None;
        }
        let byte = buf[*cursor];
        *cursor += 1;
        value = value.saturating_add(((byte & 0x7F) as u32) * multiplier);
        if (byte & 0x80) == 0 {
            break;
        }
        multiplier = multiplier.checked_mul(128)?;
    }
    Some(value)
}

async fn decode_remaining_length_stream<S: AsyncReadExt + Unpin>(stream: &mut S) -> Result<u32> {
    let mut multiplier = 1u32;
    let mut value = 0u32;
    loop {
        let mut buf = [0u8; 1];
        stream.read_exact(&mut buf).await?;
        let byte = buf[0];
        value = value.saturating_add(((byte & 0x7F) as u32) * multiplier);
        if (byte & 0x80) == 0 {
            break;
        }
        multiplier = multiplier
            .checked_mul(128)
            .context("remaining length overflow")?;
    }
    Ok(value)
}

fn parse_connect_properties(
    buf: &[u8],
    cursor: &mut usize,
    props_len: u32,
) -> Result<ConnectProperties> {
    let mut props = ConnectProperties::default();
    let props_end = (*cursor)
        .checked_add(props_len as usize)
        .context("connect properties overflow")?;
    while *cursor < props_end {
        let prop_id = read_u8(buf, cursor)?;
        match prop_id {
            0x11 => props.session_expiry_interval = Some(read_u32(buf, cursor)?),
            0x21 => props.receive_maximum = Some(read_u16(buf, cursor)?),
            0x27 => props.max_packet_size = Some(read_u32(buf, cursor)?),
            0x22 => props.topic_alias_max = Some(read_u16(buf, cursor)?),
            0x15 => props.auth_method = Some(read_string(buf, cursor)?),
            0x16 => props.auth_data = Some(read_binary(buf, cursor)?),
            0x26 => {
                let key = read_string(buf, cursor)?;
                let value = read_string(buf, cursor)?;
                props.user_properties.push((key, value));
            }
            _ => anyhow::bail!("unsupported CONNECT property {prop_id}"),
        }
    }
    if *cursor != props_end {
        anyhow::bail!("connect properties length mismatch");
    }
    Ok(props)
}

fn parse_will_properties(buf: &[u8], cursor: &mut usize) -> Result<WillProperties> {
    let props_len = decode_varint(buf, cursor).unwrap_or(0);
    let props_end = (*cursor)
        .checked_add(props_len as usize)
        .context("will properties overflow")?;
    let mut props = WillProperties::default();
    while *cursor < props_end {
        let prop_id = read_u8(buf, cursor)?;
        match prop_id {
            0x18 => props.delay_interval = Some(read_u32(buf, cursor)?),
            0x01 => props.payload_format_indicator = Some(read_u8(buf, cursor)?),
            0x02 => props.message_expiry_interval = Some(read_u32(buf, cursor)?),
            0x03 => props.content_type = Some(read_string(buf, cursor)?),
            0x08 => props.response_topic = Some(read_string(buf, cursor)?),
            0x09 => props.correlation_data = Some(read_binary(buf, cursor)?),
            0x26 => {
                let key = read_string(buf, cursor)?;
                let value = read_string(buf, cursor)?;
                props.user_properties.push((key, value));
            }
            _ => anyhow::bail!("unsupported will property {prop_id}"),
        }
    }
    Ok(props)
}

fn parse_publish_properties(
    buf: &[u8],
    cursor: &mut usize,
    props_len: u32,
) -> Result<PublishProperties> {
    let mut props = PublishProperties::default();
    let props_end = (*cursor)
        .checked_add(props_len as usize)
        .context("publish properties overflow")?;
    while *cursor < props_end {
        let prop_id = read_u8(buf, cursor)?;
        match prop_id {
            0x01 => props.payload_format_indicator = Some(read_u8(buf, cursor)?),
            0x02 => props.message_expiry_interval = Some(read_u32(buf, cursor)?),
            0x08 => props.response_topic = Some(read_string(buf, cursor)?),
            0x09 => props.correlation_data = Some(read_binary(buf, cursor)?),
            0x0B => props.subscription_identifier = Some(read_u32(buf, cursor)?),
            0x23 => props.topic_alias = Some(read_u16(buf, cursor)?),
            0x26 => {
                let key = read_string(buf, cursor)?;
                let value = read_string(buf, cursor)?;
                props.user_properties.push((key, value));
            }
            _ => anyhow::bail!("unsupported publish property {prop_id}"),
        }
    }
    if *cursor != props_end {
        anyhow::bail!("publish properties length mismatch");
    }
    Ok(props)
}

fn encode_publish_properties(props: &PublishProperties) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Some(pfi) = props.payload_format_indicator {
        buf.push(0x01);
        buf.push(pfi);
    }
    if let Some(expiry) = props.message_expiry_interval {
        buf.push(0x02);
        buf.extend_from_slice(&expiry.to_be_bytes());
    }
    if let Some(alias) = props.topic_alias {
        buf.push(0x23);
        buf.extend_from_slice(&alias.to_be_bytes());
    }
    if let Some(ref topic) = props.response_topic {
        buf.push(0x08);
        buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        buf.extend_from_slice(topic.as_bytes());
    }
    if let Some(ref corr) = props.correlation_data {
        buf.push(0x09);
        buf.extend_from_slice(&(corr.len() as u16).to_be_bytes());
        buf.extend_from_slice(corr);
    }
    if let Some(sub_id) = props.subscription_identifier {
        buf.push(0x0B);
        buf.extend(encode_remaining_length(sub_id as usize));
    }
    for (k, v) in &props.user_properties {
        buf.push(0x26);
        buf.extend_from_slice(&(k.len() as u16).to_be_bytes());
        buf.extend_from_slice(k.as_bytes());
        buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
        buf.extend_from_slice(v.as_bytes());
    }
    buf
}

fn encode_remaining_length(mut len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    out
}

async fn write_puback_variant<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    proto: ProtocolVersion,
    packet_type: u8,
    mid: u16,
    code: InternalCode,
) -> Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&mid.to_be_bytes());
    if matches!(proto, ProtocolVersion::V5) {
        payload.push(map_reason(proto, code));
        payload.push(0); // property length
    }
    let mut header = vec![packet_type];
    header.extend(encode_remaining_length(payload.len()));
    header.extend(payload);
    stream.write_all(&header).await?;
    Ok(())
}
