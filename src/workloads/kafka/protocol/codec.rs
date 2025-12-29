//! Kafka wire protocol codec for request/response framing.
//!
//! This module handles the framing layer of the Kafka protocol:
//! - 4-byte length prefix (big-endian)
//! - Request header parsing
//! - Response header encoding

use super::{
    types::{
        read_compact_string, read_int16, read_int32, read_string, read_tagged_fields,
        write_compact_string, write_empty_tagged_fields, write_int16, write_int32, write_string,
        write_tagged_fields,
    },
    RequestHeader, ResponseHeader, TaggedFields,
};
use crate::workloads::kafka::ApiKey;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self, Cursor};

// ---------------------------------------------------------------------------
// Frame Constants
// ---------------------------------------------------------------------------

/// Maximum frame size (100MB).
pub const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

/// Minimum frame size (header only).
pub const MIN_FRAME_SIZE: usize = 4;

// ---------------------------------------------------------------------------
// Frame Decoder
// ---------------------------------------------------------------------------

/// Kafka frame decoder state machine.
#[derive(Debug, Default)]
pub struct KafkaDecoder {
    /// Current decode state.
    state: DecodeState,
}

#[derive(Debug, Default)]
enum DecodeState {
    #[default]
    ReadingLength,
    ReadingPayload {
        length: usize,
    },
}

impl KafkaDecoder {
    /// Create a new decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode a frame from the buffer.
    ///
    /// Returns `Ok(Some(frame))` if a complete frame was decoded,
    /// `Ok(None)` if more data is needed, or `Err` on protocol error.
    pub fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Bytes>> {
        loop {
            match self.state {
                DecodeState::ReadingLength => {
                    if buf.len() < 4 {
                        return Ok(None);
                    }
                    let length = buf.get_u32() as usize;
                    if length > MAX_FRAME_SIZE {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("frame too large: {} bytes", length),
                        ));
                    }
                    self.state = DecodeState::ReadingPayload { length };
                }
                DecodeState::ReadingPayload { length } => {
                    if buf.len() < length {
                        return Ok(None);
                    }
                    let frame = buf.split_to(length).freeze();
                    self.state = DecodeState::ReadingLength;
                    return Ok(Some(frame));
                }
            }
        }
    }

    /// Reset the decoder state.
    pub fn reset(&mut self) {
        self.state = DecodeState::ReadingLength;
    }
}

// ---------------------------------------------------------------------------
// Frame Encoder
// ---------------------------------------------------------------------------

/// Kafka frame encoder.
#[derive(Debug, Default)]
pub struct KafkaEncoder;

impl KafkaEncoder {
    /// Create a new encoder.
    pub fn new() -> Self {
        Self
    }

    /// Encode a frame with length prefix.
    pub fn encode(&self, payload: &[u8], buf: &mut BytesMut) {
        buf.put_u32(payload.len() as u32);
        buf.put_slice(payload);
    }

    /// Encode a frame with length prefix, returning bytes.
    pub fn encode_to_bytes(&self, payload: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(4 + payload.len());
        self.encode(payload, &mut buf);
        buf.freeze()
    }
}

// ---------------------------------------------------------------------------
// Request Header Parsing
// ---------------------------------------------------------------------------

/// Parse a request header from a buffer.
pub fn parse_request_header(buf: &mut impl Buf) -> io::Result<RequestHeader> {
    let api_key_raw = buf.get_i16();
    let api_key = ApiKey::from_i16(api_key_raw).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown API key: {}", api_key_raw),
        )
    })?;

    let api_version = buf.get_i16();
    let correlation_id = buf.get_i32();

    // Read client_id - the format depends on whether we're using flexible versions
    // For simplicity, we'll detect based on API version
    let is_flexible = is_flexible_version(api_key, api_version);

    let remaining = buf.chunk();
    let mut cursor = Cursor::new(remaining);

    let client_id = if is_flexible {
        read_compact_string(&mut cursor)?
    } else {
        read_string(&mut cursor)?
    };

    // Advance the buffer
    let consumed = cursor.position() as usize;
    buf.advance(consumed);

    // Read tagged fields for flexible versions
    let tagged_fields = if is_flexible {
        let remaining = buf.chunk();
        let mut cursor = Cursor::new(remaining);
        let fields = read_tagged_fields(&mut cursor)?;
        buf.advance(cursor.position() as usize);
        fields
    } else {
        TaggedFields::default()
    };

    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
        tagged_fields,
    })
}

/// Parse a request header from bytes.
pub fn parse_request_header_from_bytes(data: &[u8]) -> io::Result<(RequestHeader, usize)> {
    let mut cursor = Cursor::new(data);

    let api_key_raw = read_int16(&mut cursor)?;
    let api_key = ApiKey::from_i16(api_key_raw).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown API key: {}", api_key_raw),
        )
    })?;

    let api_version = read_int16(&mut cursor)?;
    let correlation_id = read_int32(&mut cursor)?;

    let is_flexible = is_flexible_version(api_key, api_version);

    let client_id = if is_flexible {
        read_compact_string(&mut cursor)?
    } else {
        read_string(&mut cursor)?
    };

    let tagged_fields = if is_flexible {
        read_tagged_fields(&mut cursor)?
    } else {
        TaggedFields::default()
    };

    let consumed = cursor.position() as usize;

    Ok((
        RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tagged_fields,
        },
        consumed,
    ))
}

// ---------------------------------------------------------------------------
// Response Header Encoding
// ---------------------------------------------------------------------------

/// Encode a response header.
pub fn encode_response_header(header: &ResponseHeader, is_flexible: bool, buf: &mut impl BufMut) {
    buf.put_i32(header.correlation_id);
    if is_flexible {
        // Write tagged fields
        let mut temp = Vec::new();
        write_tagged_fields(&mut temp, &header.tagged_fields).unwrap();
        buf.put_slice(&temp);
    }
}

/// Encode a response header to bytes.
pub fn encode_response_header_to_bytes(header: &ResponseHeader, is_flexible: bool) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    write_int32(&mut buf, header.correlation_id).unwrap();
    if is_flexible {
        write_tagged_fields(&mut buf, &header.tagged_fields).unwrap();
    }
    buf
}

// ---------------------------------------------------------------------------
// Version Detection
// ---------------------------------------------------------------------------

/// Check if an API version uses flexible encoding.
///
/// Flexible encoding uses compact strings and tagged fields.
pub fn is_flexible_version(api_key: ApiKey, version: i16) -> bool {
    // Most APIs become flexible at a specific version
    match api_key {
        ApiKey::Produce => version >= 9,
        ApiKey::Fetch => version >= 12,
        ApiKey::ListOffsets => version >= 6,
        ApiKey::Metadata => version >= 9,
        ApiKey::OffsetCommit => version >= 8,
        ApiKey::OffsetFetch => version >= 6,
        ApiKey::FindCoordinator => version >= 3,
        ApiKey::JoinGroup => version >= 6,
        ApiKey::Heartbeat => version >= 4,
        ApiKey::LeaveGroup => version >= 4,
        ApiKey::SyncGroup => version >= 4,
        ApiKey::DescribeGroups => version >= 5,
        ApiKey::ListGroups => version >= 3,
        ApiKey::SaslHandshake => false, // Never flexible
        ApiKey::ApiVersions => version >= 3,
        ApiKey::CreateTopics => version >= 5,
        ApiKey::DeleteTopics => version >= 4,
        ApiKey::InitProducerId => version >= 2,
        ApiKey::AddPartitionsToTxn => version >= 3,
        ApiKey::EndTxn => version >= 3,
        ApiKey::SaslAuthenticate => version >= 2,
        _ => false,
    }
}

/// Get the response header version for an API.
pub fn response_header_version(api_key: ApiKey, version: i16) -> i16 {
    if is_flexible_version(api_key, version) {
        1
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Request Builder
// ---------------------------------------------------------------------------

/// Builder for constructing Kafka requests.
pub struct RequestBuilder {
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    payload: Vec<u8>,
}

impl RequestBuilder {
    /// Create a new request builder.
    pub fn new(api_key: ApiKey, api_version: i16, correlation_id: i32) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id: None,
            payload: Vec::new(),
        }
    }

    /// Set the client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Set the request payload.
    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    /// Build the complete request frame.
    pub fn build(self) -> io::Result<Bytes> {
        let mut buf = Vec::new();

        // Write header
        write_int16(&mut buf, self.api_key.as_i16())?;
        write_int16(&mut buf, self.api_version)?;
        write_int32(&mut buf, self.correlation_id)?;

        let is_flexible = is_flexible_version(self.api_key, self.api_version);

        if is_flexible {
            write_compact_string(&mut buf, self.client_id.as_deref())?;
            write_empty_tagged_fields(&mut buf)?;
        } else {
            write_string(&mut buf, self.client_id.as_deref())?;
        }

        // Write payload
        buf.extend_from_slice(&self.payload);

        // Add length prefix
        let mut frame = Vec::with_capacity(4 + buf.len());
        write_int32(&mut frame, buf.len() as i32)?;
        frame.extend_from_slice(&buf);

        Ok(Bytes::from(frame))
    }
}

// ---------------------------------------------------------------------------
// Response Builder
// ---------------------------------------------------------------------------

/// Builder for constructing Kafka responses.
pub struct ResponseBuilder {
    correlation_id: i32,
    is_flexible: bool,
    payload: Vec<u8>,
}

impl ResponseBuilder {
    /// Create a new response builder.
    pub fn new(correlation_id: i32, is_flexible: bool) -> Self {
        Self {
            correlation_id,
            is_flexible,
            payload: Vec::new(),
        }
    }

    /// Set the response payload.
    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    /// Build the complete response frame.
    pub fn build(self) -> io::Result<Bytes> {
        let mut buf = Vec::new();

        // Write header
        write_int32(&mut buf, self.correlation_id)?;
        if self.is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        // Write payload
        buf.extend_from_slice(&self.payload);

        // Add length prefix
        let mut frame = Vec::with_capacity(4 + buf.len());
        write_int32(&mut frame, buf.len() as i32)?;
        frame.extend_from_slice(&buf);

        Ok(Bytes::from(frame))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decoder_single_frame() {
        let mut decoder = KafkaDecoder::new();
        let mut buf = BytesMut::new();

        // Write a frame
        buf.put_u32(5); // length
        buf.put_slice(b"hello");

        let frame = decoder.decode(&mut buf).unwrap();
        assert_eq!(frame, Some(Bytes::from_static(b"hello")));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decoder_partial_length() {
        let mut decoder = KafkaDecoder::new();
        let mut buf = BytesMut::new();

        // Write partial length
        buf.put_u8(0);
        buf.put_u8(0);

        let frame = decoder.decode(&mut buf).unwrap();
        assert_eq!(frame, None);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_decoder_partial_payload() {
        let mut decoder = KafkaDecoder::new();
        let mut buf = BytesMut::new();

        // Write length and partial payload
        buf.put_u32(5);
        buf.put_slice(b"hel");

        let frame = decoder.decode(&mut buf).unwrap();
        assert_eq!(frame, None);

        // Complete the payload
        buf.put_slice(b"lo");
        let frame = decoder.decode(&mut buf).unwrap();
        assert_eq!(frame, Some(Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_decoder_multiple_frames() {
        let mut decoder = KafkaDecoder::new();
        let mut buf = BytesMut::new();

        // Write two frames
        buf.put_u32(5);
        buf.put_slice(b"hello");
        buf.put_u32(5);
        buf.put_slice(b"world");

        let frame1 = decoder.decode(&mut buf).unwrap();
        assert_eq!(frame1, Some(Bytes::from_static(b"hello")));

        let frame2 = decoder.decode(&mut buf).unwrap();
        assert_eq!(frame2, Some(Bytes::from_static(b"world")));

        assert!(buf.is_empty());
    }

    #[test]
    fn test_decoder_frame_too_large() {
        let mut decoder = KafkaDecoder::new();
        let mut buf = BytesMut::new();

        // Write oversized length
        buf.put_u32((MAX_FRAME_SIZE + 1) as u32);

        let result = decoder.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_encoder() {
        let encoder = KafkaEncoder::new();
        let payload = b"hello";

        let frame = encoder.encode_to_bytes(payload);

        // Verify frame format
        assert_eq!(frame.len(), 4 + 5);
        assert_eq!(&frame[0..4], &[0, 0, 0, 5]); // length prefix
        assert_eq!(&frame[4..], b"hello");
    }

    #[test]
    fn test_request_builder() {
        let request = RequestBuilder::new(ApiKey::Metadata, 0, 12345)
            .client_id("test-client")
            .build()
            .unwrap();

        // Should have length prefix
        assert!(request.len() > 4);

        // Parse back the header
        let data = &request[4..]; // Skip length prefix
        let (header, _) = parse_request_header_from_bytes(data).unwrap();

        assert_eq!(header.api_key, ApiKey::Metadata);
        assert_eq!(header.api_version, 0);
        assert_eq!(header.correlation_id, 12345);
        assert_eq!(header.client_id, Some("test-client".to_string()));
    }

    #[test]
    fn test_response_builder() {
        let response = ResponseBuilder::new(12345, false)
            .payload(vec![1, 2, 3, 4, 5])
            .build()
            .unwrap();

        // Should have length prefix
        assert!(response.len() > 4);

        // Verify correlation ID
        let data = &response[4..]; // Skip length prefix
        let correlation_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(correlation_id, 12345);
    }

    #[test]
    fn test_is_flexible_version() {
        // Non-flexible
        assert!(!is_flexible_version(ApiKey::Metadata, 0));
        assert!(!is_flexible_version(ApiKey::Metadata, 8));

        // Flexible
        assert!(is_flexible_version(ApiKey::Metadata, 9));
        assert!(is_flexible_version(ApiKey::Metadata, 12));

        // SaslHandshake never flexible
        assert!(!is_flexible_version(ApiKey::SaslHandshake, 0));
        assert!(!is_flexible_version(ApiKey::SaslHandshake, 1));
    }
}
