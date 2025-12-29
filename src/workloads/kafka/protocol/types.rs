//! Kafka wire protocol primitive types.
//!
//! This module implements the primitive types used in the Kafka protocol:
//! - Fixed-width integers (int8, int16, int32, int64)
//! - Variable-length integers (varint, varlong) with zigzag encoding
//! - Strings (length-prefixed and compact)
//! - Byte arrays (length-prefixed and compact)
//! - UUIDs
//! - Arrays (length-prefixed and compact)

use bytes::{Buf, BufMut};
use std::io::{self, Read, Write};

// ---------------------------------------------------------------------------
// Varint Encoding (ZigZag)
// ---------------------------------------------------------------------------

/// Encode a signed 32-bit integer as a varint with zigzag encoding.
pub fn encode_varint(value: i32) -> Vec<u8> {
    let unsigned = ((value << 1) ^ (value >> 31)) as u32;
    encode_unsigned_varint(unsigned)
}

/// Decode a varint with zigzag encoding to a signed 32-bit integer.
pub fn decode_varint<R: Read>(reader: &mut R) -> io::Result<i32> {
    let unsigned = decode_unsigned_varint(reader)?;
    Ok(((unsigned >> 1) as i32) ^ (-((unsigned & 1) as i32)))
}

/// Encode an unsigned 32-bit integer as a varint.
pub fn encode_unsigned_varint(mut value: u32) -> Vec<u8> {
    let mut result = Vec::with_capacity(5);
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

/// Decode an unsigned varint.
pub fn decode_unsigned_varint<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut result: u32 = 0;
    let mut shift = 0;
    let mut buf = [0u8; 1];

    loop {
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 35 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint too long",
            ));
        }
    }
    Ok(result)
}

/// Encode a signed 64-bit integer as a varlong with zigzag encoding.
pub fn encode_varlong(value: i64) -> Vec<u8> {
    let unsigned = ((value << 1) ^ (value >> 63)) as u64;
    encode_unsigned_varlong(unsigned)
}

/// Decode a varlong with zigzag encoding to a signed 64-bit integer.
pub fn decode_varlong<R: Read>(reader: &mut R) -> io::Result<i64> {
    let unsigned = decode_unsigned_varlong(reader)?;
    Ok(((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64)))
}

/// Encode an unsigned 64-bit integer as a varlong.
pub fn encode_unsigned_varlong(mut value: u64) -> Vec<u8> {
    let mut result = Vec::with_capacity(10);
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

/// Decode an unsigned varlong.
pub fn decode_unsigned_varlong<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut buf = [0u8; 1];

    loop {
        reader.read_exact(&mut buf)?;
        let byte = buf[0];
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 70 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varlong too long",
            ));
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Fixed-width Integer Types
// ---------------------------------------------------------------------------

/// Read a big-endian i8.
pub fn read_int8<R: Read>(reader: &mut R) -> io::Result<i8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0] as i8)
}

/// Write a big-endian i8.
pub fn write_int8<W: Write>(writer: &mut W, value: i8) -> io::Result<()> {
    writer.write_all(&[value as u8])
}

/// Read a big-endian i16.
pub fn read_int16<R: Read>(reader: &mut R) -> io::Result<i16> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

/// Write a big-endian i16.
pub fn write_int16<W: Write>(writer: &mut W, value: i16) -> io::Result<()> {
    writer.write_all(&value.to_be_bytes())
}

/// Read a big-endian i32.
pub fn read_int32<R: Read>(reader: &mut R) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

/// Write a big-endian i32.
pub fn write_int32<W: Write>(writer: &mut W, value: i32) -> io::Result<()> {
    writer.write_all(&value.to_be_bytes())
}

/// Read a big-endian i64.
pub fn read_int64<R: Read>(reader: &mut R) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(i64::from_be_bytes(buf))
}

/// Write a big-endian i64.
pub fn write_int64<W: Write>(writer: &mut W, value: i64) -> io::Result<()> {
    writer.write_all(&value.to_be_bytes())
}

/// Read a big-endian u32.
pub fn read_uint32<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

/// Write a big-endian u32.
pub fn write_uint32<W: Write>(writer: &mut W, value: u32) -> io::Result<()> {
    writer.write_all(&value.to_be_bytes())
}

// ---------------------------------------------------------------------------
// Boolean
// ---------------------------------------------------------------------------

/// Read a boolean.
pub fn read_boolean<R: Read>(reader: &mut R) -> io::Result<bool> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0] != 0)
}

/// Write a boolean.
pub fn write_boolean<W: Write>(writer: &mut W, value: bool) -> io::Result<()> {
    writer.write_all(&[if value { 1 } else { 0 }])
}

// ---------------------------------------------------------------------------
// String Types
// ---------------------------------------------------------------------------

/// Read a length-prefixed string (non-flexible).
pub fn read_string<R: Read>(reader: &mut R) -> io::Result<Option<String>> {
    let len = read_int16(reader)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Write a length-prefixed string (non-flexible).
pub fn write_string<W: Write>(writer: &mut W, value: Option<&str>) -> io::Result<()> {
    match value {
        None => write_int16(writer, -1),
        Some(s) => {
            let bytes = s.as_bytes();
            if bytes.len() > i16::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "string too long",
                ));
            }
            write_int16(writer, bytes.len() as i16)?;
            writer.write_all(bytes)
        }
    }
}

/// Read a compact string (flexible versions).
pub fn read_compact_string<R: Read>(reader: &mut R) -> io::Result<Option<String>> {
    let len = decode_unsigned_varint(reader)? as usize;
    if len == 0 {
        return Ok(None);
    }
    let len = len - 1; // Length is encoded as actual_length + 1
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Write a compact string (flexible versions).
pub fn write_compact_string<W: Write>(writer: &mut W, value: Option<&str>) -> io::Result<()> {
    match value {
        None => {
            let encoded = encode_unsigned_varint(0);
            writer.write_all(&encoded)
        }
        Some(s) => {
            let bytes = s.as_bytes();
            let encoded = encode_unsigned_varint((bytes.len() + 1) as u32);
            writer.write_all(&encoded)?;
            writer.write_all(bytes)
        }
    }
}

/// Read a non-nullable string (non-flexible).
pub fn read_non_nullable_string<R: Read>(reader: &mut R) -> io::Result<String> {
    read_string(reader)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "expected non-null string"))
}

/// Read a non-nullable compact string (flexible versions).
pub fn read_compact_non_nullable_string<R: Read>(reader: &mut R) -> io::Result<String> {
    read_compact_string(reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "expected non-null compact string",
        )
    })
}

// ---------------------------------------------------------------------------
// Byte Array Types
// ---------------------------------------------------------------------------

/// Read a length-prefixed byte array (non-flexible).
pub fn read_bytes<R: Read>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let len = read_int32(reader)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(Some(buf))
}

/// Write a length-prefixed byte array (non-flexible).
pub fn write_bytes<W: Write>(writer: &mut W, value: Option<&[u8]>) -> io::Result<()> {
    match value {
        None => write_int32(writer, -1),
        Some(bytes) => {
            if bytes.len() > i32::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "byte array too long",
                ));
            }
            write_int32(writer, bytes.len() as i32)?;
            writer.write_all(bytes)
        }
    }
}

/// Read a compact byte array (flexible versions).
pub fn read_compact_bytes<R: Read>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let len = decode_unsigned_varint(reader)? as usize;
    if len == 0 {
        return Ok(None);
    }
    let len = len - 1; // Length is encoded as actual_length + 1
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(Some(buf))
}

/// Write a compact byte array (flexible versions).
pub fn write_compact_bytes<W: Write>(writer: &mut W, value: Option<&[u8]>) -> io::Result<()> {
    match value {
        None => {
            let encoded = encode_unsigned_varint(0);
            writer.write_all(&encoded)
        }
        Some(bytes) => {
            let encoded = encode_unsigned_varint((bytes.len() + 1) as u32);
            writer.write_all(&encoded)?;
            writer.write_all(bytes)
        }
    }
}

// ---------------------------------------------------------------------------
// Array Types
// ---------------------------------------------------------------------------

/// Read a length-prefixed array (non-flexible).
pub fn read_array<R: Read, T, F>(reader: &mut R, read_element: F) -> io::Result<Option<Vec<T>>>
where
    F: Fn(&mut R) -> io::Result<T>,
{
    let len = read_int32(reader)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let mut result = Vec::with_capacity(len);
    for _ in 0..len {
        result.push(read_element(reader)?);
    }
    Ok(Some(result))
}

/// Write a length-prefixed array (non-flexible).
pub fn write_array<W: Write, T, F>(
    writer: &mut W,
    value: Option<&[T]>,
    write_element: F,
) -> io::Result<()>
where
    F: Fn(&mut W, &T) -> io::Result<()>,
{
    match value {
        None => write_int32(writer, -1),
        Some(elements) => {
            if elements.len() > i32::MAX as usize {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "array too long"));
            }
            write_int32(writer, elements.len() as i32)?;
            for element in elements {
                write_element(writer, element)?;
            }
            Ok(())
        }
    }
}

/// Read a compact array (flexible versions).
pub fn read_compact_array<R: Read, T, F>(
    reader: &mut R,
    read_element: F,
) -> io::Result<Option<Vec<T>>>
where
    F: Fn(&mut R) -> io::Result<T>,
{
    let len = decode_unsigned_varint(reader)? as usize;
    if len == 0 {
        return Ok(None);
    }
    let len = len - 1; // Length is encoded as actual_length + 1
    let mut result = Vec::with_capacity(len);
    for _ in 0..len {
        result.push(read_element(reader)?);
    }
    Ok(Some(result))
}

/// Write a compact array (flexible versions).
pub fn write_compact_array<W: Write, T, F>(
    writer: &mut W,
    value: Option<&[T]>,
    write_element: F,
) -> io::Result<()>
where
    F: Fn(&mut W, &T) -> io::Result<()>,
{
    match value {
        None => {
            let encoded = encode_unsigned_varint(0);
            writer.write_all(&encoded)
        }
        Some(elements) => {
            let encoded = encode_unsigned_varint((elements.len() + 1) as u32);
            writer.write_all(&encoded)?;
            for element in elements {
                write_element(writer, element)?;
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

/// Read a UUID (16 bytes).
pub fn read_uuid<R: Read>(reader: &mut R) -> io::Result<uuid::Uuid> {
    let mut buf = [0u8; 16];
    reader.read_exact(&mut buf)?;
    Ok(uuid::Uuid::from_bytes(buf))
}

/// Write a UUID (16 bytes).
pub fn write_uuid<W: Write>(writer: &mut W, value: uuid::Uuid) -> io::Result<()> {
    writer.write_all(value.as_bytes())
}

// ---------------------------------------------------------------------------
// Tagged Fields
// ---------------------------------------------------------------------------

use super::TaggedFields;

/// Read tagged fields.
pub fn read_tagged_fields<R: Read>(reader: &mut R) -> io::Result<TaggedFields> {
    let num_fields = decode_unsigned_varint(reader)?;
    let mut fields = TaggedFields::new();

    for _ in 0..num_fields {
        let tag = decode_unsigned_varint(reader)?;
        let size = decode_unsigned_varint(reader)? as usize;
        let mut data = vec![0u8; size];
        reader.read_exact(&mut data)?;
        fields.push(tag, data);
    }

    Ok(fields)
}

/// Write tagged fields.
pub fn write_tagged_fields<W: Write>(writer: &mut W, fields: &TaggedFields) -> io::Result<()> {
    let encoded = encode_unsigned_varint(fields.len() as u32);
    writer.write_all(&encoded)?;

    for field in fields.iter() {
        let tag_encoded = encode_unsigned_varint(field.tag);
        writer.write_all(&tag_encoded)?;
        let size_encoded = encode_unsigned_varint(field.data.len() as u32);
        writer.write_all(&size_encoded)?;
        writer.write_all(&field.data)?;
    }

    Ok(())
}

/// Write empty tagged fields.
pub fn write_empty_tagged_fields<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(&[0])
}

// ---------------------------------------------------------------------------
// Buffer-based helpers (using bytes crate)
// ---------------------------------------------------------------------------

/// Extension trait for reading Kafka types from bytes::Buf.
pub trait KafkaBufExt: Buf {
    fn get_kafka_int8(&mut self) -> i8 {
        self.get_i8()
    }

    fn get_kafka_int16(&mut self) -> i16 {
        self.get_i16()
    }

    fn get_kafka_int32(&mut self) -> i32 {
        self.get_i32()
    }

    fn get_kafka_int64(&mut self) -> i64 {
        self.get_i64()
    }

    fn get_kafka_boolean(&mut self) -> bool {
        self.get_u8() != 0
    }

    fn get_kafka_varint(&mut self) -> i32 {
        let unsigned = self.get_kafka_unsigned_varint();
        ((unsigned >> 1) as i32) ^ (-((unsigned & 1) as i32))
    }

    fn get_kafka_unsigned_varint(&mut self) -> u32 {
        let mut result: u32 = 0;
        let mut shift = 0;
        loop {
            let byte = self.get_u8();
            result |= ((byte & 0x7F) as u32) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        result
    }

    fn get_kafka_varlong(&mut self) -> i64 {
        let unsigned = self.get_kafka_unsigned_varlong();
        ((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64))
    }

    fn get_kafka_unsigned_varlong(&mut self) -> u64 {
        let mut result: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = self.get_u8();
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        result
    }

    fn get_kafka_string(&mut self) -> Option<String> {
        let len = self.get_i16();
        if len < 0 {
            return None;
        }
        let len = len as usize;
        let mut buf = vec![0u8; len];
        self.copy_to_slice(&mut buf);
        String::from_utf8(buf).ok()
    }

    fn get_kafka_compact_string(&mut self) -> Option<String> {
        let len = self.get_kafka_unsigned_varint() as usize;
        if len == 0 {
            return None;
        }
        let len = len - 1;
        let mut buf = vec![0u8; len];
        self.copy_to_slice(&mut buf);
        String::from_utf8(buf).ok()
    }

    fn get_kafka_bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.get_i32();
        if len < 0 {
            return None;
        }
        let len = len as usize;
        let mut buf = vec![0u8; len];
        self.copy_to_slice(&mut buf);
        Some(buf)
    }

    fn get_kafka_compact_bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.get_kafka_unsigned_varint() as usize;
        if len == 0 {
            return None;
        }
        let len = len - 1;
        let mut buf = vec![0u8; len];
        self.copy_to_slice(&mut buf);
        Some(buf)
    }
}

impl<T: Buf> KafkaBufExt for T {}

/// Extension trait for writing Kafka types to bytes::BufMut.
pub trait KafkaBufMutExt: BufMut {
    fn put_kafka_int8(&mut self, value: i8) {
        self.put_i8(value);
    }

    fn put_kafka_int16(&mut self, value: i16) {
        self.put_i16(value);
    }

    fn put_kafka_int32(&mut self, value: i32) {
        self.put_i32(value);
    }

    fn put_kafka_int64(&mut self, value: i64) {
        self.put_i64(value);
    }

    fn put_kafka_boolean(&mut self, value: bool) {
        self.put_u8(if value { 1 } else { 0 });
    }

    fn put_kafka_varint(&mut self, value: i32) {
        let unsigned = ((value << 1) ^ (value >> 31)) as u32;
        self.put_kafka_unsigned_varint(unsigned);
    }

    fn put_kafka_unsigned_varint(&mut self, mut value: u32) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            self.put_u8(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn put_kafka_varlong(&mut self, value: i64) {
        let unsigned = ((value << 1) ^ (value >> 63)) as u64;
        self.put_kafka_unsigned_varlong(unsigned);
    }

    fn put_kafka_unsigned_varlong(&mut self, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            self.put_u8(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn put_kafka_string(&mut self, value: Option<&str>) {
        match value {
            None => self.put_i16(-1),
            Some(s) => {
                let bytes = s.as_bytes();
                self.put_i16(bytes.len() as i16);
                self.put_slice(bytes);
            }
        }
    }

    fn put_kafka_compact_string(&mut self, value: Option<&str>) {
        match value {
            None => self.put_kafka_unsigned_varint(0),
            Some(s) => {
                let bytes = s.as_bytes();
                self.put_kafka_unsigned_varint((bytes.len() + 1) as u32);
                self.put_slice(bytes);
            }
        }
    }

    fn put_kafka_bytes(&mut self, value: Option<&[u8]>) {
        match value {
            None => self.put_i32(-1),
            Some(bytes) => {
                self.put_i32(bytes.len() as i32);
                self.put_slice(bytes);
            }
        }
    }

    fn put_kafka_compact_bytes(&mut self, value: Option<&[u8]>) {
        match value {
            None => self.put_kafka_unsigned_varint(0),
            Some(bytes) => {
                self.put_kafka_unsigned_varint((bytes.len() + 1) as u32);
                self.put_slice(bytes);
            }
        }
    }

    fn put_kafka_empty_tagged_fields(&mut self) {
        self.put_u8(0);
    }
}

impl<T: BufMut> KafkaBufMutExt for T {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_varint_encoding() {
        let test_cases = [0, 1, -1, 127, 128, 255, 256, -128, i32::MAX, i32::MIN];

        for value in test_cases {
            let encoded = encode_varint(value);
            let mut cursor = Cursor::new(encoded);
            let decoded = decode_varint(&mut cursor).unwrap();
            assert_eq!(decoded, value, "Failed for value {}", value);
        }
    }

    #[test]
    fn test_varlong_encoding() {
        let test_cases = [0i64, 1, -1, 127, 128, 255, 256, -128, i64::MAX, i64::MIN];

        for value in test_cases {
            let encoded = encode_varlong(value);
            let mut cursor = Cursor::new(encoded);
            let decoded = decode_varlong(&mut cursor).unwrap();
            assert_eq!(decoded, value, "Failed for value {}", value);
        }
    }

    #[test]
    fn test_string_encoding() {
        let mut buf = Vec::new();

        // Non-null string
        write_string(&mut buf, Some("hello")).unwrap();
        let mut cursor = Cursor::new(&buf);
        let decoded = read_string(&mut cursor).unwrap();
        assert_eq!(decoded, Some("hello".to_string()));

        // Null string
        buf.clear();
        write_string(&mut buf, None).unwrap();
        let mut cursor = Cursor::new(&buf);
        let decoded = read_string(&mut cursor).unwrap();
        assert_eq!(decoded, None);
    }

    #[test]
    fn test_compact_string_encoding() {
        let mut buf = Vec::new();

        // Non-null string
        write_compact_string(&mut buf, Some("hello")).unwrap();
        let mut cursor = Cursor::new(&buf);
        let decoded = read_compact_string(&mut cursor).unwrap();
        assert_eq!(decoded, Some("hello".to_string()));

        // Null string
        buf.clear();
        write_compact_string(&mut buf, None).unwrap();
        let mut cursor = Cursor::new(&buf);
        let decoded = read_compact_string(&mut cursor).unwrap();
        assert_eq!(decoded, None);
    }

    #[test]
    fn test_bytes_encoding() {
        let mut buf = Vec::new();

        // Non-null bytes
        write_bytes(&mut buf, Some(&[1, 2, 3, 4, 5])).unwrap();
        let mut cursor = Cursor::new(&buf);
        let decoded = read_bytes(&mut cursor).unwrap();
        assert_eq!(decoded, Some(vec![1, 2, 3, 4, 5]));

        // Null bytes
        buf.clear();
        write_bytes(&mut buf, None).unwrap();
        let mut cursor = Cursor::new(&buf);
        let decoded = read_bytes(&mut cursor).unwrap();
        assert_eq!(decoded, None);
    }

    #[test]
    fn test_array_encoding() {
        let mut buf = Vec::new();

        // Non-null array
        let elements = vec![1i32, 2, 3, 4, 5];
        write_array(&mut buf, Some(&elements), |w, e| write_int32(w, *e)).unwrap();
        let mut cursor = Cursor::new(&buf);
        let decoded = read_array(&mut cursor, read_int32).unwrap();
        assert_eq!(decoded, Some(elements));

        // Null array
        buf.clear();
        write_array::<_, i32, _>(&mut buf, None, |w, e| write_int32(w, *e)).unwrap();
        let mut cursor = Cursor::new(&buf);
        let decoded = read_array(&mut cursor, read_int32).unwrap();
        assert_eq!(decoded, None);
    }

    #[test]
    fn test_buf_ext_varint() {
        let mut buf = bytes::BytesMut::new();
        buf.put_kafka_varint(12345);
        buf.put_kafka_varint(-12345);

        let mut reader = buf.freeze();
        assert_eq!(reader.get_kafka_varint(), 12345);
        assert_eq!(reader.get_kafka_varint(), -12345);
    }

    #[test]
    fn test_buf_ext_strings() {
        let mut buf = bytes::BytesMut::new();
        buf.put_kafka_string(Some("hello"));
        buf.put_kafka_string(None);
        buf.put_kafka_compact_string(Some("world"));
        buf.put_kafka_compact_string(None);

        let mut reader = buf.freeze();
        assert_eq!(reader.get_kafka_string(), Some("hello".to_string()));
        assert_eq!(reader.get_kafka_string(), None);
        assert_eq!(reader.get_kafka_compact_string(), Some("world".to_string()));
        assert_eq!(reader.get_kafka_compact_string(), None);
    }
}
