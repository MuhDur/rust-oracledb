#![forbid(unsafe_code)]

use crate::{ProtocolError, Result};

pub const TNS_MAX_SHORT_LENGTH: usize = 252;
pub const TNS_LONG_LENGTH_INDICATOR: u8 = 0xfe;
pub const TNS_NULL_LENGTH_INDICATOR: u8 = 0xff;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketLengthWidth {
    Legacy16,
    Large32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TtcWriter {
    bytes: Vec<u8>,
    seq_num: u8,
}

impl TtcWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn write_u16be(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u16le(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn write_u32be(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u64be(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_ub2(&mut self, value: u16) {
        if value == 0 {
            self.write_u8(0);
        } else if value <= u16::from(u8::MAX) {
            self.write_u8(1);
            self.write_u8(value as u8);
        } else {
            self.write_u8(2);
            self.write_u16be(value);
        }
    }

    pub fn write_ub4(&mut self, value: u32) {
        if value == 0 {
            self.write_u8(0);
        } else if value <= u32::from(u8::MAX) {
            self.write_u8(1);
            self.write_u8(value as u8);
        } else if value <= u32::from(u16::MAX) {
            self.write_u8(2);
            self.write_u16be(value as u16);
        } else {
            self.write_u8(4);
            self.write_u32be(value);
        }
    }

    pub fn write_ub8(&mut self, value: u64) {
        if value == 0 {
            self.write_u8(0);
        } else if value <= u64::from(u8::MAX) {
            self.write_u8(1);
            self.write_u8(value as u8);
        } else if value <= u64::from(u16::MAX) {
            self.write_u8(2);
            self.write_u16be(value as u16);
        } else if value <= u64::from(u32::MAX) {
            self.write_u8(4);
            self.write_u32be(value as u32);
        } else {
            self.write_u8(8);
            self.write_u64be(value);
        }
    }

    pub fn write_seq_num(&mut self) {
        self.seq_num = self.seq_num.wrapping_add(1);
        if self.seq_num == 0 {
            self.seq_num = 1;
        }
        self.write_u8(self.seq_num);
    }

    pub fn write_raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub fn write_bytes_with_length(&mut self, value: &[u8]) -> Result<()> {
        if value.len() <= TNS_MAX_SHORT_LENGTH {
            self.write_u8(value.len() as u8);
            self.write_raw(value);
            return Ok(());
        }
        self.write_u8(TNS_LONG_LENGTH_INDICATOR);
        for chunk in value.chunks(32_767) {
            self.write_ub4(u32::try_from(chunk.len()).map_err(|_| {
                ProtocolError::InvalidPacketLength {
                    length: chunk.len(),
                    minimum: 0,
                }
            })?);
            self.write_raw(chunk);
        }
        self.write_ub4(0);
        Ok(())
    }

    pub fn write_bytes_with_two_lengths(&mut self, value: Option<&[u8]>) -> Result<()> {
        match value {
            Some(bytes) => {
                self.write_ub4(u32::try_from(bytes.len()).map_err(|_| {
                    ProtocolError::InvalidPacketLength {
                        length: bytes.len(),
                        minimum: 0,
                    }
                })?);
                if !bytes.is_empty() {
                    self.write_bytes_with_length(bytes)?;
                }
            }
            None => self.write_ub4(0),
        }
        Ok(())
    }

    pub fn write_str_two_lengths(&mut self, value: &str) -> Result<()> {
        self.write_bytes_with_two_lengths(Some(value.as_bytes()))
    }

    pub fn write_function_code(&mut self, function_code: u8) {
        self.write_u8(crate::thin::TNS_MSG_TYPE_FUNCTION);
        self.write_u8(function_code);
        self.write_seq_num();
    }

    pub fn write_function_code_with_seq(&mut self, function_code: u8, seq_num: u8) {
        self.write_u8(crate::thin::TNS_MSG_TYPE_FUNCTION);
        self.write_u8(function_code);
        self.write_u8(seq_num);
    }
}

#[derive(Clone, Debug)]
pub struct TtcReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> TtcReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn peek_u8(&self) -> Result<u8> {
        self.bytes
            .get(self.pos)
            .copied()
            .ok_or(ProtocolError::TtcDecode("missing u8"))
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.pos)
            .ok_or(ProtocolError::TtcDecode("missing u8"))?;
        self.pos += 1;
        Ok(value)
    }

    pub fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    pub fn read_u16be(&mut self) -> Result<u16> {
        let bytes = self.read_raw(2)?;
        Ok(u16::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| ProtocolError::TtcDecode("invalid u16"))?,
        ))
    }

    pub fn read_u16le(&mut self) -> Result<u16> {
        let bytes = self.read_raw(2)?;
        Ok(u16::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| ProtocolError::TtcDecode("invalid u16"))?,
        ))
    }

    pub fn read_u32be(&mut self) -> Result<u32> {
        let bytes = self.read_raw(4)?;
        Ok(u32::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| ProtocolError::TtcDecode("invalid u32"))?,
        ))
    }

    pub fn read_raw(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ProtocolError::TtcDecode("read offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.pos..end)
            .ok_or(ProtocolError::TtcDecode("truncated TTC payload"))?;
        self.pos = end;
        Ok(bytes)
    }

    pub fn skip(&mut self, len: usize) -> Result<()> {
        self.read_raw(len).map(|_| ())
    }

    pub fn read_ub2(&mut self) -> Result<u16> {
        let len = self.read_u8()?;
        match len {
            0 => Ok(0),
            1 => Ok(u16::from(self.read_u8()?)),
            2 => self.read_u16be(),
            _ => Err(ProtocolError::TtcDecode("invalid ub2 length")),
        }
    }

    pub fn read_ub4(&mut self) -> Result<u32> {
        let len = self.read_u8()?;
        if len == 0 {
            return Ok(0);
        }
        if len > 4 {
            return Err(ProtocolError::TtcDecode("invalid ub4 length"));
        }
        let mut value = 0u32;
        for byte in self.read_raw(usize::from(len))? {
            value = (value << 8) | u32::from(*byte);
        }
        Ok(value)
    }

    pub fn read_sb4(&mut self) -> Result<i32> {
        let len = self.read_u8()?;
        let is_negative = len & 0x80 != 0;
        let len = len & 0x7f;
        if len == 0 {
            return Ok(0);
        }
        if len > 4 {
            return Err(ProtocolError::TtcDecode("invalid sb4 length"));
        }
        let mut value = 0i32;
        for byte in self.read_raw(usize::from(len))? {
            value = (value << 8) | i32::from(*byte);
        }
        if is_negative {
            value = -value;
        }
        Ok(value)
    }

    pub fn read_sb8(&mut self) -> Result<i64> {
        let len = self.read_u8()?;
        let is_negative = len & 0x80 != 0;
        let len = len & 0x7f;
        if len == 0 {
            return Ok(0);
        }
        if len > 8 {
            return Err(ProtocolError::TtcDecode("invalid sb8 length"));
        }
        let mut value = 0i64;
        for byte in self.read_raw(usize::from(len))? {
            value = (value << 8) | i64::from(*byte);
        }
        if is_negative {
            value = -value;
        }
        Ok(value)
    }

    pub fn read_ub8(&mut self) -> Result<u64> {
        let len = self.read_u8()?;
        if len == 0 {
            return Ok(0);
        }
        if len > 8 {
            return Err(ProtocolError::TtcDecode("invalid ub8 length"));
        }
        let mut value = 0u64;
        for byte in self.read_raw(usize::from(len))? {
            value = (value << 8) | u64::from(*byte);
        }
        Ok(value)
    }

    pub fn read_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let len = self.read_u8()?;
        if len == TNS_LONG_LENGTH_INDICATOR {
            let mut out = Vec::new();
            loop {
                let chunk_len = self.read_ub4()?;
                if chunk_len == 0 {
                    break;
                }
                let chunk = self.read_raw(chunk_len as usize)?;
                out.extend_from_slice(chunk);
            }
            Ok(Some(out))
        } else if len == 0 || len == TNS_NULL_LENGTH_INDICATOR {
            Ok(None)
        } else {
            Ok(Some(self.read_raw(usize::from(len))?.to_vec()))
        }
    }

    pub fn read_bytes_with_length(&mut self) -> Result<Option<Vec<u8>>> {
        let len =
            usize::try_from(self.read_ub4()?).map_err(|_| ProtocolError::InvalidPacketLength {
                length: usize::MAX,
                minimum: 0,
            })?;
        if len == 0 {
            return Ok(None);
        }
        let value_start = self.pos;
        match self.read_bytes() {
            Ok(Some(bytes)) if bytes.len() == len => Ok(Some(bytes)),
            Ok(_) | Err(_) => {
                self.pos = value_start;
                Ok(Some(self.read_raw(len)?.to_vec()))
            }
        }
    }

    pub fn read_string_with_length(&mut self) -> Result<Option<String>> {
        let Some(bytes) = self.read_bytes_with_length()? else {
            return Ok(None);
        };
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| ProtocolError::TtcDecode("server sent non-UTF8 string"))
    }

    pub fn read_string(&mut self) -> Result<Option<String>> {
        let Some(bytes) = self.read_bytes()? else {
            return Ok(None);
        };
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| ProtocolError::TtcDecode("server sent non-UTF8 string"))
    }
}

pub fn encode_packet(
    packet_type: u8,
    packet_flags: u8,
    data_flags: Option<u16>,
    payload: &[u8],
    width: PacketLengthWidth,
) -> Result<Vec<u8>> {
    let data_flags_len = usize::from(data_flags.is_some()) * 2;
    let length = crate::packet::TNS_HEADER_LEN + data_flags_len + payload.len();
    let mut out = Vec::with_capacity(length);
    match width {
        PacketLengthWidth::Legacy16 => {
            let wire_length =
                u16::try_from(length).map_err(|_| ProtocolError::PacketTooLarge { length })?;
            out.extend_from_slice(&wire_length.to_be_bytes());
            out.extend_from_slice(&0u16.to_be_bytes());
        }
        PacketLengthWidth::Large32 => {
            let wire_length =
                u32::try_from(length).map_err(|_| ProtocolError::PacketTooLarge { length })?;
            out.extend_from_slice(&wire_length.to_be_bytes());
        }
    }
    out.push(packet_type);
    out.push(packet_flags);
    out.extend_from_slice(&0u16.to_be_bytes());
    if let Some(flags) = data_flags {
        out.extend_from_slice(&flags.to_be_bytes());
    }
    out.extend_from_slice(payload);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ub4_round_trips_representative_values() {
        for value in [0, 1, 255, 256, 65_535, 65_536, u32::MAX] {
            let mut writer = TtcWriter::new();
            writer.write_ub4(value);
            let bytes = writer.into_bytes();
            let mut reader = TtcReader::new(&bytes);
            assert_eq!(reader.read_ub4().expect("ub4 should decode"), value);
            assert_eq!(reader.remaining(), 0);
        }
    }

    #[test]
    fn bytes_with_length_accepts_nested_ttc_bytes() {
        let mut writer = TtcWriter::new();
        writer
            .write_bytes_with_two_lengths(Some(b"abc"))
            .expect("bytes should encode");
        let bytes = writer.into_bytes();
        let mut reader = TtcReader::new(&bytes);
        assert_eq!(
            reader
                .read_bytes_with_length()
                .expect("bytes should decode"),
            Some(b"abc".to_vec())
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn bytes_with_length_accepts_direct_payload_bytes() {
        let bytes = [1, 3, b'a', b'b', b'c'];
        let mut reader = TtcReader::new(&bytes);
        assert_eq!(
            reader
                .read_bytes_with_length()
                .expect("bytes should decode"),
            Some(b"abc".to_vec())
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn data_packet_uses_four_byte_length_when_negotiated() {
        let packet = encode_packet(
            6,
            0,
            Some(0),
            &[0x03, 0x93, 0x01],
            PacketLengthWidth::Large32,
        )
        .expect("packet should encode");
        assert_eq!(&packet[..10], &[0, 0, 0, 13, 6, 0, 0, 0, 0, 0]);
    }
}
