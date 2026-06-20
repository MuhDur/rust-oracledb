#![forbid(unsafe_code)]

use crate::wire::ProtocolLimits;
use crate::{ProtocolError, Result};

pub const TNS_HEADER_LEN: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TnsPacket {
    pub packet_type: u8,
    pub flags: u8,
    pub payload: Vec<u8>,
}

impl TnsPacket {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let length = TNS_HEADER_LEN + self.payload.len();
        let wire_length =
            u16::try_from(length).map_err(|_| ProtocolError::PacketTooLarge { length })?;
        let mut out = Vec::with_capacity(length);
        out.extend_from_slice(&wire_length.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.push(self.packet_type);
        out.push(self.flags);
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    pub fn parse(input: &[u8]) -> Result<Self> {
        Self::parse_with_limits(input, ProtocolLimits::DEFAULT)
    }

    pub fn parse_with_limits(input: &[u8], limits: ProtocolLimits) -> Result<Self> {
        let limits = limits.validate()?;
        let header = input
            .get(..TNS_HEADER_LEN)
            .ok_or(ProtocolError::TruncatedHeader { got: input.len() })?;
        let length_bytes = input
            .get(..2)
            .ok_or(ProtocolError::TruncatedHeader { got: input.len() })?;
        let declared = usize::from(u16::from_be_bytes(
            length_bytes
                .try_into()
                .map_err(|_| ProtocolError::TruncatedHeader { got: input.len() })?,
        ));
        if declared < TNS_HEADER_LEN {
            return Err(ProtocolError::InvalidPacketLength {
                length: declared,
                minimum: TNS_HEADER_LEN,
            });
        }
        limits.check_packet_bytes(declared)?;
        if declared > input.len() {
            return Err(ProtocolError::IncompletePacket {
                declared,
                available: input.len(),
            });
        }

        Ok(Self {
            packet_type: *header
                .get(4)
                .ok_or(ProtocolError::TruncatedHeader { got: input.len() })?,
            flags: *header
                .get(5)
                .ok_or(ProtocolError::TruncatedHeader { got: input.len() })?,
            payload: input
                .get(TNS_HEADER_LEN..declared)
                .ok_or(ProtocolError::IncompletePacket {
                    declared,
                    available: input.len(),
                })?
                .to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_round_trips() {
        let packet = TnsPacket {
            packet_type: 1,
            flags: 0,
            payload: b"hello".to_vec(),
        };

        let encoded = packet.encode().expect("small packet should encode");
        assert_eq!(
            TnsPacket::parse(&encoded).expect("encoded packet should parse"),
            packet
        );
    }

    #[test]
    fn packet_decoder_fails_closed_on_short_header() {
        assert!(matches!(
            TnsPacket::parse(&[0, 1, 2]),
            Err(ProtocolError::TruncatedHeader { got: 3 })
        ));
    }

    #[test]
    fn packet_decoder_fails_closed_on_incomplete_body() {
        let mut bytes = TnsPacket {
            packet_type: 1,
            flags: 0,
            payload: b"hello".to_vec(),
        }
        .encode()
        .expect("small packet should encode");
        *bytes
            .get_mut(1)
            .expect("encoded packet header should contain length byte") = 128;

        assert!(matches!(
            TnsPacket::parse(&bytes),
            Err(ProtocolError::IncompletePacket { .. })
        ));
    }

    #[test]
    fn packet_decoder_uses_protocol_limits_before_copying_payload() {
        let bytes = TnsPacket {
            packet_type: 1,
            flags: 0,
            payload: b"hello".to_vec(),
        }
        .encode()
        .expect("small packet should encode");
        let limits = ProtocolLimits {
            max_packet_bytes: bytes.len() - 1,
            max_frame_bytes: bytes.len() - 1,
            max_response_bytes: bytes.len() - 1,
            ..ProtocolLimits::DEFAULT
        };

        assert!(matches!(
            TnsPacket::parse_with_limits(&bytes, limits),
            Err(ProtocolError::ResourceLimit {
                limit: "packet_bytes",
                observed,
                maximum,
            }) if observed == bytes.len() && maximum == bytes.len() - 1
        ));
    }

    #[test]
    fn packet_encoder_fails_closed_on_oversize_payload() {
        let packet = TnsPacket {
            packet_type: 1,
            flags: 0,
            payload: vec![0; usize::from(u16::MAX) + 1],
        };

        assert!(matches!(
            packet.encode(),
            Err(ProtocolError::PacketTooLarge { .. })
        ));
    }
}
