//! Microbenchmark + allocation count for the single-packet response passthrough
//! (bead rust-oracledb-0n0).
//!
//! When a fetch/data response is ONE terminal DATA packet, the reassembly loop
//! can MOVE the packet's owned payload buffer into the response and strip the
//! 2 flag bytes in place, instead of allocating a fresh `Vec` and copying the
//! whole payload into it. This bench isolates that exact operation on a freshly
//! allocated packet buffer (as `read_packet` produces one), comparing:
//!
//!   * legacy:      `Vec::new()` + `extend_from_slice(&payload[2..])`
//!   * passthrough: move the buffer + `drain(..2)`
//!
//! across the single-packet payload sizes the optimization actually applies to
//! (a response that fits one DATA packet — small queries, single-row fetches,
//! login/handshake responses). It reports both wall time (criterion) and the
//! allocation delta (counting allocator), so the win is measured, not assumed.
//!
//! Run:
//!   cargo bench -p oracledb --bench single_packet_passthrough

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
#[cfg(feature = "cassette")]
use oracledb::protocol::net::cassette::{self, Direction};
use oracledb::protocol::thin::{
    bind_value_type_info, build_execute_payload_with_bind_rows_with_seq, decode_lob_text,
    decode_number_text_into, decode_number_value, encode_number_text, BindValue, QueryValue,
    CS_FORM_NCHAR,
};
use oracledb::protocol::vector::{decode_vector, encode_vector, Vector, VectorValues};
use oracledb::{ExecutemanyManager, FromSql, IntoBinds, ToSql};

/// A fresh owned packet buffer of `size` bytes (the first 2 are data flags), as
/// `read_packet` allocates one per packet. Filled so the copy is real work.
fn fresh_packet(size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    v
}

/// Legacy reassembly: allocate a fresh response Vec and copy the flag-stripped
/// payload into it.
#[inline]
fn legacy(packet_payload: Vec<u8>) -> Vec<u8> {
    let mut response = Vec::new();
    response.extend_from_slice(&packet_payload[2..]);
    response
}

/// Passthrough: move the owned buffer and strip the 2 leading flag bytes in
/// place — no second allocation, no copy into a separate buffer.
#[inline]
fn passthrough(packet_payload: Vec<u8>) -> Vec<u8> {
    let mut response = packet_payload;
    response.drain(..2);
    response
}

fn bench_size(c: &mut Criterion, size: usize) {
    let label = format!("single_packet_{size}b");
    let mut group = c.benchmark_group(&label);
    group.throughput(Throughput::Bytes((size - 2) as u64));

    group.bench_function("legacy_extend", |b| {
        b.iter_batched(
            || fresh_packet(size),
            |p| black_box(legacy(black_box(p))),
            BatchSize::SmallInput,
        )
    });
    group.bench_function("passthrough_drain", |b| {
        b.iter_batched(
            || fresh_packet(size),
            |p| black_box(passthrough(black_box(p))),
            BatchSize::SmallInput,
        )
    });
    group.finish();

    // Allocation count over many iterations (counting allocator). Each `legacy`
    // call allocates one response Vec; each `passthrough` call allocates none
    // (it reuses the input buffer, which the caller already paid for).
    const ITERS: usize = 10_000;
    let legacy_allocs = allocation_counter::measure(|| {
        let mut sink = 0usize;
        for _ in 0..ITERS {
            let r = legacy(fresh_packet(size));
            sink = sink.wrapping_add(r.len());
        }
        black_box(sink);
    });
    let passthrough_allocs = allocation_counter::measure(|| {
        let mut sink = 0usize;
        for _ in 0..ITERS {
            let r = passthrough(fresh_packet(size));
            sink = sink.wrapping_add(r.len());
        }
        black_box(sink);
    });
    // Subtract the shared `fresh_packet` allocation (1 per iter) to isolate the
    // reassembly-side allocations.
    let legacy_reassembly = legacy_allocs.count_total as i64 - ITERS as i64;
    let passthrough_reassembly = passthrough_allocs.count_total as i64 - ITERS as i64;
    println!(
        "[{label}] reassembly allocs over {ITERS} iters — legacy: {legacy_reassembly}, \
         passthrough: {passthrough_reassembly} (bytes: legacy {}, passthrough {})",
        legacy_allocs.bytes_total, passthrough_allocs.bytes_total
    );
}

fn benches(c: &mut Criterion) {
    // Sizes that fit a single DATA packet (default SDU 8KB):
    bench_size(c, 64); // tiny: login / status response
    bench_size(c, 512); // small query result
    bench_size(c, 2048); // a few rows
    bench_size(c, 8000); // near a full single packet
    bench_response_reassembly(c);
    bench_protocol_codecs(c);
    bench_conversion_and_binds(c);
    #[cfg(feature = "cassette")]
    bench_cassette_overhead(c);
}

criterion_group!(passthrough_bench, benches);
criterion_main!(passthrough_bench);

fn number_wire_values() -> Vec<Vec<u8>> {
    [
        "0",
        "1",
        "-1",
        "42",
        "1000003",
        "-9876543210",
        "12345678901234567890",
        "0.00000125",
        "-12345.6789",
        "1.2345678901234567890123456789012345678e+25",
    ]
    .iter()
    .map(|value| encode_number_text(value).expect("NUMBER fixture encodes"))
    .collect()
}

fn utf16be_lob_fixture(chars: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(chars * 2);
    for i in 0..chars {
        let ch = b'a' + (i % 26) as u8;
        out.push(0);
        out.push(ch);
    }
    out
}

fn multi_packet_fixture(packet_count: usize, packet_size: usize) -> Vec<Vec<u8>> {
    (0..packet_count)
        .map(|packet_idx| {
            let mut packet = Vec::with_capacity(packet_size);
            packet.extend_from_slice(&[0, 0]);
            packet.extend((2..packet_size).map(|i| ((i + packet_idx) % 251) as u8));
            packet
        })
        .collect()
}

fn reassemble_multi_packet_response(packets: &[Vec<u8>]) -> Vec<u8> {
    let payload_len = packets.iter().map(|packet| packet.len() - 2).sum();
    let mut response = Vec::with_capacity(payload_len);
    for packet in packets {
        response.extend_from_slice(&packet[2..]);
    }
    response
}

fn bench_response_reassembly(c: &mut Criterion) {
    let packets = multi_packet_fixture(4, 8192);
    let payload_len: usize = packets.iter().map(|packet| packet.len() - 2).sum();
    let mut group = c.benchmark_group("deterministic_reassembly");
    group.throughput(Throughput::Bytes(payload_len as u64));
    group.bench_function("multi_packet_4x8k_extend", |b| {
        b.iter(|| {
            let response = reassemble_multi_packet_response(black_box(&packets));
            black_box(response.len())
        });
    });
    group.finish();
}

fn dense_vector_fixture() -> Vec<u8> {
    let values: Vec<f32> = (0..1024).map(|i| i as f32 / 16.0).collect();
    encode_vector(&Vector::Dense(VectorValues::Float32(values)))
}

fn bench_protocol_codecs(c: &mut Criterion) {
    let numbers = number_wire_values();
    let utf16_lob = utf16be_lob_fixture(65_536);
    let vector = dense_vector_fixture();

    let mut group = c.benchmark_group("deterministic_codec");
    group.throughput(Throughput::Elements(numbers.len() as u64));
    group.bench_function("number_decode_owned", |b| {
        b.iter(|| {
            let mut total = 0usize;
            for wire in &numbers {
                let value = decode_number_value(black_box(wire)).expect("NUMBER decodes");
                if let QueryValue::Number(number) = value {
                    total = total.wrapping_add(number.to_canonical_string().len());
                }
            }
            black_box(total)
        });
    });

    group.bench_function("number_decode_reused_scratch", |b| {
        b.iter(|| {
            let mut digits = Vec::new();
            let mut text = String::new();
            let mut integral_count = 0usize;
            for wire in &numbers {
                text.clear();
                let is_integral = decode_number_text_into(black_box(wire), &mut digits, &mut text)
                    .expect("NUMBER text decodes");
                integral_count += usize::from(is_integral);
                black_box(&text);
            }
            black_box(integral_count)
        });
    });

    group.throughput(Throughput::Bytes(utf16_lob.len() as u64));
    group.bench_function("lob_utf16_64k_decode", |b| {
        b.iter(|| {
            let text = decode_lob_text(black_box(&utf16_lob), CS_FORM_NCHAR, None)
                .expect("UTF-16 LOB decodes");
            black_box(text.len())
        });
    });

    group.throughput(Throughput::Bytes(vector.len() as u64));
    group.bench_function("vector_image_decode_1024_f32", |b| {
        b.iter(|| {
            let decoded = decode_vector(black_box(&vector)).expect("VECTOR image decodes");
            match decoded {
                Vector::Dense(VectorValues::Float32(values)) => black_box(values.len()),
                other => panic!("expected dense f32 VECTOR, got {other:?}"),
            }
        });
    });
    group.finish();
}

fn bind_rows_fixture(rows: usize) -> Vec<Vec<BindValue>> {
    (0..rows)
        .map(|i| {
            vec![
                BindValue::Number(i.to_string()),
                BindValue::Text(format!("row-{i:05}")),
                BindValue::Raw(vec![(i % 251) as u8; 32]),
            ]
        })
        .collect()
}

fn drive_executemany_manager(total_rows: usize, batch_size: u32) -> usize {
    let mut manager = ExecutemanyManager::with_chunks(total_rows, batch_size, vec![512, 768, 1024])
        .expect("batch manager accepts fixture shape");
    let mut batches = 0usize;
    while manager.num_rows() > 0 {
        batches =
            batches.wrapping_add(manager.num_rows() as usize + manager.message_offset() as usize);
        manager.next_batch();
    }
    batches
}

fn bench_conversion_and_binds(c: &mut Criterion) {
    let rows = bind_rows_fixture(256);
    let values = [
        QueryValue::number_from_text("123456789", true),
        QueryValue::Text("rust-oracledb".to_string()),
        QueryValue::Raw(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        QueryValue::Boolean(true),
    ];

    let mut group = c.benchmark_group("deterministic_binds");
    group.throughput(Throughput::Elements(values.len() as u64));
    group.bench_function("typed_conversion_core", |b| {
        b.iter(|| {
            let n = i64::from_sql(black_box(&values[0])).expect("NUMBER converts to i64");
            let s = String::from_sql(black_box(&values[1])).expect("text converts to String");
            let bytes = Vec::<u8>::from_sql(black_box(&values[2])).expect("RAW converts to bytes");
            let flag = bool::from_sql(black_box(&values[3])).expect("BOOLEAN converts to bool");
            black_box((n, s.len(), bytes.len(), flag))
        });
    });

    group.bench_function("tosql_and_bind_metadata", |b| {
        b.iter(|| {
            let mut binds = (42_i64, "alpha", true, vec![1_u8, 2, 3, 4]).into_binds();
            binds.push(Option::<i64>::None.to_sql());
            let mut total = 0u32;
            for bind in &binds {
                if let Some(info) = bind_value_type_info(black_box(bind)) {
                    total = total.wrapping_add(info.buffer_size);
                }
            }
            black_box(total)
        });
    });

    group.throughput(Throughput::Elements(rows.len() as u64));
    group.bench_function("executemany_batch_windows", |b| {
        b.iter(|| black_box(drive_executemany_manager(2304, 256)));
    });

    group.bench_function("execute_payload_256x3_binds", |b| {
        b.iter(|| {
            let payload = build_execute_payload_with_bind_rows_with_seq(
                "insert into bench_t (id, label, payload) values (:1, :2, :3)",
                1,
                1,
                false,
                black_box(&rows),
            )
            .expect("execute payload builds");
            black_box(payload.len())
        });
    });
    group.finish();
}

#[cfg(feature = "cassette")]
fn cassette_fixture(frame_count: usize, payload_len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    cassette::write_header(&mut out);
    let payload: Vec<u8> = (0..payload_len).map(|i| (i % 251) as u8).collect();
    for i in 0..frame_count {
        let direction = if i % 2 == 0 {
            Direction::ClientToServer
        } else {
            Direction::ServerToClient
        };
        cassette::write_frame(&mut out, direction, (i as u64) * 125, &payload);
    }
    out
}

#[cfg(feature = "cassette")]
fn bench_cassette_overhead(c: &mut Criterion) {
    let cassette = cassette_fixture(256, 512);
    let mut group = c.benchmark_group("deterministic_cassette");
    group.throughput(Throughput::Bytes(cassette.len() as u64));
    group.bench_function("decode_256x512b_frames", |b| {
        b.iter(|| {
            let frames = cassette::decode_all(black_box(&cassette)).expect("cassette decodes");
            black_box(frames.len())
        });
    });
    group.finish();
}
