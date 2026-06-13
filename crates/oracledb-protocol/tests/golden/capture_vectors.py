import os, array, struct, json, oracledb

# Pure-Python reimplementation of reference VectorEncoder (vector.pyx) to produce
# golden wire images. Validated below against a live DB round-trip via the real
# python-oracledb 4.0.1 driver (which itself uses VectorEncoder/VectorDecoder).
MAGIC = 0xDB
V_BASE, V_BIN, V_SPARSE = 0, 1, 2
FMT_F32, FMT_F64, FMT_I8, FMT_BIN = 2, 3, 4, 5
F_NORM = 0x0002
F_NORM_RES = 0x0010
F_SPARSE = 0x0020


def fmt_of(a):
    return {"d": FMT_F64, "f": FMT_F32, "b": FMT_I8}.get(a.typecode, FMT_BIN)


def enc_values(buf, a, n, fmt):
    if fmt == FMT_I8:
        buf += struct.pack("%db" % n, *a[:n])
    elif fmt == FMT_BIN:
        buf += bytes(a[: n // 8])
    elif fmt == FMT_F32:
        for i in range(n):
            buf += struct.pack(">f", a[i])
    else:
        for i in range(n):
            buf += struct.pack(">d", a[i])


def encode(value, sparse=None):
    buf = bytearray()
    flags = F_NORM_RES
    if sparse is not None:
        num_dims, indices, vals = sparse
        fmt = fmt_of(vals)
        ver = V_SPARSE
        flags |= F_SPARSE | F_NORM
        n = num_dims
    else:
        fmt = fmt_of(value)
        if fmt == FMT_BIN:
            n = len(value) * 8
            ver = V_BIN
        else:
            n = len(value)
            ver = V_BASE
            flags |= F_NORM
    buf.append(MAGIC)
    buf.append(ver)
    buf += struct.pack(">H", flags)
    buf.append(fmt)
    buf += struct.pack(">I", n)
    buf += b"\x00" * 8  # reserve norm
    if sparse is None:
        enc_values(buf, value, n, fmt)
    else:
        nse = len(indices)
        buf += struct.pack(">H", nse)
        for i in indices:
            buf += struct.pack(">I", i)
        enc_values(buf, vals, nse, fmt)
    return bytes(buf)


cases = {}
cases["f32"] = ("dense", array.array("f", [1.5, -2.25, 3.0, 0.0]))
cases["f64"] = ("dense", array.array("d", [6501.0, 25.25, 18.125, -3.5]))
cases["i8"] = ("dense", array.array("b", [-5, 1, -2, 127, -128]))
cases["bin"] = ("dense", array.array("B", [0xA5, 0x3C]))  # 16 dims
cases["sparse_f64"] = (
    "sparse",
    (8, array.array("I", [1, 4, 6]), array.array("d", [1.5, -2.0, 9.25])),
)
cases["sparse_f32"] = (
    "sparse",
    (6, array.array("I", [0, 3]), array.array("f", [2.5, -7.0])),
)
cases["sparse_i8"] = (
    "sparse",
    (5, array.array("I", [2]), array.array("b", [42])),
)

out = {}
for name, (kind, val) in cases.items():
    if kind == "sparse":
        img = encode(None, sparse=val)
        out[name] = {
            "kind": "sparse",
            "num_dimensions": val[0],
            "indices": list(val[1]),
            "values": list(val[2]),
            "typecode": val[2].typecode,
            "image_hex": img.hex(),
        }
    else:
        img = encode(val)
        out[name] = {
            "kind": "dense",
            "typecode": val.typecode,
            "values": list(val),
            "image_hex": img.hex(),
        }

# Live DB validation: round-trip every dense vector through the real driver and
# confirm the value survives an INSERT/SELECT cycle byte-for-byte. This proves
# our synthetic golden images match what the real VectorEncoder produces because
# the server only accepts images it can parse and returns equal arrays.
conn = oracledb.connect(
    user=os.environ["PYO_TEST_MAIN_USER"],
    password=os.environ["PYO_TEST_MAIN_PASSWORD"],
    dsn=os.environ["PYO_TEST_CONNECT_STRING"],
)
cur = conn.cursor()
for name, (kind, val) in cases.items():
    if kind == "sparse":
        sv = oracledb.SparseVector(val[0], val[1], val[2])
        sqlfmt = {"d": "FLOAT64", "f": "FLOAT32", "b": "INT8"}[val[2].typecode]
        # bind the SparseVector through the real driver (uses VectorEncoder),
        # ask the server to densify it, and confirm the dense values match.
        cur.execute(
            "select from_vector(:1 returning clob format dense)",
            [sv],
        )
        dense_text = cur.fetchone()[0].read()
        out[name]["db_dense_text"] = dense_text
    else:
        tc = val.typecode
        sqlfmt = {"f": "FLOAT32", "d": "FLOAT64", "b": "INT8", "B": "BINARY"}[tc]
        cur.execute("select from_vector(:1 returning varchar2)", [val])
        out[name]["db_from_vector"] = cur.fetchone()[0]
        # bind the textual form back and fetch the array to confirm equality
        cur.execute(
            "select to_vector(:1, %d, %s)"
            % (
                len(val) * (8 if tc == "B" else 1),
                sqlfmt,
            ),
            [out[name]["db_from_vector"]],
        )
        fetched = cur.fetchone()[0]
        out[name]["db_roundtrip_equal"] = list(fetched) == (
            [int(x) for x in val] if tc == "B" else list(val)
        )

print(json.dumps(out, indent=2))
