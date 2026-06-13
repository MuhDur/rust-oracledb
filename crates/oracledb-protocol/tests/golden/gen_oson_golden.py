#!/usr/bin/env python3
# Regenerates crates/oracledb-protocol/tests/golden/oson_golden.json from a real
# python-oracledb against the live container DB. The hex images are the exact OSON
# binary produced by the compiled C codec (Connection.encode_oson); the Rust
# OsonEncoder must reproduce them byte-for-byte and the Rust OsonDecoder must
# round-trip them.
#
# Usage (from the worktree root, with the container env exported):
#   eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
#   .venv-py313/bin/python crates/oracledb-protocol/tests/golden/gen_oson_golden.py
import datetime
import decimal
import json
import os

import oracledb


def main() -> None:
    cs = os.environ["PYO_TEST_CONNECT_STRING"]
    user = os.environ["PYO_TEST_MAIN_USER"]
    password = os.environ["PYO_TEST_MAIN_PASSWORD"]
    conn = oracledb.connect(user=user, password=password, dsn=cs)
    cases = []

    def add(name, value):
        cases.append({"name": name, "hex": conn.encode_oson(value).hex()})

    add("scalar_int_42", 42)
    add("scalar_str_hello", "hello")
    add("scalar_true", True)
    add("scalar_false", False)
    add("scalar_null", None)
    add("scalar_empty_str", "")
    add("scalar_float_25_25", 25.25)
    add("scalar_decimal", decimal.Decimal("319438950232418390.273596"))
    add("scalar_neg_big", -9999999999999999999)
    add("scalar_bytes", b"Some Bytes")
    add("empty_obj", {})
    add("simple_obj", {"id": 6901, "value": "string 6901"})
    add("name_none", {"name": None})
    add(
        "nested",
        {
            "employee": {
                "name": "John",
                "age": 30,
                "city": "Delhi",
                "Parmanent": True,
            }
        },
    )
    add("list_in_obj", {"employees": ["John", "Matthew", "James"]})
    add(
        "list_of_obj",
        {"employees": [{"employee1": {"name": "John", "city": "Delhi"}}]},
    )
    add("obj_3516", dict(key_1="test_3516a", key_2="test_3516b"))
    add("timestamp7", datetime.datetime(2004, 2, 1, 3, 4, 5))
    add("timestamp_fs", datetime.datetime(2002, 12, 13, 9, 36, 0, 123000))
    add("date_only", datetime.datetime(2002, 12, 13))
    add("interval_ds", datetime.timedelta(8.5))
    add("long_fname_256", {"A" * 256: 6700})

    golden = {
        "_comment": (
            "Golden OSON binary images captured from python-oracledb 4.0.1 "
            "(compiled C codec) via Connection.encode_oson against Oracle 23. "
            "The Rust OsonEncoder must reproduce these byte-for-byte and the "
            "Rust OsonDecoder must round-trip them. Regenerate with "
            "gen_oson_golden.py."
        ),
        "_server_version": conn.version,
        "cases": cases,
    }
    out_path = os.path.join(os.path.dirname(__file__), "oson_golden.json")
    with open(out_path, "w") as handle:
        handle.write(json.dumps(golden, indent=2))
        handle.write("\n")
    print(f"wrote {len(cases)} cases to {out_path}")
    conn.close()


if __name__ == "__main__":
    main()
