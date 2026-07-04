#!/usr/bin/env python3
"""python-oracledb twin of crates/oracledb/examples/statement_ground_truth.rs
(bead rust-oracledb-rwoh).

Two modes:

  emit (default)   run the FIXED statement corpus against a live lane with
                   python-oracledb (thin mode) and print the canonical JSON
                   document to stdout. The corpus (case ids, SQL text, binds)
                   MUST stay in lock-step with the Rust emitter.

  --diff A.json B.json
                   compare two emitted documents field-by-field. Numbers
                   ("n:" cells) compare as exact decimals, floats ("d:") as
                   IEEE-754 bit patterns, everything else byte-for-byte.
                   Exit 0 = identical ground truth, 1 = mismatches (each
                   printed), 2 = usage/corpus-version error.

Usage:
  statement_ground_truth.py [CONNECT_STRING] [USER] [PASSWORD]
  statement_ground_truth.py --diff rust.json python.json

Env fallbacks: PYO_TEST_CONNECT_STRING, PYO_TEST_MAIN_USER,
PYO_TEST_MAIN_PASSWORD (same convention as the Rust examples).
"""

import decimal
import json
import os
import struct
import sys

CORPUS_VERSION = 1
DML_TABLE = "gt_truth_dml"
LOB_TABLE = "gt_truth_lob"


# ---------------------------------------------------------------------------
# canonical cell encoding (mirror of the Rust encoder)
# ---------------------------------------------------------------------------

def encode_cell(value):
    import datetime

    if value is None:
        return "null"
    if isinstance(value, str):
        return f"s:{value}"
    if isinstance(value, bytes):
        return f"r:{value.hex()}"
    if isinstance(value, bool):
        return f"b:{'true' if value else 'false'}"
    if isinstance(value, decimal.Decimal):
        return f"n:{value}"
    if isinstance(value, int):
        return f"n:{value}"
    if isinstance(value, float):
        return f"d:{struct.pack('>d', value).hex()}"
    if isinstance(value, datetime.datetime):
        # Manual formatting: strftime("%Y") does not zero-pad years < 1000.
        base = (
            f"{value.year:04}-{value.month:02}-{value.day:02}"
            f"T{value.hour:02}:{value.minute:02}:{value.second:02}"
            f".{value.microsecond:06}"
        )
        if value.tzinfo is not None:
            offset = value.utcoffset()
            total = int(offset.total_seconds())
            sign = "-" if total < 0 else "+"
            total = abs(total)
            return f"tz:{base}{sign}{total // 3600:02}:{(total % 3600) // 60:02}"
        return f"dt:{base}"
    raise TypeError(f"unencodable python value: {type(value)!r} = {value!r}")


def ora_code(exc):
    text = str(exc)
    pos = text.find("ORA-")
    if pos >= 0:
        code = text[pos : pos + 9]
        if len(code) == 9 and code[4:].isdigit():
            return code
    return "noora:" + text[:60]


# ---------------------------------------------------------------------------
# corpus — MUST stay in lock-step with the Rust emitter
# ---------------------------------------------------------------------------

SIMPLE_QUERIES = [
    (
        "num_int_edges",
        "select 0, 1, -1, 42, -42, 2147483647, -2147483648, 9223372036854775807, "
        "to_number('99999999999999999999999999999999999999'), "
        "to_number('-99999999999999999999999999999999999999') from dual",
    ),
    (
        "num_frac_edges",
        "select 0.5, -0.5, 0.1, -0.1, 123.456, -123.456, "
        "0.000000000000000000000000000001, 1.5E125, -1.5E125, 1E-130, -1E-130 from dual",
    ),
    (
        "num_negative_scale",
        "select 12345678901234567890, 1E10, 123450000, "
        "99999999999999999999999999999999999000 from dual",
    ),
    (
        "str_basic",
        "select 'plain', 'trailing ', ' leading', 'MiXeD-42_!@#' from dual",
    ),
    (
        "str_unicode",
        "select 'üñíçødé', 'żółć', '日本語テキスト', '💾🚀', unistr('\\20AC') from dual",
    ),
    (
        "str_nvarchar",
        "select cast('nvalue' as nvarchar2(30)), n'unicode-ñ', cast('ab' as nchar(4)) from dual",
    ),
    ("str_char_pad", "select cast('ab' as char(5)) from dual"),
    (
        "empty_null",
        "select '', null, cast(null as number), cast(null as date), "
        "cast(null as raw(10)) from dual",
    ),
    (
        "long_4000",
        "select rpad('x', 4000, 'x'), rpad('ab', 2000, 'ab') from dual",
    ),
    (
        "date_vals",
        "select date '2026-02-28', date '2000-02-29', date '1970-01-01', "
        "to_date('1900-01-01 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), "
        "to_date('0001-01-01', 'YYYY-MM-DD'), date '9999-12-31' from dual",
    ),
    (
        "ts_vals",
        "select timestamp '2026-07-04 12:34:56.123456', "
        "timestamp '2026-07-04 00:00:00', "
        "to_timestamp('2026-12-31 23:59:59.999999', 'YYYY-MM-DD HH24:MI:SS.FF6') from dual",
    ),
    (
        "tstz_vals",
        "select timestamp '2026-07-04 12:34:56.123456 +05:30', "
        "timestamp '2026-07-04 12:34:56.123456 -08:00', "
        "timestamp '2026-07-04 12:34:56.123456 +00:00' from dual",
    ),
    (
        "raw_vals",
        "select hextoraw('DEADBEEF00FF'), hextoraw(''), utl_raw.cast_to_raw('abc') from dual",
    ),
    (
        "float_native",
        "select cast(1.5 as binary_double), cast(-2.25 as binary_double), "
        "cast(0.1 as binary_double), cast(1.5 as binary_float), "
        "cast(0.1 as binary_float), binary_double_infinity, "
        "-binary_double_infinity, binary_double_nan from dual",
    ),
    (
        "float_oracle",
        "select cast(2.5 as float(126)), cast(123.25 as number(10,2)), "
        "cast(7 as integer) from dual",
    ),
    (
        "fetch_pages_99",
        "select level, 'r' || level from dual connect by level <= 99 order by level",
    ),
    (
        "fetch_pages_100",
        "select level, 'r' || level from dual connect by level <= 100 order by level",
    ),
    (
        "fetch_pages_101",
        "select level, 'r' || level from dual connect by level <= 101 order by level",
    ),
    (
        "fetch_pages_250",
        "select level, mod(level * 7, 97), rpad('p', 100, 'p') "
        "from dual connect by level <= 250 order by level",
    ),
    ("err_no_table", "select * from gt_truth_missing_tbl"),
    ("err_bad_col", "select bogus_col from dual"),
    ("err_div_zero", "select 1/0 from dual"),
    ("err_syntax", "select from dual"),
]


def run_query(conn, sql, binds=()):
    import oracledb

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, binds)
            columns = [d[0] for d in cursor.description]
            rows = [[encode_cell(v) for v in row] for row in cursor.fetchall()]
            return {"ok": True, "columns": columns, "rows": rows}
    except oracledb.Error as exc:
        return {"ok": False, "error": ora_code(exc)}


def run_count(conn, sql):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return [f"n:{cursor.rowcount}"]


def drop_if_exists(conn, table):
    import oracledb

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"drop table {table} purge")
    except oracledb.Error:
        pass


def run_corpus(conn):
    import oracledb

    cases = {}

    with conn.cursor() as cursor:
        cursor.execute("alter session set time_zone = '+00:00'")

    for case_id, sql in SIMPLE_QUERIES:
        cases[case_id] = run_query(conn, sql)

    # Bind round-trips (positional binds; ints/str/None mirror the Rust
    # Number/Text/Null BindValues).
    cases["bind_roundtrip"] = run_query(
        conn, "select :1, :2, :3 from dual", (42, "text-bind", None)
    )
    cases["bind_exprs"] = run_query(
        conn, "select :1 * 2, upper(:2), nvl(:3, 'was-null') from dual", (21, "abc", None)
    )

    # PL/SQL block with OUT binds.
    try:
        with conn.cursor() as cursor:
            out_num = cursor.var(oracledb.DB_TYPE_NUMBER)
            out_str = cursor.var(oracledb.DB_TYPE_VARCHAR, size=64)
            cursor.execute(
                "begin :1 := 40 + 2; :2 := 'out-' || :3; end;", [out_num, out_str, "x"]
            )
            cases["plsql_out"] = {
                "ok": True,
                "out": [encode_cell(out_num.getvalue()), encode_cell(out_str.getvalue())],
            }
    except oracledb.Error as exc:
        cases["plsql_out"] = {"ok": False, "error": ora_code(exc)}

    try:
        with conn.cursor() as cursor:
            cursor.execute("begin raise_application_error(-20001, 'boom'); end;")
        cases["err_plsql"] = {"ok": False, "error": "noora:unexpected success"}
    except oracledb.Error as exc:
        cases["err_plsql"] = {"ok": False, "error": ora_code(exc)}

    # DML rows_affected suite.
    try:
        drop_if_exists(conn, DML_TABLE)
        rows = []
        for sql in [
            f"create table {DML_TABLE} (id number(10), label varchar2(40))",
            f"insert into {DML_TABLE} (id, label) values (0, 'zero')",
            f"insert into {DML_TABLE} (id, label) "
            "select level, 'row-' || level from dual connect by level <= 3",
            f"update {DML_TABLE} set label = label || '!' where id >= 2",
            f"delete from {DML_TABLE} where id = 0",
        ]:
            rows.append(run_count(conn, sql))
        content = run_query(conn, f"select id, label from {DML_TABLE} order by id")
        if not content["ok"]:
            cases["dml_counts"] = content
        else:
            rows.extend(content["rows"])
            drop_if_exists(conn, DML_TABLE)
            cases["dml_counts"] = {"ok": True, "columns": ["DML"], "rows": rows}
    except oracledb.Error as exc:
        cases["dml_counts"] = {"ok": False, "error": ora_code(exc)}

    # LOB roundtrip (server-side construction, materialized fetch).
    try:
        drop_if_exists(conn, LOB_TABLE)
        with conn.cursor() as cursor:
            cursor.execute(f"create table {LOB_TABLE} (id number(5), c clob, b blob)")
            cursor.execute(
                f"insert into {LOB_TABLE} (id, c, b) values (1, empty_clob(), empty_blob())"
            )
            cursor.execute(
                "declare "
                "  l_c clob; l_b blob; l_chunk varchar2(1000); "
                "begin "
                f"  select c, b into l_c, l_b from {LOB_TABLE} where id = 1 for update; "
                "  for i in 0..99 loop "
                "    l_chunk := lpad(to_char(i), 10, '0') || rpad('abcdefghij', 990, 'k'); "
                "    dbms_lob.writeappend(l_c, length(l_chunk), l_chunk); "
                "    dbms_lob.writeappend(l_b, 500, utl_raw.cast_to_raw(substr(l_chunk, 1, 500))); "
                "  end loop; "
                "  commit; "
                "end;"
            )
        cases["lob_roundtrip"] = run_query(
            conn,
            "select dbms_lob.getlength(c), dbms_lob.getlength(b), c, b "
            f"from {LOB_TABLE} where id = 1",
        )
        drop_if_exists(conn, LOB_TABLE)
    except oracledb.Error as exc:
        cases["lob_roundtrip"] = {"ok": False, "error": ora_code(exc)}

    return cases


# ---------------------------------------------------------------------------
# field-by-field diff
# ---------------------------------------------------------------------------

def cells_equal(a, b):
    """Canonical cell comparison: decimals numerically, floats bit-exact,
    everything else byte-for-byte."""
    if a == b:
        return True
    if a.startswith("n:") and b.startswith("n:"):
        try:
            return decimal.Decimal(a[2:]) == decimal.Decimal(b[2:])
        except decimal.InvalidOperation:
            return False
    return False


def diff_documents(doc_a, doc_b, name_a, name_b):
    problems = []
    if doc_a.get("corpus_version") != doc_b.get("corpus_version"):
        print(
            f"corpus_version mismatch: {name_a}={doc_a.get('corpus_version')} "
            f"{name_b}={doc_b.get('corpus_version')}",
            file=sys.stderr,
        )
        sys.exit(2)
    cases_a = doc_a.get("cases", {})
    cases_b = doc_b.get("cases", {})
    for missing in sorted(set(cases_a) ^ set(cases_b)):
        side = name_b if missing in cases_a else name_a
        problems.append(f"{missing}: case missing on {side}")
    for case_id in sorted(set(cases_a) & set(cases_b)):
        a, b = cases_a[case_id], cases_b[case_id]
        if a.get("ok") != b.get("ok"):
            problems.append(
                f"{case_id}: ok mismatch {name_a}={a.get('ok')} ({a.get('error', '')}) "
                f"{name_b}={b.get('ok')} ({b.get('error', '')})"
            )
            continue
        if not a.get("ok"):
            if a.get("error") != b.get("error"):
                problems.append(
                    f"{case_id}: error mismatch {name_a}={a.get('error')!r} "
                    f"{name_b}={b.get('error')!r}"
                )
            continue
        if "out" in a or "out" in b:
            out_a, out_b = a.get("out", []), b.get("out", [])
            if len(out_a) != len(out_b):
                problems.append(
                    f"{case_id}: out-bind count {name_a}={len(out_a)} {name_b}={len(out_b)}"
                )
            else:
                for i, (ca, cb) in enumerate(zip(out_a, out_b)):
                    if not cells_equal(ca, cb):
                        problems.append(
                            f"{case_id}: out[{i}] {name_a}={ca!r} {name_b}={cb!r}"
                        )
            continue
        cols_a = [c.upper() for c in a.get("columns", [])]
        cols_b = [c.upper() for c in b.get("columns", [])]
        if cols_a != cols_b:
            problems.append(
                f"{case_id}: columns {name_a}={cols_a} {name_b}={cols_b}"
            )
        rows_a, rows_b = a.get("rows", []), b.get("rows", [])
        if len(rows_a) != len(rows_b):
            problems.append(
                f"{case_id}: row count {name_a}={len(rows_a)} {name_b}={len(rows_b)}"
            )
            continue
        for r, (row_a, row_b) in enumerate(zip(rows_a, rows_b)):
            if len(row_a) != len(row_b):
                problems.append(
                    f"{case_id}: row {r} cell count {name_a}={len(row_a)} "
                    f"{name_b}={len(row_b)}"
                )
                continue
            for c, (ca, cb) in enumerate(zip(row_a, row_b)):
                if not cells_equal(ca, cb):
                    show_a = ca if len(ca) <= 80 else f"{ca[:77]}... (len={len(ca)})"
                    show_b = cb if len(cb) <= 80 else f"{cb[:77]}... (len={len(cb)})"
                    problems.append(
                        f"{case_id}: row {r} col {c}: {name_a}={show_a!r} {name_b}={show_b!r}"
                    )
    return problems


def main_diff(path_a, path_b):
    with open(path_a, encoding="utf-8") as f:
        doc_a = json.load(f)
    with open(path_b, encoding="utf-8") as f:
        doc_b = json.load(f)
    name_a = doc_a.get("impl", os.path.basename(path_a))
    name_b = doc_b.get("impl", os.path.basename(path_b))
    problems = diff_documents(doc_a, doc_b, name_a, name_b)
    if problems:
        print(f"[ground-truth] {len(problems)} mismatch(es):")
        for p in problems:
            print(f"  MISMATCH {p}")
        return 1
    n = len(doc_a.get("cases", {}))
    print(f"[ground-truth] IDENTICAL: {n} cases match field-by-field ({name_a} vs {name_b})")
    return 0


def main_emit(argv):
    import oracledb

    connect = argv[0] if len(argv) > 0 else os.environ.get(
        "PYO_TEST_CONNECT_STRING", "localhost:1522/FREEPDB1"
    )
    user = argv[1] if len(argv) > 1 else os.environ.get("PYO_TEST_MAIN_USER", "pythontest")
    password = argv[2] if len(argv) > 2 else os.environ.get(
        "PYO_TEST_MAIN_PASSWORD", "pythontest"
    )

    # Match the Rust driver's lossless decode: NUMBER as decimal, LOBs
    # materialized.
    oracledb.defaults.fetch_decimals = True
    oracledb.defaults.fetch_lobs = False

    conn = oracledb.connect(user=user, password=password, dsn=connect)
    try:
        cases = run_corpus(conn)
    finally:
        conn.close()

    doc = {
        "harness": "statement-ground-truth",
        "impl": "python",
        "corpus_version": CORPUS_VERSION,
        "cases": cases,
    }
    json.dump(doc, sys.stdout, ensure_ascii=False, indent=1)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "--diff":
        if len(args) != 3:
            print("usage: statement_ground_truth.py --diff A.json B.json", file=sys.stderr)
            sys.exit(2)
        sys.exit(main_diff(args[1], args[2]))
    sys.exit(main_emit(args))
