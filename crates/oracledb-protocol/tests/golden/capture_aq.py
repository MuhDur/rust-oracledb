import os, sys, oracledb

u = os.environ["PYO_TEST_MAIN_USER"]
p = os.environ["PYO_TEST_MAIN_PASSWORD"]
dsn = os.environ["PYO_TEST_CONNECT_STRING"]

mode = sys.argv[1] if len(sys.argv) > 1 else "raw"

conn = oracledb.connect(user=u, password=p, dsn=dsn)
cur = conn.cursor()

def clear(qname, ptype=None):
    q = conn.queue(qname, ptype)
    q.deqoptions.wait = oracledb.DEQ_NO_WAIT
    q.deqoptions.deliverymode = oracledb.MSG_PERSISTENT_OR_BUFFERED
    q.deqoptions.visibility = oracledb.DEQ_IMMEDIATE
    while q.deqone():
        pass
    return conn.queue(qname, ptype)

sys.stderr.write("\n===CLEAR_DONE marker for %s===\n" % mode)
sys.stderr.flush()

if mode == "raw":
    q = clear("TEST_RAW_QUEUE")
    sys.stderr.write("\n===ENQ_RAW===\n"); sys.stderr.flush()
    props = conn.msgproperties(payload=b"sample raw data 1", correlation="CORR1", priority=2)
    q.enqone(props)
    conn.commit()
    sys.stderr.write("\n===ENQ_RAW_MSGID=%s===\n" % props.msgid.hex()); sys.stderr.flush()
    sys.stderr.write("\n===DEQ_RAW===\n"); sys.stderr.flush()
    q.deqoptions.wait = oracledb.DEQ_NO_WAIT
    q.deqoptions.navigation = oracledb.DEQ_FIRST_MSG
    got = q.deqone()
    conn.commit()
    sys.stderr.write("\n===DEQ_RAW_PAYLOAD=%r===\n" % (got.payload if got else None)); sys.stderr.flush()

elif mode == "obj":
    typ = conn.gettype("UDT_BOOK")
    q = clear("TEST_BOOK_QUEUE", typ)
    book = typ.newobject()
    book.TITLE = "Test Book"; book.AUTHORS = "An Author"; book.PRICE = 1.5
    sys.stderr.write("\n===ENQ_OBJ===\n"); sys.stderr.flush()
    props = conn.msgproperties(payload=book)
    q.enqone(props)
    conn.commit()
    sys.stderr.write("\n===DEQ_OBJ===\n"); sys.stderr.flush()
    q.deqoptions.wait = oracledb.DEQ_NO_WAIT
    q.deqoptions.navigation = oracledb.DEQ_FIRST_MSG
    got = q.deqone()
    conn.commit()
    sys.stderr.write("\n===DEQ_OBJ_TITLE=%r===\n" % (got.payload.TITLE if got else None)); sys.stderr.flush()

elif mode == "json":
    q = clear("TEST_JSON_QUEUE", "JSON")
    sys.stderr.write("\n===ENQ_JSON===\n"); sys.stderr.flush()
    props = conn.msgproperties(payload=dict(name="John", age=30, city="NY"))
    q.enqone(props)
    conn.commit()
    sys.stderr.write("\n===DEQ_JSON===\n"); sys.stderr.flush()
    q.deqoptions.wait = oracledb.DEQ_NO_WAIT
    q.deqoptions.navigation = oracledb.DEQ_FIRST_MSG
    got = q.deqone()
    conn.commit()
    sys.stderr.write("\n===DEQ_JSON_PAYLOAD=%r===\n" % (got.payload if got else None)); sys.stderr.flush()

elif mode == "bulk":
    q = clear("TEST_RAW_QUEUE")
    sys.stderr.write("\n===ENQMANY_RAW===\n"); sys.stderr.flush()
    msgs = [conn.msgproperties(payload=d) for d in [b"m1", b"m2", b"m3"]]
    q.enqmany(msgs)
    conn.commit()
    sys.stderr.write("\n===DEQMANY_RAW===\n"); sys.stderr.flush()
    q.deqoptions.wait = oracledb.DEQ_NO_WAIT
    q.deqoptions.navigation = oracledb.DEQ_FIRST_MSG
    got = q.deqmany(5)
    conn.commit()
    sys.stderr.write("\n===DEQMANY_RAW_N=%d===\n" % len(got)); sys.stderr.flush()

conn.close()
