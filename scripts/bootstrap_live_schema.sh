#!/usr/bin/env bash
# Bootstrap the minimal test schema for the live driver tests in a running
# Oracle Free container. The driver's live (#[ignore]) tests self-provision their
# own tables and reference NO python-oracledb schema objects, so only the
# `pythontest` main user + `pythontestproxy` proxy user (with the grants the live
# tests exercise) are required — NOT the full 50 KB reference create_schema.sql.
#
# Grants mirror reference/python-oracledb/tests/sql/create_schema.sql (the
# canonical main-user grant set) so the live tests behave identically to the
# conformance schema. Idempotent: re-running drops + recreates the users.
set -euo pipefail

CONTAINER_NAME="${ORACLEDB_CONTAINER_NAME:-rust-oracledb-free}"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-OracledbTest#2026}"
MAIN_USER="${PYO_TEST_MAIN_USER:-pythontest}"
MAIN_PASSWORD="${PYO_TEST_MAIN_PASSWORD:-testpw}"
PROXY_USER="${PYO_TEST_PROXY_USER:-pythontestproxy}"
PROXY_PASSWORD="${PYO_TEST_PROXY_PASSWORD:-proxypw}"
PDB="${ORACLEDB_PDB:-FREEPDB1}"

if [ "$MAIN_USER" = "$MAIN_PASSWORD" ]; then
  echo "bootstrap-live-schema: PYO_TEST_MAIN_PASSWORD must differ from PYO_TEST_MAIN_USER so connect-trace secret checks are meaningful" >&2
  exit 2
fi

docker exec -i "$CONTAINER_NAME" \
  sqlplus -S -L "sys/${ORACLE_PASSWORD}@localhost:1521/${PDB} as sysdba" <<SQL
whenever sqlerror exit failure
set echo off feedback off heading off verify off
-- Idempotent: drop the test users if a prior run left them behind. Terminate
-- any lingering sessions first (a leftover INACTIVE connection from a prior
-- suite otherwise makes DROP USER fail with ORA-01940 "cannot drop a user that
-- is currently connected"). This matters on the reused xe18/xe21 app-user
-- lanes where the connecting user is the same across suites.
begin
  for s in (
    select sid, serial# from v\$session
    where username in (upper('${MAIN_USER}'), upper('${PROXY_USER}'))
  ) loop
    begin
      execute immediate
        'alter system disconnect session ''' || s.sid || ',' || s.serial#
        || ''' immediate';
    exception when others then null;
    end;
  end loop;
end;
/
begin
  for u in (
    select username from dba_users
    where username in (upper('${MAIN_USER}'), upper('${PROXY_USER}'))
  ) loop
    execute immediate 'drop user "' || u.username || '" cascade';
  end loop;
end;
/
create user ${MAIN_USER} identified by ${MAIN_PASSWORD}
/
create user ${PROXY_USER} identified by ${PROXY_PASSWORD}
/
alter user ${PROXY_USER} grant connect through ${MAIN_USER}
/
grant create session to ${PROXY_USER}
/
grant
    create session,
    create table,
    create procedure,
    create type,
    create view,
    select any dictionary,
    change notification,
    unlimited tablespace
to ${MAIN_USER}
/
grant aq_administrator_role to ${MAIN_USER}
/
-- Edition for the edition-selection live test (it connects with_edition("E_TEST")
-- and asserts the session runs under it). Idempotent: tolerate "already exists".
begin
  execute immediate 'create edition E_TEST';
exception when others then if sqlcode != -955 then raise; end if;
end;
/
grant use on edition E_TEST to ${MAIN_USER}
/
-- Object types + tables the object-decode live test expects to pre-exist in the
-- main user's schema (see crates/oracledb/tests/live_object_decode.rs headers).
-- The main user was just (re)created above, so its schema is empty.
create type ${MAIN_USER}.vx6_addr as object (street varchar2(40), zip number, ok number(1))
/
create table ${MAIN_USER}.vx6_people (id number, home ${MAIN_USER}.vx6_addr)
/
create type ${MAIN_USER}.vx6_nums as varray(10) of number
/
create table ${MAIN_USER}.vx6_coll (id number, vals ${MAIN_USER}.vx6_nums)
/
insert into ${MAIN_USER}.vx6_people values (1, ${MAIN_USER}.vx6_addr('12 Oak St', 90210, 1))
/
insert into ${MAIN_USER}.vx6_people values (2, ${MAIN_USER}.vx6_addr('  ', null, 0))
/
insert into ${MAIN_USER}.vx6_coll values (1, ${MAIN_USER}.vx6_nums(10, 20, 30))
/
insert into ${MAIN_USER}.vx6_coll values (2, ${MAIN_USER}.vx6_nums(7, null, 9))
/
insert into ${MAIN_USER}.vx6_coll values (3, ${MAIN_USER}.vx6_nums())
/
commit
/
-- Optional roles the slim image may not ship (Oracle Text CTXAPP; SODA_APP for
-- the feature-gated SODA tests). Grant if present, tolerate if absent — the
-- driver live tests do not require full-text search and self-skip SODA.
begin
  execute immediate 'grant ctxapp to ${MAIN_USER}';
exception when others then null;
end;
/
begin
  execute immediate 'grant soda_app to ${MAIN_USER}';
exception when others then null;
end;
/
exit
SQL

# Synthetic AQ queue for the post-auth AQ cassette (bead iec3.1.32). Provisioned
# as the MAIN_USER (which holds aq_administrator_role, granted above) so the
# captured enqueue/dequeue slice carries no DDL. Idempotent: stop + drop first.
docker exec -i "$CONTAINER_NAME" \
  sqlplus -S -L "${MAIN_USER}/${MAIN_PASSWORD}@localhost:1521/${PDB}" <<SQL
whenever sqlerror exit failure
set echo off feedback off heading off verify off pagesize 0
declare
  procedure ignore_err(stmt varchar2) is begin
    execute immediate stmt;
  exception when others then null; end;
begin
  ignore_err(q'[begin dbms_aqadm.stop_queue(queue_name => 'RUST_CASS_RAWQ'); end;]');
  ignore_err(q'[begin dbms_aqadm.drop_queue(queue_name => 'RUST_CASS_RAWQ'); end;]');
  ignore_err(q'[begin dbms_aqadm.drop_queue_table(queue_table => 'RUST_CASS_RAWQT', force => true); end;]');
end;
/
begin
  dbms_aqadm.create_queue_table(
    queue_table        => 'RUST_CASS_RAWQT',
    queue_payload_type => 'RAW',
    multiple_consumers => false);
  dbms_aqadm.create_queue(
    queue_name  => 'RUST_CASS_RAWQ',
    queue_table => 'RUST_CASS_RAWQT');
  dbms_aqadm.start_queue(queue_name => 'RUST_CASS_RAWQ');
end;
/
-- Empty target table for the direct-path-load (DPL) post-auth cassette. Every
-- loaded row carries the same value, so the cassette's read-back is deterministic
-- even if a re-capture appends more rows. Idempotent: drop + recreate empty.
begin execute immediate 'drop table RUST_CASS_DPL purge'; exception when others then null; end;
/
create table RUST_CASS_DPL (v number(6))
/
exit
SQL

echo "bootstrap-live-schema: created ${MAIN_USER} + ${PROXY_USER} in ${PDB} (+ AQ queue RUST_CASS_RAWQ, DPL table RUST_CASS_DPL)"
