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
MAIN_PASSWORD="${PYO_TEST_MAIN_PASSWORD:-pythontest}"
PROXY_USER="${PYO_TEST_PROXY_USER:-pythontestproxy}"
PROXY_PASSWORD="${PYO_TEST_PROXY_PASSWORD:-pythontestproxy}"
PDB="${ORACLEDB_PDB:-FREEPDB1}"

docker exec -i "$CONTAINER_NAME" \
  sqlplus -S -L "system/${ORACLE_PASSWORD}@localhost:1521/${PDB}" <<SQL
whenever sqlerror exit failure
set echo off feedback off heading off verify off
-- Idempotent: drop the test users if a prior run left them behind.
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

echo "bootstrap-live-schema: created ${MAIN_USER} + ${PROXY_USER} in ${PDB}"
