\echo Use "CREATE EXTENSION pg_micromanage" to load this file. \quit

CREATE FUNCTION dump_query(text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION run_select(text)
RETURNS SETOF int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION encode_protobuf(text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
