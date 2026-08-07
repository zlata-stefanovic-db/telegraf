# Zerobus Output Plugin

This plugin writes metrics to a Unity Catalog Delta table using the
[Databricks Zerobus Ingest][zerobus] service and its pure-Go SDK. It supports a
static schema that stores arbitrary metrics in a fixed envelope and an opt-in
table-schema mode that derives the protobuf schema from the destination table.

> [!IMPORTANT]
> Be aware that this plugin accesses APIs that are [chargeable][pricing] and
> might incur costs.

⭐ Telegraf v1.40.0
🏷️ cloud, datastore
💻 all

[pricing]: https://www.databricks.com/product/pricing/lakeflow-connect
[zerobus]: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Secret store support

This plugin supports secrets from secret stores for the `client_secret` option.
See the [secret store documentation][SECRETSTORE] for more details on how
to use them.

[SECRETSTORE]: ../../../docs/CONFIGURATION.md#secret-store-secrets

## Configuration

```toml @sample.conf
# Configuration for sending metrics to Databricks Zerobus
[[outputs.zerobus]]
  ## Zerobus gRPC service endpoint.
  zerobus_server_endpoint = "https://<workspace-id>.zerobus.<region>.cloud.databricks.com"

  ## Databricks workspace URL used for OAuth authentication.
  workspace_url = "https://<workspace>.cloud.databricks.com"

  ## Fully qualified Unity Catalog destination table.
  table_name = "catalog.schema.telegraf_metrics"

  ## Schema mode: static stores fields in a VARIANT column; table_schema maps
  ## tags and fields to columns from the destination table schema.
  # schema_mode = "static"

  ## Optional timestamp column for table_schema mode, encoded as Unix
  ## microseconds. Leave empty if the table has no timestamp column.
  # timestamp_column = "timestamp"

  ## Optional measurement-name column for table_schema mode.
  # measurement_column = ""

  ## uint64 fields require BIGINT columns; values above the BIGINT maximum are
  ## unsupported.

  ## OAuth service-principal credentials.
  client_id = ""
  client_secret = ""

  ## Optional identifier appended to Telegraf's product token.
  # application_name = ""

  ## Stream startup timeout.
  # connect_timeout = "30s"

  ## Schema-fetch timeout; zero uses the SDK default.
  # schema_fetch_timeout = "0s"

  ## Unacknowledged ingest-call limit; zero uses the SDK default.
  # max_inflight = 0

  ## Buffered payload limit; zero uses the SDK default.
  # max_buffered_payload_bytes = "0B"

  ## Records per request; larger batches are split.
  # max_batch_records = 100000

  ## Request size limit; zero uses the SDK default below 10 MiB.
  # max_payload_bytes = "0B"

  ## Recovery-attempt limit; zero uses the SDK default.
  # recovery_retries = 0

  ## Recovery-attempt timeout; zero uses the SDK default.
  # recovery_timeout = "0s"

  ## Recovery delay; zero uses the SDK default.
  # recovery_backoff = "0s"

  ## Acknowledgment timeout; zero uses the SDK default.
  # lack_of_ack_timeout = "0s"

  ## Flush timeout; zero uses the SDK default.
  # flush_timeout = "0s"
```

The service principal identified by `client_id` must have `USE CATALOG` on the
catalog, `USE SCHEMA` on the schema, and both `SELECT` and `MODIFY` on the
table. `Connect` waits for stream startup for up to `connect_timeout`, returning
network, authentication, and schema errors before metrics are written.

## Schema modes

### Static schema

Static mode is the default. It uses one fixed protobuf record per Telegraf
metric, with the metric fields in a `VARIANT` column. Create the destination
table with this exact schema and column order:

```sql
CREATE TABLE catalog.schema.telegraf_metrics (
  measurement STRING NOT NULL,
  timestamp_ns BIGINT NOT NULL,
  tags MAP<STRING, STRING> NOT NULL,
  fields VARIANT NOT NULL
);
```

The static descriptor must match the Delta table one-to-one. Do not reorder,
rename, remove, or change the nullability of these columns. Compatible future
schema revisions will only add nullable fields with new protobuf field numbers.

`timestamp_ns` is a raw Unix nanosecond `BIGINT`, not a Delta `TIMESTAMP`.

### Field mapping

All fields of a metric become one JSON object in the `fields` `VARIANT` column,
which Zerobus transports as a protobuf string. Field names and types can
therefore change without altering the destination table.

- `int64` becomes a JSON number.
- `uint64` values through `math.MaxInt64` become JSON numbers; larger values
  are rejected because Delta cannot represent the full `uint64` range.
- `float64` becomes a JSON number; non-finite values such as `NaN` are rejected
  because JSON cannot represent them.
- `bool` becomes a JSON boolean.
- `string` becomes a JSON string.

Telegraf normalizes input field values to these supported types before outputs
receive them. The plugin then encodes them as JSON text, so the type stored in
the `VARIANT` column follows how a value is written rather than its Telegraf
type. A `float64` holding an integer value is indistinguishable from an
`int64`, so Databricks reads it back as a `BIGINT`. The same field can therefore
be an integer in one row and a decimal in another.

Cast extracted values to a concrete type when querying. The cast pins the type
across rows and is required for filtering, grouping, ordering, and aggregation,
because `VARIANT` values cannot be compared, grouped, ordered, or used in set
operations. Use `:` to select a field from the column and `::` to cast it:

```sql
SELECT
  measurement,
  tags['host'] AS host,
  fields:usage_idle::double AS usage_idle
FROM catalog.schema.telegraf_metrics;
```

A `::` cast fails the query when a value cannot be converted. Use
`try_variant_get(fields, '$.usage_idle', 'double')` to return `NULL` instead.

### Table schema

Set `schema_mode = "table_schema"` to fetch the destination table schema from
Unity Catalog before creating the stream. The SDK builds a protobuf descriptor,
opens a regular protobuf stream with it, and converts each JSON record produced
by the plugin to protobuf before admission. The SDK caches fetched descriptors
for five minutes.

Table-schema mode creates one flat record per metric:

- If configured, the metric timestamp is written to `timestamp_column` as Unix
  microseconds, which is the representation expected by a Delta `TIMESTAMP`.
- Tags and fields become same-named top-level columns.
- The measurement name is omitted unless `measurement_column` is configured.

For example, a metric with the tag `host` and field `usage` can target:

```sql
CREATE TABLE catalog.schema.cpu_metrics (
  timestamp TIMESTAMP NOT NULL,
  host STRING,
  usage DOUBLE
);
```

All metrics from one plugin instance target the configured table and must match
its schema. Use Telegraf filtering or processors when separate measurements need
different tables or column layouts.

Declare columns as `BIGINT` for `int64` and `uint64`, `DOUBLE` for `float64`,
`BOOLEAN` for `bool`, and `STRING` for `string`; the value limits from
[Field mapping](#field-mapping) apply here too. A tag, field, or metadata column
name collision is rejected before admission, as are table schemas with nullable
arrays or maps, or collections that allow null elements, which protobuf cannot
represent.

## Batching and durability

Each `Write` serializes the metrics, splits them into requests that fit the
record, payload, and buffered-payload limits, and flushes once. A successful
`Write` means every record was acknowledged.

Failures return to Telegraf, which retries the original metrics. A retry resumes
where the previous attempt stopped rather than re-sending acknowledged records.
A metric that cannot be serialized or does not fit the budgets is rejected on
its own, so the rest of the batch is still written.

## Development: regenerating the protobuf binding

This section is for contributors modifying the plugin's protobuf schema. After
an additive update to `metric.proto`, install `protoc-gen-go` and run:

```shell
go generate ./plugins/outputs/zerobus
```

Include the regenerated `metric.pb.go` in the same pull request as the schema
change.
