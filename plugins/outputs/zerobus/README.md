# Zerobus Output Plugin

This plugin writes metrics to a Unity Catalog Delta table using the
[Databricks Zerobus Ingest][zerobus] service and its pure-Go SDK. It supports a
static schema that stores arbitrary metrics in a fixed envelope and an opt-in
table-schema mode that derives the protobuf schema from the destination table.

⭐ Telegraf v1.40.0
🏷️ cloud, datastore
💻 all

[zerobus]: https://docs.databricks.com/aws/en/ingestion/zerobus-ingest

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Secret store support

This plugin supports secrets from secret stores for the `client_secret` option.
See the [secret store documentation][SECRETSTORE] for details.

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

The service principal identified by `client_id` must have permission to write
to the configured table. `Connect` waits for stream startup for up to
`connect_timeout`, returning network, authentication, and schema errors before
metrics are written.

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
which Zerobus transports as a protobuf string. Field names and types therefore can be changed without altering the destination table.

- `int64` becomes a JSON number.
- `uint64` values through `math.MaxInt64` become JSON numbers; larger values
  are rejected because Delta cannot represent the full `uint64` range.
- `float64` becomes a JSON number; non-finite values such as `NaN` are rejected
  because JSON cannot represent them.
- `bool` becomes a JSON boolean.
- `string` becomes a JSON string.

Telegraf normalizes input field values to these supported types before outputs
receive them. Variant keeps the JSON type of each value rather than the
Telegraf type, so a float with an integral value is stored as an integer. Cast
on read:

```sql
SELECT
  measurement,
  tags['host'] AS host,
  fields:usage_idle::double AS usage_idle
FROM catalog.schema.telegraf_metrics;
```

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

All metrics sent through one plugin instance target the configured table and
must match its schema. Use Telegraf filtering or processors when separate
measurements require different tables or column layouts. A tag, field, or
configured metadata-column name collision is rejected before admission.
Non-finite floats cannot be represented in the intermediate JSON and are also
rejected. The SDK rejects table schemas containing nullable arrays or maps, or
collections that allow null elements or values, because protobuf cannot
preserve those distinctions.

Telegraf field types are preserved where they have a lossless Delta
representation. Recommended Delta columns are `BIGINT` for `int64`, `DOUBLE`
for `float64`, `BOOLEAN` for `bool`, and `STRING` for `string`. The schema does
not support the full `uint64` range: destination columns for `uint64` fields
must be `BIGINT`, and values above the `BIGINT` maximum are unsupported and
rejected.

## Batching and durability

Telegraf supplies a batch of metrics to each `Write` call. The plugin serializes
each metric, splits valid records into requests that satisfy the record,
payload, and buffered-payload limits, queues those requests with
`IngestRecordsOffset`, and calls `Flush` once. A successful `Write` therefore
means all valid records were acknowledged. It never waits per record.

Admission and acknowledgment failures are returned to Telegraf, which keeps the
original metrics for retry. A deterministic serialization or size failure
rejects only the affected metric and reports its error to Telegraf; valid
metrics in the same batch are still written.

The plugin retains admission progress after a failed `Write`. On retry it first
confirms already admitted requests instead of admitting them again. If SDK
recovery is exhausted, it creates a new stream. Static mode replays only
records the SDK reports as unacknowledged. Table-schema mode re-encodes the
pending portion from JSON against the descriptor selected for the replacement
stream instead of replaying protobuf bytes that may use an obsolete descriptor.

Each individual serialized metric must fit within the configured payload
and buffered-payload budgets. Larger Telegraf batches are split automatically.

## Development: regenerating the protobuf binding

This section is for contributors modifying the plugin's protobuf schema. After
an additive update to `metric.proto`, install `protoc-gen-go` and run:

```shell
go generate ./plugins/outputs/zerobus
```

Include the regenerated `metric.pb.go` in the same pull request as the schema
change.
