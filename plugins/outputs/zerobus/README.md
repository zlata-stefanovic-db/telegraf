# Zerobus Output Plugin

This plugin writes metrics to a Unity Catalog Delta table using the
[Databricks Zerobus Ingest][zerobus] service and its pure-Go SDK. It supports a
canonical protobuf schema and an opt-in Unity Catalog mode that derives the
protobuf schema from the destination table.

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

  ## Schema mode: canonical uses TelegrafMetric; unity_catalog maps tags and
  ## fields to columns from the destination table schema.
  # schema_mode = "canonical"

  ## Optional timestamp column for unity_catalog mode, encoded as Unix
  ## microseconds. Leave empty if the table has no timestamp column.
  # timestamp_column = "timestamp"

  ## Optional measurement-name column for unity_catalog mode.
  # measurement_column = ""

  ## In unity_catalog mode, uint64 columns must be STRING.

  ## OAuth service-principal credentials.
  client_id = ""
  client_secret = ""

  ## Optional identifier appended to Telegraf's product token.
  # application_name = ""

  ## Stream startup timeout.
  # connect_timeout = "30s"

  ## Schema-fetch timeout; zero uses the SDK default.
  # schema_fetch_timeout = "0s"

  ## Schema cache TTL; zero uses the default and negative disables caching.
  # schema_cache_ttl = "0s"

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

### Canonical schema

Canonical mode is the default. It uses one fixed protobuf record per Telegraf
metric. Create the destination table with this exact schema and column order:

```sql
CREATE TABLE catalog.schema.telegraf_metrics (
  measurement STRING NOT NULL,
  timestamp_ns BIGINT NOT NULL,
  tags MAP<STRING, STRING> NOT NULL,
  fields ARRAY<STRUCT<
    key: STRING NOT NULL,
    type: STRING NOT NULL,
    int_value: BIGINT,
    uint_value: STRING,
    float_value: DOUBLE,
    bool_value: BOOLEAN,
    string_value: STRING
  >> NOT NULL
);
```

The static descriptor must match the Delta table one-to-one. Do not reorder,
rename, remove, or change the nullability of these columns. Compatible future
schema revisions will only add nullable fields with new protobuf field numbers.

`timestamp_ns` is a raw Unix nanosecond `BIGINT`, not a Delta `TIMESTAMP`.
Unsigned integers use decimal strings because protobuf `uint64` has no
lossless Delta numeric mapping over its full range.

### Field mapping

- `int64` becomes `type = "int"` and `int_value`.
- `uint64` becomes `type = "uint"` and a decimal `uint_value`.
- `float64` becomes `type = "float"` and `float_value`.
- `bool` becomes `type = "bool"` and `bool_value`.
- `string` becomes `type = "string"` and `string_value`.

Exactly one value member is populated for each field. Telegraf normalizes input
field values to these canonical types before outputs receive them.

### Unity Catalog schema

Set `schema_mode = "unity_catalog"` to fetch the destination table schema from
Unity Catalog when the stream is created. The SDK builds a protobuf descriptor
at runtime and converts each JSON record produced by the plugin to protobuf
before admission.

Unity Catalog mode creates one flat record per metric:

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
rejected.
Unsigned integers are encoded as decimal strings to preserve the full `uint64`
range, so their destination columns must be `STRING`.

## Batching and durability

Telegraf supplies a batch of metrics to each `Write` call. The plugin serializes
the complete batch before admitting it, splits it into requests that satisfy
`max_batch_records` and `max_payload_bytes`, queues those requests with
`IngestRecordsOffset`, and calls `Flush` once. A successful `Write` therefore
means the full Telegraf batch was acknowledged. It never waits per record.

Admission and acknowledgment failures are returned to Telegraf, which keeps the
original metrics for retry. Errors include their retryability, and the plugin
does not silently discard metrics.

The plugin retains admission progress after a failed `Write`. On retry it first
confirms already admitted requests instead of admitting them again. If SDK
recovery is exhausted, it creates a new stream. Canonical mode replays only
records the SDK reports as unacknowledged. Unity Catalog mode re-encodes the
pending portion from JSON against the newly fetched schema instead of replaying
protobuf bytes that may use an obsolete descriptor. This can duplicate an
already acknowledged chunk after a terminal failure, preserving at-least-once
delivery without risking column corruption.

Each individual serialized metric must fit within the configured payload
budget. Larger Telegraf batches are split automatically.

## Development: regenerating the protobuf binding

This section is for contributors modifying the plugin's protobuf schema. After
an additive update to `metric.proto`, install `protoc-gen-go` and run:

```shell
go generate ./plugins/outputs/zerobus
```

Include the regenerated `metric.pb.go` in the same pull request as the schema
change.
