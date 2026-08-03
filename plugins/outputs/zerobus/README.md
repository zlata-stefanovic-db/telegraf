# Zerobus Output Plugin

This plugin writes metrics to a Unity Catalog Delta table using the
[Databricks Zerobus Ingest][zerobus] service and its pure-Go SDK.

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

  ## OAuth service-principal credentials.
  client_id = ""
  client_secret = ""

  ## Identifier appended to the Zerobus SDK user-agent.
  # application_name = "telegraf"

  ## Maximum number of unacknowledged ingest calls before applying backpressure.
  ## A value of zero uses the SDK default.
  # max_inflight = 0

  ## Maximum encoded payload bytes retained by queued and in-flight records.
  ## A value of zero uses the SDK default.
  # max_buffered_payload_bytes = "0B"

  ## Maximum records per Zerobus ingest request. Larger Telegraf batches are
  ## split into multiple requests before one final flush.
  # max_batch_records = 100000

  ## Maximum encoded size of one Zerobus ingest request. Larger batches are
  ## split automatically, but each individual metric must fit this limit.
  ## A value of zero uses the SDK default, just below the 10 MiB service limit.
  # max_payload_bytes = "0B"

  ## Maximum consecutive stream-recovery attempts.
  ## A value of zero uses the SDK default.
  # recovery_retries = 0

  ## Timeout for each stream-open attempt during recovery.
  ## A value of zero uses the SDK default.
  # recovery_timeout = "0s"

  ## Delay between stream-recovery attempts.
  ## A value of zero uses the SDK default.
  # recovery_backoff = "0s"

  ## Time records may remain unacknowledged before recovery starts.
  ## A value of zero uses the SDK default.
  # lack_of_ack_timeout = "0s"

  ## Maximum time Write waits for all records in a batch to be acknowledged.
  ## A value of zero uses the SDK default.
  # flush_timeout = "0s"
```

The service principal identified by `client_id` must have permission to write
to the configured table. The pure-Go SDK opens a stream asynchronously, so
network, authentication, and schema errors can first appear during `Write`.

## Destination table

The plugin uses one static protobuf record per Telegraf metric. Create the
destination table with this exact schema and column order:

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

## Batching and durability

Telegraf supplies a batch of metrics to each `Write` call. The plugin serializes
the complete batch before admitting it, splits it into requests that satisfy
`max_batch_records` and `max_payload_bytes`, queues those requests with
`IngestRecordsOffset`, and calls `Flush` once. A successful `Write` therefore
means the full Telegraf batch was acknowledged. It never waits per record.

Admission and acknowledgment failures are returned to Telegraf, which keeps the
original metrics for retry. Non-retryable Zerobus errors are identified in the
error and plugin log, but the plugin does not silently discard metrics.

The plugin retains admission progress after a failed `Write`. On retry it first
confirms already admitted requests instead of admitting them again. If SDK
recovery is exhausted, it creates a new stream and replays only records the SDK
reports as unacknowledged.

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
