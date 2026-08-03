package zerobus

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	sdkzerobus "github.com/databricks/zerobus-sdk/purego/zerobus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/testutil"
)

func TestDefaults(t *testing.T) {
	creator, found := outputs.Outputs["zerobus"]
	require.True(t, found)

	plugin, ok := creator().(*Zerobus)
	require.True(t, ok)
	require.Equal(t, "telegraf", plugin.ApplicationName)
	require.Equal(t, schemaModeCanonical, plugin.SchemaMode)
	require.Equal(t, "timestamp", plugin.TimestampColumn)
	require.Equal(t, defaultMaxBatchRecords, plugin.MaxBatchRecords)
	require.NotEmpty(t, plugin.SampleConfig())
}

func TestInitRequiredOptions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Zerobus)
		option string
	}{
		{
			name:   "server endpoint",
			mutate: func(z *Zerobus) { z.ServerEndpoint = "" },
			option: "zerobus_server_endpoint",
		},
		{
			name:   "workspace URL",
			mutate: func(z *Zerobus) { z.WorkspaceURL = "" },
			option: "workspace_url",
		},
		{
			name:   "table name",
			mutate: func(z *Zerobus) { z.TableName = "" },
			option: "table_name",
		},
		{
			name:   "client ID",
			mutate: func(z *Zerobus) { z.ClientID = "" },
			option: "client_id",
		},
		{
			name:   "client secret",
			mutate: func(z *Zerobus) { z.ClientSecret = config.Secret{} },
			option: "client_secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := validPlugin()
			tt.mutate(plugin)
			require.ErrorContains(t, plugin.Init(), tt.option)
		})
	}
}

func TestInitRejectsInvalidTuning(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Zerobus)
		option string
	}{
		{
			name:   "negative max inflight",
			mutate: func(z *Zerobus) { z.MaxInflight = -1 },
			option: "max_inflight",
		},
		{
			name:   "negative buffered bytes",
			mutate: func(z *Zerobus) { z.MaxBufferedPayloadBytes = -1 },
			option: "max_buffered_payload_bytes",
		},
		{
			name:   "zero batch records",
			mutate: func(z *Zerobus) { z.MaxBatchRecords = 0 },
			option: "max_batch_records",
		},
		{
			name:   "negative payload bytes",
			mutate: func(z *Zerobus) { z.MaxPayloadBytes = -1 },
			option: "max_payload_bytes",
		},
		{
			name:   "payload bytes without envelope room",
			mutate: func(z *Zerobus) { z.MaxPayloadBytes = batchEnvelopeReserve },
			option: "max_payload_bytes",
		},
		{
			name:   "negative recovery retries",
			mutate: func(z *Zerobus) { z.RecoveryRetries = -1 },
			option: "recovery_retries",
		},
		{
			name:   "negative recovery timeout",
			mutate: func(z *Zerobus) { z.RecoveryTimeout = -1 },
			option: "recovery_timeout",
		},
		{
			name:   "negative recovery backoff",
			mutate: func(z *Zerobus) { z.RecoveryBackoff = -1 },
			option: "recovery_backoff",
		},
		{
			name:   "negative ack timeout",
			mutate: func(z *Zerobus) { z.LackOfAckTimeout = -1 },
			option: "lack_of_ack_timeout",
		},
		{
			name:   "negative flush timeout",
			mutate: func(z *Zerobus) { z.FlushTimeout = -1 },
			option: "flush_timeout",
		},
		{
			name:   "negative schema fetch timeout",
			mutate: func(z *Zerobus) { z.SchemaFetchTimeout = -1 },
			option: "schema_fetch_timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := validPlugin()
			tt.mutate(plugin)
			require.ErrorContains(t, plugin.Init(), tt.option)
		})
	}
}

func TestInitSchemaMode(t *testing.T) {
	t.Run("normalizes mode", func(t *testing.T) {
		plugin := validPlugin()
		plugin.SchemaMode = " UNITY_CATALOG "
		require.NoError(t, plugin.Init())
		require.Equal(t, schemaModeUnityCatalog, plugin.SchemaMode)
	})

	t.Run("rejects unknown mode", func(t *testing.T) {
		plugin := validPlugin()
		plugin.SchemaMode = "automatic"
		require.ErrorContains(t, plugin.Init(), "schema_mode")
	})

	t.Run("requires Unity Catalog timestamp column", func(t *testing.T) {
		plugin := validPlugin()
		plugin.SchemaMode = schemaModeUnityCatalog
		plugin.TimestampColumn = ""
		require.ErrorContains(t, plugin.Init(), "timestamp_column")
	})

	t.Run("rejects reserved column collision", func(t *testing.T) {
		plugin := validPlugin()
		plugin.SchemaMode = schemaModeUnityCatalog
		plugin.MeasurementColumn = plugin.TimestampColumn
		require.ErrorContains(t, plugin.Init(), "must be different")
	})
}

func TestMessageDescriptor(t *testing.T) {
	raw, err := messageDescriptor()
	require.NoError(t, err)

	var descriptor descriptorpb.DescriptorProto
	require.NoError(t, proto.Unmarshal(raw, &descriptor))
	require.Equal(t, "TelegrafMetric", descriptor.GetName())
	require.Len(t, descriptor.Field, 4)

	require.Equal(t, "measurement", descriptor.Field[0].GetName())
	require.Equal(t, int32(1), descriptor.Field[0].GetNumber())
	require.Equal(t, descriptorpb.FieldDescriptorProto_LABEL_REQUIRED, descriptor.Field[0].GetLabel())
	require.Equal(t, "timestamp_ns", descriptor.Field[1].GetName())
	require.Equal(t, int32(2), descriptor.Field[1].GetNumber())
	require.Equal(t, "tags", descriptor.Field[2].GetName())
	require.Equal(t, "fields", descriptor.Field[3].GetName())
	require.Equal(
		t,
		".telegraf.zerobus.v1.TelegrafMetric.FieldValue",
		descriptor.Field[3].GetTypeName(),
	)

	var fieldValue *descriptorpb.DescriptorProto
	for _, nested := range descriptor.NestedType {
		if nested.GetName() == "FieldValue" {
			fieldValue = nested
			break
		}
	}
	require.NotNil(t, fieldValue, "FieldValue must be nested so the descriptor is self-contained")
	require.Len(t, fieldValue.Field, 7)
	require.Equal(t, "key", fieldValue.Field[0].GetName())
	require.Equal(t, descriptorpb.FieldDescriptorProto_LABEL_REQUIRED, fieldValue.Field[0].GetLabel())
	require.Equal(t, "string_value", fieldValue.Field[6].GetName())
}

func TestMetricToProtoPreservesTypesAndOrder(t *testing.T) {
	timestamp := time.Unix(1_700_000_000, 123)
	input := metric.New(
		"cpu",
		map[string]string{"host": "server-01", "region": "west"},
		map[string]interface{}{
			"z-string": "ready",
			"b-uint":   uint64(math.MaxUint64),
			"d-bool":   true,
			"a-int":    int64(-42),
			"c-float":  1.25,
		},
		timestamp,
	)

	record, err := metricToProto(input)
	require.NoError(t, err)
	require.Equal(t, "cpu", record.GetMeasurement())
	require.Equal(t, timestamp.UnixNano(), record.GetTimestampNs())
	require.Equal(t, map[string]string{"host": "server-01", "region": "west"}, record.GetTags())

	keys := make([]string, 0, len(record.Fields))
	for _, field := range record.Fields {
		keys = append(keys, field.GetKey())
	}
	require.Equal(t, []string{"a-int", "b-uint", "c-float", "d-bool", "z-string"}, keys)

	require.Equal(t, "int", record.Fields[0].GetType())
	require.Equal(t, int64(-42), record.Fields[0].GetIntValue())
	require.Nil(t, record.Fields[0].UintValue)
	require.Equal(t, "uint", record.Fields[1].GetType())
	require.Equal(t, "18446744073709551615", record.Fields[1].GetUintValue())
	require.Equal(t, "float", record.Fields[2].GetType())
	require.Equal(t, 1.25, record.Fields[2].GetFloatValue())
	require.Equal(t, "bool", record.Fields[3].GetType())
	require.True(t, record.Fields[3].GetBoolValue())
	require.Equal(t, "string", record.Fields[4].GetType())
	require.Equal(t, "ready", record.Fields[4].GetStringValue())
}

func TestMetricToUnityCatalogJSONFlattensMetric(t *testing.T) {
	input := metric.New(
		"cpu",
		map[string]string{"host": "server-01"},
		map[string]interface{}{
			"active": true,
			"count":  int64(-42),
			"ratio":  1.25,
			"status": "ready",
			"total":  uint64(math.MaxUint64),
		},
		time.Unix(1_700_000_000, 123_456_000),
	)

	record, err := metricToUnityCatalogJSON(input, "event_time", "measurement")
	require.NoError(t, err)

	var values map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(record, &values))
	require.JSONEq(t, `"cpu"`, string(values["measurement"]))
	require.JSONEq(t, `"server-01"`, string(values["host"]))
	require.JSONEq(t, `1700000000123456`, string(values["event_time"]))
	require.JSONEq(t, `-42`, string(values["count"]))
	require.JSONEq(t, `"18446744073709551615"`, string(values["total"]))
	require.JSONEq(t, `1.25`, string(values["ratio"]))
	require.JSONEq(t, `true`, string(values["active"]))
	require.JSONEq(t, `"ready"`, string(values["status"]))
}

func TestMetricToUnityCatalogJSONRejectsInvalidMetric(t *testing.T) {
	tests := []struct {
		name   string
		metric telegraf.Metric
		match  string
	}{
		{
			name: "timestamp collision",
			metric: metric.New(
				"cpu",
				map[string]string{"timestamp": "tag"},
				map[string]interface{}{"value": 1.0},
				time.Now(),
			),
			match: `tag "timestamp" conflicts`,
		},
		{
			name: "tag and field collision",
			metric: metric.New(
				"cpu",
				map[string]string{"host": "tag"},
				map[string]interface{}{"host": "field"},
				time.Now(),
			),
			match: `field "host" conflicts`,
		},
		{
			name: "non-finite float",
			metric: metric.New(
				"cpu",
				nil,
				map[string]interface{}{"value": math.NaN()},
				time.Now(),
			),
			match: "non-finite float",
		},
		{
			name: "unsupported field",
			metric: metricWithFields{
				Metric: testutil.TestMetric(1),
				fields: []*telegraf.Field{
					{Key: "value", Value: []int{1}},
				},
			},
			match: "unsupported type []int",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := metricToUnityCatalogJSON(tt.metric, "timestamp", "")
			require.ErrorContains(t, err, tt.match)
		})
	}
}

func TestFieldToProtoRejectsUnsupportedValue(t *testing.T) {
	_, err := fieldToProto(&telegraf.Field{Key: "invalid", Value: []int{1}})
	require.ErrorContains(t, err, "unsupported field type []int")
}

func TestMetricToProtoRejectsNilField(t *testing.T) {
	input := metricWithFields{
		Metric: testutil.TestMetric(1),
		fields: []*telegraf.Field{
			nil,
			{Key: "valid", Value: int64(1)},
		},
	}
	_, err := metricToProto(input)
	require.ErrorContains(t, err, "contains a nil field")
}

func TestConnectPassesConfiguration(t *testing.T) {
	stream := &fakeStream{}
	sdk := &fakeSDK{stream: stream}
	plugin := validPlugin()
	plugin.ApplicationName = "telegraf-test"
	plugin.MaxInflight = 12
	plugin.MaxBufferedPayloadBytes = 1_024
	plugin.MaxBatchRecords = 123
	plugin.MaxPayloadBytes = 2_048
	plugin.RecoveryRetries = 3
	plugin.RecoveryTimeout = config.Duration(time.Second)
	plugin.RecoveryBackoff = config.Duration(2 * time.Second)
	plugin.LackOfAckTimeout = config.Duration(3 * time.Second)
	plugin.FlushTimeout = config.Duration(4 * time.Second)

	var gotServer, gotWorkspace string
	var sdkOptionCount int
	plugin.newSDK = func(server, workspace string, options ...sdkzerobus.Option) (sdkClient, error) {
		gotServer = server
		gotWorkspace = workspace
		sdkOptionCount = len(options)
		return sdk, nil
	}

	require.NoError(t, plugin.Init())
	require.NoError(t, plugin.Connect())
	require.Equal(t, plugin.ServerEndpoint, gotServer)
	require.Equal(t, plugin.WorkspaceURL, gotWorkspace)
	require.Equal(t, 1, sdkOptionCount)
	require.Equal(t, plugin.TableName, sdk.tableName)
	require.Equal(t, plugin.ClientID, sdk.clientID)
	require.Equal(t, "secret", sdk.clientSecret)
	require.Len(t, sdk.options, 10)
	require.Same(t, stream, plugin.stream)
}

func TestConnectCreatesUnityCatalogStream(t *testing.T) {
	stream := &fakeStream{}
	sdk := &fakeSDK{stream: stream}
	plugin := validPlugin()
	plugin.SchemaMode = schemaModeUnityCatalog
	plugin.SchemaFetchTimeout = config.Duration(5 * time.Second)
	plugin.SchemaCacheTTL = config.Duration(-1)

	var sdkOptionCount int
	plugin.newSDK = func(_ string, _ string, options ...sdkzerobus.Option) (sdkClient, error) {
		sdkOptionCount = len(options)
		return sdk, nil
	}

	require.NoError(t, plugin.Init())
	require.NoError(t, plugin.Connect())
	require.Equal(t, 3, sdkOptionCount)
	require.Zero(t, sdk.createCalls)
	require.Equal(t, 1, sdk.unityCatalogCalls)
	require.Len(t, sdk.options, 1)
	require.Same(t, stream, plugin.stream)
}

func TestConnectClosesSDKWhenStreamCreationFails(t *testing.T) {
	createErr := errors.New("create stream failed")
	sdk := &fakeSDK{createErr: createErr}
	plugin := validPlugin()
	plugin.newSDK = func(string, string, ...sdkzerobus.Option) (sdkClient, error) {
		return sdk, nil
	}

	require.NoError(t, plugin.Init())
	err := plugin.Connect()
	require.ErrorIs(t, err, createErr)
	require.Equal(t, 1, sdk.closeCalls)
}

func TestConnectReturnsSDKCreationError(t *testing.T) {
	createErr := errors.New("create SDK failed")
	plugin := validPlugin()
	plugin.newSDK = func(string, string, ...sdkzerobus.Option) (sdkClient, error) {
		return nil, createErr
	}

	require.NoError(t, plugin.Init())
	require.ErrorIs(t, plugin.Connect(), createErr)
}

func TestWriteBatchesAndFlushesOnce(t *testing.T) {
	stream := &fakeStream{}
	plugin := validPlugin()
	plugin.stream = stream
	metrics := []telegraf.Metric{
		metric.New("cpu", map[string]string{"host": "a"}, map[string]interface{}{"usage": 1.5}, time.Unix(1, 2)),
		metric.New("mem", nil, map[string]interface{}{"used": uint64(7)}, time.Unix(3, 4)),
	}

	require.NoError(t, plugin.Write(metrics))
	require.Equal(t, 1, stream.ingestCalls)
	require.Equal(t, 1, stream.flushCalls)
	require.Len(t, stream.records, 2)

	var first TelegrafMetric
	require.NoError(t, proto.Unmarshal(stream.records[0], &first))
	require.Equal(t, "cpu", first.GetMeasurement())
	require.Equal(t, "usage", first.Fields[0].GetKey())
}

func TestWriteUnityCatalogSchemaUsesJSON(t *testing.T) {
	stream := &fakeStream{}
	plugin := validPlugin()
	plugin.SchemaMode = schemaModeUnityCatalog
	plugin.MeasurementColumn = "measurement"
	plugin.stream = stream
	input := metric.New(
		"cpu",
		map[string]string{"host": "a"},
		map[string]interface{}{"usage": 1.5},
		time.Unix(1, 2_000),
	)

	require.NoError(t, plugin.Write([]telegraf.Metric{input}))
	require.Equal(t, 1, stream.ingestCalls)
	require.Equal(t, []bool{false}, stream.encoded)
	require.Len(t, stream.records, 1)
	require.JSONEq(
		t,
		`{"host":"a","measurement":"cpu","timestamp":1000002,"usage":1.5}`,
		string(stream.records[0]),
	)
}

func TestWriteIsDeterministic(t *testing.T) {
	stream := &fakeStream{}
	plugin := validPlugin()
	plugin.stream = stream
	input := metric.New(
		"cpu",
		map[string]string{"z": "last", "a": "first"},
		map[string]interface{}{"z": "last", "a": int64(1)},
		time.Unix(1, 2),
	)

	require.NoError(t, plugin.Write([]telegraf.Metric{input}))
	first := slices.Clone(stream.records[0])
	require.NoError(t, plugin.Write([]telegraf.Metric{input}))
	require.Equal(t, first, stream.records[0])
}

func TestWriteFailures(t *testing.T) {
	admissionErr := context.Canceled
	flushErr := errors.New("flush failed")

	t.Run("not connected", func(t *testing.T) {
		require.ErrorIs(t, validPlugin().Write([]telegraf.Metric{testutil.TestMetric(1)}), internal.ErrNotConnected)
	})

	t.Run("batch is split by record count", func(t *testing.T) {
		stream := &fakeStream{}
		plugin := validPlugin()
		plugin.MaxBatchRecords = 1
		plugin.stream = stream
		require.NoError(t, plugin.Write([]telegraf.Metric{testutil.TestMetric(1), testutil.TestMetric(2)}))
		require.Equal(t, 2, stream.ingestCalls)
		require.Equal(t, 1, stream.flushCalls)
		require.Len(t, stream.batches, 2)
		require.Len(t, stream.batches[0], 1)
		require.Len(t, stream.batches[1], 1)
	})

	t.Run("unsupported field", func(t *testing.T) {
		stream := &fakeStream{}
		plugin := validPlugin()
		plugin.stream = stream
		input := metricWithFields{
			Metric: testutil.TestMetric(1),
			fields: []*telegraf.Field{
				{Key: "unsupported", Value: []int{1}},
			},
		}
		err := plugin.Write([]telegraf.Metric{input})
		require.ErrorContains(t, err, "unsupported field type []int")
		require.Zero(t, stream.ingestCalls)
	})

	t.Run("admission", func(t *testing.T) {
		stream := &fakeStream{ingestErr: admissionErr}
		plugin := validPlugin()
		plugin.stream = stream
		err := plugin.Write([]telegraf.Metric{testutil.TestMetric(1)})
		require.ErrorIs(t, err, admissionErr)
		require.ErrorContains(t, err, "retryable=false")
		require.Zero(t, stream.flushCalls)
	})

	t.Run("flush", func(t *testing.T) {
		stream := &fakeStream{flushErr: flushErr}
		plugin := validPlugin()
		plugin.stream = stream
		err := plugin.Write([]telegraf.Metric{testutil.TestMetric(1)})
		require.ErrorIs(t, err, flushErr)
		require.Equal(t, 1, stream.ingestCalls)
		require.Equal(t, 1, stream.flushCalls)
	})
}

func TestWriteEmptyBatchIsNoop(t *testing.T) {
	require.NoError(t, validPlugin().Write(nil))
}

func TestWriteRetriesFlushWithoutReadmitting(t *testing.T) {
	flushErr := errors.New("flush timed out")
	stream := &fakeStream{flushErrors: []error{flushErr, nil}}
	plugin := validPlugin()
	plugin.stream = stream
	input := []telegraf.Metric{testutil.TestMetric(1)}

	require.ErrorIs(t, plugin.Write(input), flushErr)
	require.NotNil(t, plugin.pending)
	require.Equal(t, 1, stream.ingestCalls)
	require.Equal(t, 1, stream.flushCalls)

	require.NoError(t, plugin.Write(input))
	require.Nil(t, plugin.pending)
	require.Equal(t, 1, stream.ingestCalls, "the admitted batch must not be admitted twice")
	require.Equal(t, 2, stream.flushCalls)
}

func TestWriteAdmitsOnlyNewSuffixOnAugmentedRetry(t *testing.T) {
	flushErr := errors.New("flush timed out")
	stream := &fakeStream{flushErrors: []error{flushErr, nil, nil}}
	plugin := validPlugin()
	plugin.stream = stream
	original := testutil.TestMetric(1)
	added := testutil.TestMetric(2)

	require.ErrorIs(t, plugin.Write([]telegraf.Metric{original}), flushErr)
	require.NoError(t, plugin.Write([]telegraf.Metric{original, added}))
	require.Equal(t, 2, stream.ingestCalls)
	require.Len(t, stream.batches, 2)

	expectedAdded, err := serializeMetrics([]telegraf.Metric{added})
	require.NoError(t, err)
	require.Equal(t, expectedAdded, stream.batches[1])
	require.Equal(t, 3, stream.flushCalls)
}

func TestWritePreservesCumulativeIdentityAcrossSuffixFailure(t *testing.T) {
	firstFlushErr := errors.New("first flush timed out")
	suffixFlushErr := errors.New("suffix flush timed out")
	stream := &fakeStream{
		flushErrors: []error{firstFlushErr, nil, suffixFlushErr, nil},
	}
	plugin := validPlugin()
	plugin.stream = stream
	original := testutil.TestMetric(1)
	added := testutil.TestMetric(2)
	augmented := []telegraf.Metric{original, added}

	require.ErrorIs(t, plugin.Write([]telegraf.Metric{original}), firstFlushErr)
	require.ErrorIs(t, plugin.Write(augmented), suffixFlushErr)
	require.NoError(t, plugin.Write(augmented))

	require.Equal(t, 2, stream.ingestCalls)
	require.Len(t, stream.batches, 2)
	expectedAdded, err := serializeMetrics([]telegraf.Metric{added})
	require.NoError(t, err)
	require.Equal(t, expectedAdded, stream.batches[1])
	require.Equal(t, 4, stream.flushCalls)
	require.Nil(t, plugin.pending)
}

func TestWritePreservesConfirmedPrefixAcrossSerializationFailure(t *testing.T) {
	flushErr := errors.New("flush timed out")
	stream := &fakeStream{flushErrors: []error{flushErr, nil, nil}}
	plugin := validPlugin()
	plugin.stream = stream
	original := testutil.TestMetric(1)
	unsupported := metricWithFields{
		Metric: testutil.TestMetric(2),
		fields: []*telegraf.Field{
			{Key: "unsupported", Value: []int{1}},
		},
	}
	added := testutil.TestMetric(3)

	require.ErrorIs(t, plugin.Write([]telegraf.Metric{original}), flushErr)
	err := plugin.Write([]telegraf.Metric{original, unsupported})
	require.ErrorContains(t, err, "unsupported field type")
	require.NotNil(t, plugin.confirmed)

	require.NoError(t, plugin.Write([]telegraf.Metric{original, added}))
	require.Equal(t, 2, stream.ingestCalls)
	expectedAdded, err := serializeMetrics([]telegraf.Metric{added})
	require.NoError(t, err)
	require.Equal(t, expectedAdded, stream.batches[1])
	require.Nil(t, plugin.confirmed)
}

func TestWriteResumesAfterPartialChunkAdmission(t *testing.T) {
	admissionErr := errors.New("temporary admission failure")
	stream := &fakeStream{ingestErrors: []error{nil, admissionErr, nil}}
	plugin := validPlugin()
	plugin.MaxBatchRecords = 1
	plugin.stream = stream
	input := []telegraf.Metric{testutil.TestMetric(1), testutil.TestMetric(2)}

	require.ErrorIs(t, plugin.Write(input), admissionErr)
	require.NotNil(t, plugin.pending)
	require.Equal(t, 2, stream.ingestCalls)
	require.Equal(t, 0, stream.flushCalls)

	require.NoError(t, plugin.Write(input))
	require.Nil(t, plugin.pending)
	require.Equal(t, 3, stream.ingestCalls)
	require.Equal(t, 2, stream.flushCalls)
}

func TestWriteRecreatesTerminalStream(t *testing.T) {
	closed := &fakeStream{closed: true}
	replacement := &fakeStream{}
	sdk := &fakeSDK{stream: replacement}
	plugin := validPlugin()
	plugin.sdk = sdk
	plugin.stream = closed

	require.NoError(t, plugin.Write([]telegraf.Metric{testutil.TestMetric(1)}))
	require.Equal(t, 1, closed.unackedCalls)
	require.Equal(t, 1, closed.closeCalls)
	require.Equal(t, 1, sdk.createCalls)
	require.Equal(t, 1, replacement.ingestCalls)
	require.Equal(t, 1, replacement.flushCalls)
	require.Same(t, replacement, plugin.stream)
}

func TestWriteRetriesFailedStreamRecreation(t *testing.T) {
	recreateErr := errors.New("stream recreation failed")
	closed := &fakeStream{closed: true}
	replacement := &fakeStream{}
	sdk := &fakeSDK{
		streams:      []ingestStream{replacement, replacement},
		createErrors: []error{recreateErr, nil},
	}
	plugin := validPlugin()
	plugin.sdk = sdk
	plugin.stream = closed
	input := []telegraf.Metric{testutil.TestMetric(1)}

	require.ErrorIs(t, plugin.Write(input), recreateErr)
	require.Nil(t, plugin.stream)
	require.NotNil(t, plugin.pending)

	require.NoError(t, plugin.Write(input))
	require.Same(t, replacement, plugin.stream)
	require.Equal(t, 2, sdk.createCalls)
	require.Equal(t, 1, replacement.ingestCalls)
	require.Nil(t, plugin.pending)
}

func TestWriteReplaysOnlyUnacknowledgedRecordsAfterTerminalFailure(t *testing.T) {
	flushErr := errors.New("stream failed")
	closed := &fakeStream{flushErrors: []error{flushErr}}
	replacement := &fakeStream{}
	sdk := &fakeSDK{stream: replacement}
	plugin := validPlugin()
	plugin.sdk = sdk
	plugin.stream = closed
	input := []telegraf.Metric{testutil.TestMetric(1)}

	require.ErrorIs(t, plugin.Write(input), flushErr)
	require.Len(t, closed.batches, 1)
	closed.unacked = closed.batches
	closed.closed = true

	require.NoError(t, plugin.Write(input))
	require.Equal(t, 1, closed.ingestCalls)
	require.Equal(t, 1, replacement.ingestCalls)
	require.Equal(t, closed.batches[0], replacement.batches[0])
	require.Equal(t, []bool{true}, replacement.encoded)
	require.Nil(t, plugin.pending)
}

func TestWriteUnityCatalogSchemaReencodesRecordsAfterTerminalFailure(t *testing.T) {
	flushErr := errors.New("stream failed")
	closed := &fakeStream{flushErrors: []error{flushErr}}
	replacement := &fakeStream{}
	sdk := &fakeSDK{stream: replacement}
	plugin := validPlugin()
	plugin.SchemaMode = schemaModeUnityCatalog
	plugin.sdk = sdk
	plugin.stream = closed
	input := []telegraf.Metric{testutil.TestMetric(1)}

	require.ErrorIs(t, plugin.Write(input), flushErr)
	require.Equal(t, []bool{false}, closed.encoded)
	closed.unacked = [][][]byte{{{0x08, 0x01}}}
	closed.closed = true

	require.NoError(t, plugin.Write(input))
	require.Equal(t, 1, sdk.unityCatalogCalls)
	require.Equal(t, closed.batches[0], replacement.batches[0])
	require.Equal(t, []bool{false}, replacement.encoded)
	require.Nil(t, plugin.pending)
}

func TestWriteUnityCatalogSchemaReplaysOnlyCurrentSuffix(t *testing.T) {
	firstFlushErr := errors.New("first flush failed")
	suffixFlushErr := errors.New("suffix flush failed")
	closed := &fakeStream{flushErrors: []error{firstFlushErr, nil, suffixFlushErr}}
	replacement := &fakeStream{}
	sdk := &fakeSDK{stream: replacement}
	plugin := validPlugin()
	plugin.SchemaMode = schemaModeUnityCatalog
	plugin.sdk = sdk
	plugin.stream = closed
	original := testutil.TestMetric(1)
	added := testutil.TestMetric(2)

	require.ErrorIs(t, plugin.Write([]telegraf.Metric{original}), firstFlushErr)
	require.ErrorIs(t, plugin.Write([]telegraf.Metric{original, added}), suffixFlushErr)
	closed.unacked = [][][]byte{{{0x08, 0x01}}}
	closed.closed = true

	require.NoError(t, plugin.Write([]telegraf.Metric{original, added}))
	expectedAdded, err := serializeUnityCatalogMetrics(
		[]telegraf.Metric{added},
		plugin.TimestampColumn,
		plugin.MeasurementColumn,
	)
	require.NoError(t, err)
	require.Equal(t, expectedAdded, replacement.batches[0])
	require.Equal(t, []bool{false}, replacement.encoded)
}

func TestWriteRejectsIndividuallyOversizedMetricBeforeAdmission(t *testing.T) {
	stream := &fakeStream{}
	plugin := validPlugin()
	plugin.MaxPayloadBytes = batchEnvelopeReserve + 1
	plugin.stream = stream

	err := plugin.Write([]telegraf.Metric{testutil.TestMetric(1)})
	require.ErrorContains(t, err, "exceeding the payload budget")
	require.Zero(t, stream.ingestCalls)
}

func TestWriteSplitsBatchByPayloadSize(t *testing.T) {
	input := []telegraf.Metric{testutil.TestMetric(1), testutil.TestMetric(2)}
	records, err := serializeMetrics(input)
	require.NoError(t, err)

	stream := &fakeStream{}
	plugin := validPlugin()
	plugin.MaxPayloadBytes = config.Size(
		batchEnvelopeReserve + protowire.SizeTag(1) + protowire.SizeBytes(len(records[0])),
	)
	plugin.stream = stream

	require.NoError(t, plugin.Write(input))
	require.Equal(t, 2, stream.ingestCalls)
	require.Len(t, stream.batches, 2)
	require.Len(t, stream.batches[0], 1)
	require.Len(t, stream.batches[1], 1)
	require.Equal(t, 1, stream.flushCalls)
}

func TestCloseIsIdempotentAndJoinsErrors(t *testing.T) {
	streamErr := errors.New("stream close failed")
	sdkErr := errors.New("SDK close failed")
	stream := &fakeStream{closeErr: streamErr}
	sdk := &fakeSDK{closeErr: sdkErr}
	plugin := validPlugin()
	plugin.stream = stream
	plugin.sdk = sdk

	err := plugin.Close()
	require.ErrorIs(t, err, streamErr)
	require.ErrorIs(t, err, sdkErr)
	require.Equal(t, 1, stream.closeCalls)
	require.Equal(t, 1, sdk.closeCalls)

	require.NoError(t, plugin.Close())
	require.Equal(t, 1, stream.closeCalls)
	require.Equal(t, 1, sdk.closeCalls)
}

func validPlugin() *Zerobus {
	return &Zerobus{
		ServerEndpoint:  "https://workspace.zerobus.example.com",
		WorkspaceURL:    "https://workspace.example.com",
		TableName:       "catalog.schema.metrics",
		ClientID:        "client",
		ClientSecret:    config.NewSecret([]byte("secret")),
		ApplicationName: "telegraf",
		SchemaMode:      schemaModeCanonical,
		TimestampColumn: "timestamp",
		MaxBatchRecords: defaultMaxBatchRecords,
		Log:             testutil.Logger{},
	}
}

type fakeStream struct {
	records      [][]byte
	batches      [][][]byte
	unacked      [][][]byte
	ingestErr    error
	ingestErrors []error
	flushErr     error
	flushErrors  []error
	unackedErr   error
	closeErr     error
	closed       bool
	encoded      []bool
	ingestCalls  int
	flushCalls   int
	unackedCalls int
	closeCalls   int
}

func (s *fakeStream) IngestRecordsOffset(records [][]byte, encoded bool) (int64, error) {
	s.ingestCalls++
	s.records = records
	s.batches = append(s.batches, records)
	s.encoded = append(s.encoded, encoded)
	if len(s.ingestErrors) > 0 {
		err := s.ingestErrors[0]
		s.ingestErrors = s.ingestErrors[1:]
		return int64(s.ingestCalls), err
	}
	return int64(s.ingestCalls), s.ingestErr
}

func (s *fakeStream) Flush() error {
	s.flushCalls++
	if len(s.flushErrors) > 0 {
		err := s.flushErrors[0]
		s.flushErrors = s.flushErrors[1:]
		return err
	}
	return s.flushErr
}

func (s *fakeStream) GetUnackedBatches() ([][][]byte, error) {
	s.unackedCalls++
	return s.unacked, s.unackedErr
}

func (s *fakeStream) IsClosed() bool {
	return s.closed
}

func (s *fakeStream) Close() error {
	s.closeCalls++
	s.closed = true
	return s.closeErr
}

type fakeSDK struct {
	stream            ingestStream
	streams           []ingestStream
	createErr         error
	createErrors      []error
	closeErr          error
	tableName         string
	clientID          string
	clientSecret      string
	options           []sdkzerobus.StreamOption
	createCalls       int
	unityCatalogCalls int
	closeCalls        int
}

func (s *fakeSDK) CreateStream(
	_ context.Context,
	tableName, clientID, clientSecret string,
	options ...sdkzerobus.StreamOption,
) (ingestStream, error) {
	s.createCalls++
	s.tableName = tableName
	s.clientID = clientID
	s.clientSecret = clientSecret
	s.options = options
	err := s.createErr
	if len(s.createErrors) > 0 {
		err = s.createErrors[0]
		s.createErrors = s.createErrors[1:]
	}
	if len(s.streams) > 0 {
		stream := s.streams[0]
		s.streams = s.streams[1:]
		return stream, err
	}
	return s.stream, err
}

func (s *fakeSDK) CreateUnityCatalogStream(
	_ context.Context,
	tableName, clientID, clientSecret string,
	options ...sdkzerobus.StreamOption,
) (ingestStream, error) {
	s.unityCatalogCalls++
	s.tableName = tableName
	s.clientID = clientID
	s.clientSecret = clientSecret
	s.options = options
	err := s.createErr
	if len(s.createErrors) > 0 {
		err = s.createErrors[0]
		s.createErrors = s.createErrors[1:]
	}
	if len(s.streams) > 0 {
		stream := s.streams[0]
		s.streams = s.streams[1:]
		return stream, err
	}
	return s.stream, err
}

func (s *fakeSDK) Close() error {
	s.closeCalls++
	return s.closeErr
}

type metricWithFields struct {
	telegraf.Metric
	fields []*telegraf.Field
}

func (m metricWithFields) FieldList() []*telegraf.Field {
	return m.fields
}
