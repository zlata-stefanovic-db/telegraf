//go:generate ../../../tools/readme_config_includer/generator
//go:generate protoc --go_out=. --go_opt=paths=source_relative metric.proto
package zerobus

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	sdkzerobus "github.com/databricks/zerobus-sdk/purego/zerobus"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

const (
	defaultMaxBatchRecords = 100_000
	defaultMaxPayloadBytes = 10*1024*1024 - 64*1024
	batchEnvelopeReserve   = 1024
	schemaModeCanonical    = "canonical"
	schemaModeUnityCatalog = "unity_catalog"
)

type Zerobus struct {
	ServerEndpoint  string        `toml:"zerobus_server_endpoint"`
	WorkspaceURL    string        `toml:"workspace_url"`
	TableName       string        `toml:"table_name"`
	ClientID        string        `toml:"client_id"`
	ClientSecret    config.Secret `toml:"client_secret"`
	ApplicationName string        `toml:"application_name"`
	SchemaMode      string        `toml:"schema_mode"`

	TimestampColumn    string          `toml:"timestamp_column"`
	MeasurementColumn  string          `toml:"measurement_column"`
	SchemaFetchTimeout config.Duration `toml:"schema_fetch_timeout"`
	SchemaCacheTTL     config.Duration `toml:"schema_cache_ttl"`

	MaxInflight             int             `toml:"max_inflight"`
	MaxBufferedPayloadBytes config.Size     `toml:"max_buffered_payload_bytes"`
	MaxBatchRecords         int             `toml:"max_batch_records"`
	MaxPayloadBytes         config.Size     `toml:"max_payload_bytes"`
	RecoveryRetries         int             `toml:"recovery_retries"`
	RecoveryTimeout         config.Duration `toml:"recovery_timeout"`
	RecoveryBackoff         config.Duration `toml:"recovery_backoff"`
	LackOfAckTimeout        config.Duration `toml:"lack_of_ack_timeout"`
	FlushTimeout            config.Duration `toml:"flush_timeout"`

	Log telegraf.Logger `toml:"-"`

	sdk       sdkClient
	stream    ingestStream
	newSDK    sdkFactory
	pending   *pendingWrite
	confirmed [][]byte
}

type ingestStream interface {
	IngestRecordsOffset(records [][]byte, encoded bool) (int64, error)
	Flush() error
	GetUnackedBatches() ([][][]byte, error)
	IsClosed() bool
	Close() error
}

type pendingWrite struct {
	original  [][]byte
	replay    [][]byte
	remaining []recordBatch
	waiting   bool
}

type recordBatch struct {
	records [][]byte
	encoded bool
}

type sdkClient interface {
	CreateStream(
		ctx context.Context,
		tableName, clientID, clientSecret string,
		opts ...sdkzerobus.StreamOption,
	) (ingestStream, error)
	CreateUnityCatalogStream(
		ctx context.Context,
		tableName, clientID, clientSecret string,
		opts ...sdkzerobus.StreamOption,
	) (ingestStream, error)
	Close() error
}

type sdkFactory func(
	serverEndpoint, workspaceURL string,
	opts ...sdkzerobus.Option,
) (sdkClient, error)

type sdkAdapter struct {
	*sdkzerobus.SDK
}

func (s *sdkAdapter) CreateStream(
	ctx context.Context,
	tableName, clientID, clientSecret string,
	opts ...sdkzerobus.StreamOption,
) (ingestStream, error) {
	stream, err := s.SDK.CreateStream(ctx, tableName, clientID, clientSecret, opts...)
	if err != nil {
		return nil, err
	}
	return &staticStreamAdapter{Stream: stream}, nil
}

func (s *sdkAdapter) CreateUnityCatalogStream(
	ctx context.Context,
	tableName, clientID, clientSecret string,
	opts ...sdkzerobus.StreamOption,
) (ingestStream, error) {
	stream, err := s.SDK.CreateDynamicProtoStream(ctx, tableName, clientID, clientSecret, opts...)
	if err != nil {
		return nil, err
	}
	return &unityCatalogStreamAdapter{DynamicProtoStream: stream}, nil
}

type staticStreamAdapter struct {
	*sdkzerobus.Stream
}

func (s *staticStreamAdapter) IngestRecordsOffset(records [][]byte, _ bool) (int64, error) {
	return s.Stream.IngestRecordsOffset(records)
}

type unityCatalogStreamAdapter struct {
	*sdkzerobus.DynamicProtoStream
}

func (s *unityCatalogStreamAdapter) IngestRecordsOffset(records [][]byte, encoded bool) (int64, error) {
	if encoded {
		return s.DynamicProtoStream.Stream.IngestRecordsOffset(records)
	}
	return s.DynamicProtoStream.IngestJSONRecordsOffset(records)
}

func (*Zerobus) SampleConfig() string {
	return sampleConfig
}

func (z *Zerobus) Init() error {
	z.SchemaMode = strings.ToLower(strings.TrimSpace(z.SchemaMode))
	z.TimestampColumn = strings.TrimSpace(z.TimestampColumn)
	z.MeasurementColumn = strings.TrimSpace(z.MeasurementColumn)
	if z.SchemaMode == "" {
		z.SchemaMode = schemaModeCanonical
	}
	if z.SchemaMode != schemaModeCanonical && z.SchemaMode != schemaModeUnityCatalog {
		return fmt.Errorf(
			`option "schema_mode" must be %q or %q`,
			schemaModeCanonical,
			schemaModeUnityCatalog,
		)
	}
	if z.SchemaMode == schemaModeUnityCatalog && strings.TrimSpace(z.TimestampColumn) == "" {
		return errors.New(`option "timestamp_column" must be set in unity_catalog schema mode`)
	}
	if z.SchemaMode == schemaModeUnityCatalog &&
		z.MeasurementColumn != "" &&
		z.MeasurementColumn == z.TimestampColumn {
		return errors.New(`options "measurement_column" and "timestamp_column" must be different`)
	}

	requiredStrings := []struct {
		name  string
		value string
	}{
		{"zerobus_server_endpoint", z.ServerEndpoint},
		{"workspace_url", z.WorkspaceURL},
		{"table_name", z.TableName},
		{"client_id", z.ClientID},
	}
	for _, option := range requiredStrings {
		if strings.TrimSpace(option.value) == "" {
			return fmt.Errorf("option %q must be set", option.name)
		}
	}
	if z.ClientSecret.Empty() {
		return errors.New(`option "client_secret" must be set`)
	}

	if z.MaxInflight < 0 {
		return errors.New(`option "max_inflight" cannot be negative`)
	}
	if z.MaxBufferedPayloadBytes < 0 {
		return errors.New(`option "max_buffered_payload_bytes" cannot be negative`)
	}
	if z.MaxBatchRecords <= 0 {
		return errors.New(`option "max_batch_records" must be greater than zero`)
	}
	if z.MaxPayloadBytes < 0 {
		return errors.New(`option "max_payload_bytes" cannot be negative`)
	}
	if z.MaxPayloadBytes > 0 && z.MaxPayloadBytes <= batchEnvelopeReserve {
		return fmt.Errorf(
			`option "max_payload_bytes" must exceed %d bytes`,
			batchEnvelopeReserve,
		)
	}
	if z.RecoveryRetries < 0 {
		return errors.New(`option "recovery_retries" cannot be negative`)
	}
	if z.SchemaFetchTimeout < 0 {
		return errors.New(`option "schema_fetch_timeout" cannot be negative`)
	}
	for _, option := range []struct {
		name  string
		value config.Duration
	}{
		{"recovery_timeout", z.RecoveryTimeout},
		{"recovery_backoff", z.RecoveryBackoff},
		{"lack_of_ack_timeout", z.LackOfAckTimeout},
		{"flush_timeout", z.FlushTimeout},
	} {
		if option.value < 0 {
			return fmt.Errorf("option %q cannot be negative", option.name)
		}
	}

	if z.newSDK == nil {
		z.newSDK = func(
			serverEndpoint, workspaceURL string,
			opts ...sdkzerobus.Option,
		) (sdkClient, error) {
			sdk, err := sdkzerobus.New(serverEndpoint, workspaceURL, opts...)
			if err != nil {
				return nil, err
			}
			return &sdkAdapter{SDK: sdk}, nil
		}
	}
	return nil
}

func (z *Zerobus) Connect() error {
	secret, err := z.ClientSecret.Get()
	if err != nil {
		return fmt.Errorf("resolving client secret failed: %w", err)
	}
	defer secret.Destroy()

	sdkOptions := make([]sdkzerobus.Option, 0, 1)
	if z.ApplicationName != "" {
		sdkOptions = append(sdkOptions, sdkzerobus.WithApplicationName(z.ApplicationName))
	}
	if z.SchemaMode == schemaModeUnityCatalog {
		if z.SchemaFetchTimeout > 0 {
			sdkOptions = append(
				sdkOptions,
				sdkzerobus.WithDynamicSchemaFetchTimeout(time.Duration(z.SchemaFetchTimeout)),
			)
		}
		if z.SchemaCacheTTL != 0 {
			sdkOptions = append(
				sdkOptions,
				sdkzerobus.WithDynamicSchemaCacheTTL(time.Duration(z.SchemaCacheTTL)),
			)
		}
	}
	sdk, err := z.newSDK(z.ServerEndpoint, z.WorkspaceURL, sdkOptions...)
	if err != nil {
		return fmt.Errorf("creating Zerobus SDK failed: %w", err)
	}

	z.sdk = sdk
	if err := z.openStream(secret.String()); err != nil {
		z.sdk = nil
		return errors.Join(fmt.Errorf("creating Zerobus stream failed: %w", err), sdk.Close())
	}

	return nil
}

func (z *Zerobus) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}
	if z.stream == nil && (z.sdk == nil || z.pending == nil) {
		return internal.ErrNotConnected
	}

	if z.pending != nil {
		pendingOriginal := z.pending.original
		if err := z.processPending(); err != nil {
			return err
		}
		z.confirmed = pendingOriginal
	}

	records, err := z.serializeMetrics(metrics)
	if err != nil {
		return err
	}
	original := records
	if z.confirmed != nil {
		if recordsHavePrefix(records, z.confirmed) {
			records = records[len(z.confirmed):]
			if len(records) == 0 {
				z.confirmed = nil
				return nil
			}
		} else {
			z.confirmed = nil
		}
	}

	chunks, err := z.chunkRecords(records)
	if err != nil {
		return err
	}
	z.pending = &pendingWrite{
		original:  original,
		replay:    records,
		remaining: chunks,
	}
	z.confirmed = nil
	return z.processPending()
}

func (z *Zerobus) Close() error {
	var streamErr, sdkErr error
	if z.stream != nil {
		streamErr = z.stream.Close()
		z.stream = nil
	}
	z.pending = nil
	z.confirmed = nil
	if z.sdk != nil {
		sdkErr = z.sdk.Close()
		z.sdk = nil
	}
	return errors.Join(streamErr, sdkErr)
}

func (z *Zerobus) openStream(clientSecret string) error {
	options := z.streamOptions()
	var (
		stream ingestStream
		err    error
	)
	if z.SchemaMode == schemaModeUnityCatalog {
		stream, err = z.sdk.CreateUnityCatalogStream(
			context.Background(),
			z.TableName,
			z.ClientID,
			clientSecret,
			options...,
		)
	} else {
		descriptor, descriptorErr := messageDescriptor()
		if descriptorErr != nil {
			return fmt.Errorf("building protobuf descriptor failed: %w", descriptorErr)
		}
		options = append([]sdkzerobus.StreamOption{sdkzerobus.WithProto(descriptor)}, options...)
		stream, err = z.sdk.CreateStream(
			context.Background(),
			z.TableName,
			z.ClientID,
			clientSecret,
			options...,
		)
	}
	if err != nil {
		return err
	}
	z.stream = stream
	return nil
}

func (z *Zerobus) recreateStream() error {
	unacked, err := z.stream.GetUnackedBatches()
	if err != nil {
		return z.writeError("retrieving unacknowledged batches", err)
	}
	_ = z.stream.Close()
	z.stream = nil
	if z.SchemaMode == schemaModeUnityCatalog {
		if len(unacked) > 0 {
			replay, chunkErr := z.chunkRecords(z.pending.replay)
			if chunkErr != nil {
				return fmt.Errorf("rebuilding Unity Catalog replay batches failed: %w", chunkErr)
			}
			z.pending.remaining = replay
		}
	} else {
		replay := make([]recordBatch, 0, len(unacked)+len(z.pending.remaining))
		for _, batch := range unacked {
			replay = append(replay, recordBatch{records: batch, encoded: true})
		}
		z.pending.remaining = append(replay, z.pending.remaining...)
	}
	z.pending.waiting = false

	return z.openStreamFromSecret()
}

func (z *Zerobus) openStreamFromSecret() error {
	secret, err := z.ClientSecret.Get()
	if err != nil {
		return fmt.Errorf("resolving client secret for stream recovery failed: %w", err)
	}
	defer secret.Destroy()
	if err := z.openStream(secret.String()); err != nil {
		return z.writeError("recreating stream", err)
	}
	return nil
}

func (z *Zerobus) processPending() error {
	if z.stream == nil {
		if err := z.openStreamFromSecret(); err != nil {
			return err
		}
	} else if z.stream.IsClosed() {
		if err := z.recreateStream(); err != nil {
			return err
		}
	}

	if z.pending.waiting {
		if err := z.stream.Flush(); err != nil {
			return z.writeError("flushing previously admitted batch", err)
		}
		z.pending.waiting = false
	}

	for len(z.pending.remaining) > 0 {
		chunk := z.pending.remaining[0]
		if _, err := z.stream.IngestRecordsOffset(chunk.records, chunk.encoded); err != nil {
			return z.writeError("admitting batch", err)
		}
		z.pending.remaining = z.pending.remaining[1:]
		z.pending.waiting = true
	}

	if z.pending.waiting {
		if err := z.stream.Flush(); err != nil {
			return z.writeError("flushing batch", err)
		}
	}
	z.pending = nil
	return nil
}

func (z *Zerobus) chunkRecords(records [][]byte) ([]recordBatch, error) {
	maxBytes := int(z.MaxPayloadBytes)
	if maxBytes <= 0 {
		maxBytes = defaultMaxPayloadBytes
	}
	payloadBudget := maxBytes - batchEnvelopeReserve
	if payloadBudget <= 0 {
		return nil, fmt.Errorf(
			"max_payload_bytes=%d is too small; it must exceed %d bytes",
			maxBytes,
			batchEnvelopeReserve,
		)
	}

	chunks := make([]recordBatch, 0, (len(records)+z.MaxBatchRecords-1)/z.MaxBatchRecords)
	for len(records) > 0 {
		count, size := 0, 0
		for count < len(records) && count < z.MaxBatchRecords {
			recordSize := protowire.SizeTag(1) + protowire.SizeBytes(len(records[count]))
			if recordSize > payloadBudget {
				return nil, fmt.Errorf(
					"serialized metric %d requires %d bytes, exceeding the payload budget of %d bytes",
					count,
					recordSize,
					payloadBudget,
				)
			}
			if size+recordSize > payloadBudget {
				break
			}
			size += recordSize
			count++
		}
		chunks = append(chunks, recordBatch{records: records[:count]})
		records = records[count:]
	}
	return chunks, nil
}

func serializeMetrics(metrics []telegraf.Metric) ([][]byte, error) {
	records := make([][]byte, 0, len(metrics))
	marshaller := proto.MarshalOptions{Deterministic: true}
	for _, metric := range metrics {
		record, err := metricToProto(metric)
		if err != nil {
			return nil, fmt.Errorf("serializing metric failed: %w", err)
		}
		serialized, err := marshaller.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("marshaling protobuf record failed: %w", err)
		}
		records = append(records, serialized)
	}
	return records, nil
}

func (z *Zerobus) serializeMetrics(metrics []telegraf.Metric) ([][]byte, error) {
	if z.SchemaMode == schemaModeUnityCatalog {
		return serializeUnityCatalogMetrics(metrics, z.TimestampColumn, z.MeasurementColumn)
	}
	return serializeMetrics(metrics)
}

func recordsHavePrefix(records, prefix [][]byte) bool {
	return len(records) >= len(prefix) &&
		slices.EqualFunc(records[:len(prefix)], prefix, slices.Equal)
}

func (z *Zerobus) streamOptions() []sdkzerobus.StreamOption {
	options := make([]sdkzerobus.StreamOption, 0, 9)
	if z.MaxInflight > 0 {
		options = append(options, sdkzerobus.WithMaxInflight(z.MaxInflight))
	}
	if z.MaxBufferedPayloadBytes > 0 {
		options = append(options, sdkzerobus.WithMaxBufferedPayloadBytes(int64(z.MaxBufferedPayloadBytes)))
	}
	if z.MaxBatchRecords > 0 {
		options = append(options, sdkzerobus.WithMaxBatchRecords(z.MaxBatchRecords))
	}
	if z.MaxPayloadBytes > 0 {
		options = append(options, sdkzerobus.WithMaxPayloadBytes(int(z.MaxPayloadBytes)))
	}
	if z.RecoveryRetries > 0 {
		options = append(options, sdkzerobus.WithRecoveryRetries(z.RecoveryRetries))
	}
	if z.RecoveryTimeout > 0 {
		options = append(options, sdkzerobus.WithRecoveryTimeout(time.Duration(z.RecoveryTimeout)))
	}
	if z.RecoveryBackoff > 0 {
		options = append(options, sdkzerobus.WithRecoveryBackoff(time.Duration(z.RecoveryBackoff)))
	}
	if z.LackOfAckTimeout > 0 {
		options = append(options, sdkzerobus.WithLackOfAckTimeout(time.Duration(z.LackOfAckTimeout)))
	}
	if z.FlushTimeout > 0 {
		options = append(options, sdkzerobus.WithFlushTimeout(time.Duration(z.FlushTimeout)))
	}
	return options
}

func (z *Zerobus) writeError(operation string, err error) error {
	retryable := sdkzerobus.Retryable(err)
	if !retryable {
		z.Log.Errorf("Zerobus %s failed with a non-retryable error: %v", operation, err)
	}
	return fmt.Errorf("Zerobus %s failed (retryable=%t): %w", operation, retryable, err)
}

func messageDescriptor() ([]byte, error) {
	descriptor := protodesc.ToDescriptorProto((&TelegrafMetric{}).ProtoReflect().Descriptor())
	return proto.Marshal(descriptor)
}

func init() {
	outputs.Add("zerobus", func() telegraf.Output {
		return &Zerobus{
			ApplicationName: "telegraf",
			SchemaMode:      schemaModeCanonical,
			TimestampColumn: "timestamp",
			MaxBatchRecords: defaultMaxBatchRecords,
		}
	})
}
