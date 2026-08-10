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
	"sync"
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

// Default values.
const (
	defaultMaxBatchRecords  = 100_000
	defaultMaxPayloadBytes  = 10*1024*1024 - 64*1024
	defaultConnectTimeout   = 30 * time.Second
	batchEnvelopeReserve    = 1024
	bufferedRequestOverhead = 512
	bufferedRecordOverhead  = 32
	maxConcurrentStreams    = 100
	schemaModeStatic        = "static"
	schemaModeTableSchema   = "table_schema"
)

// Zerobus output plugin configuration.
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
	ConnectTimeout     config.Duration `toml:"connect_timeout"`

	ConcurrentStreams       int             `toml:"concurrent_streams"`
	MaxInflight             int             `toml:"max_inflight"`
	MaxBufferedPayloadBytes config.Size     `toml:"max_buffered_payload_bytes"`
	MaxBatchRecords         int             `toml:"max_batch_records"`
	MaxPayloadBytes         config.Size     `toml:"max_payload_bytes"`
	RecoveryRetries         int             `toml:"recovery_retries"`
	RecoveryTimeout         config.Duration `toml:"recovery_timeout"`
	RecoveryBackoff         config.Duration `toml:"recovery_backoff"`
	LackOfAckTimeout        config.Duration `toml:"lack_of_ack_timeout"`
	FlushTimeout            config.Duration `toml:"flush_timeout"`

	sdk       sdkClient
	writers   []*writer
	newSDK    sdkFactory
	original  [][]byte
	confirmed [][]byte

	descriptorMu     sync.Mutex
	descriptor       []byte
	descriptorReused bool
}

// One Zerobus stream together with the write state that survives a failed
// flush. Writers are independent, so they can be flushed concurrently.
type writer struct {
	stream  ingestStream
	pending *pendingWrite
}

// Interface for the ingest stream.
type ingestStream interface {
	IngestRecordsOffset(records [][]byte, encoded bool) (int64, error)
	Flush() error
	GetUnackedBatches() ([][][]byte, error)
	IsClosed() bool
	Close() error
}

// Struct for the pending write.
type pendingWrite struct {
	admitted  []recordBatch
	remaining []recordBatch
	waiting   bool
}

// Struct for the record batch.
type recordBatch struct {
	records [][]byte
	encoded bool
}

// Struct for the prepared write.
type preparedWrite struct {
	records      [][]byte
	accept       []int
	reject       []int
	rejectErrors []error
}

// Interface for the SDK client.
type sdkClient interface {
	CreateStaticSchemaStream(
		ctx context.Context,
		tableName, clientID, clientSecret string,
		opts ...sdkzerobus.StreamOption,
	) (ingestStream, error)
	CreateTableSchemaStream(
		ctx context.Context,
		tableName, clientID, clientSecret string,
		opts ...sdkzerobus.StreamOption,
	) (ingestStream, error)
	FetchProtoDescriptor(
		ctx context.Context,
		tableName, clientID, clientSecret string,
	) ([]byte, error)
	Close() error
}

// Factory function for the SDK client.
type sdkFactory func(
	serverEndpoint, workspaceURL string,
	opts ...sdkzerobus.Option,
) (sdkClient, error)

// Adapter for the SDK client.
type sdkAdapter struct {
	*sdkzerobus.SDK
}

// Create a stream with the static schema descriptor.
func (s *sdkAdapter) CreateStaticSchemaStream(
	ctx context.Context,
	tableName, clientID, clientSecret string,
	opts ...sdkzerobus.StreamOption,
) (ingestStream, error) {
	// Create a stream.
	stream, err := s.SDK.CreateStream(ctx, tableName, clientID, clientSecret, opts...)
	if err != nil {
		return nil, err
	}
	return &staticStreamAdapter{Stream: stream}, nil
}

// Create a stream with the table schema descriptor.
func (s *sdkAdapter) CreateTableSchemaStream(
	ctx context.Context,
	tableName, clientID, clientSecret string,
	opts ...sdkzerobus.StreamOption,
) (ingestStream, error) {
	// Create a stream.
	stream, err := s.SDK.CreateStream(ctx, tableName, clientID, clientSecret, opts...)
	if err != nil {
		return nil, err
	}
	return &tableSchemaStreamAdapter{Stream: stream}, nil
}

// Fetch the table's protobuf descriptor from Unity Catalog.
func (s *sdkAdapter) FetchProtoDescriptor(
	ctx context.Context,
	tableName, clientID, clientSecret string,
) ([]byte, error) {
	return s.SDK.FetchProtoDescriptorFromUC(ctx, tableName, clientID, clientSecret)
}

// Adapter for the static schema stream.
type staticStreamAdapter struct {
	*sdkzerobus.Stream
}

// Wrap the IngestRecordsOffset method for static schema mode.
func (s *staticStreamAdapter) IngestRecordsOffset(records [][]byte, _ bool) (int64, error) {
	return s.Stream.IngestRecordsOffset(records)
}

// Adapter for the table schema stream.
type tableSchemaStreamAdapter struct {
	*sdkzerobus.Stream
}

// Wrap the IngestRecordsOffset method for table schema mode.
func (s *tableSchemaStreamAdapter) IngestRecordsOffset(
	records [][]byte,
	encoded bool,
) (int64, error) {
	if encoded {
		return s.Stream.IngestRecordsOffset(records)
	}
	return s.Stream.IngestJSONRecordsOffset(records)
}

// Return the sample configuration to fit the plugin interface.
func (*Zerobus) SampleConfig() string {
	return sampleConfig
}

// Initialize the Zerobus output plugin.
func (z *Zerobus) Init() error {
	// Convert the schema mode to lowercase and trim whitespaces.
	z.SchemaMode = strings.ToLower(strings.TrimSpace(z.SchemaMode))
	z.TimestampColumn = strings.TrimSpace(z.TimestampColumn)
	z.MeasurementColumn = strings.TrimSpace(z.MeasurementColumn)
	// Use static schema mode by default.
	if z.SchemaMode == "" {
		z.SchemaMode = schemaModeStatic
	}
	// Validate the schema mode.
	if z.SchemaMode != schemaModeStatic && z.SchemaMode != schemaModeTableSchema {
		return fmt.Errorf(
			`option "schema_mode" must be %q or %q`,
			schemaModeStatic,
			schemaModeTableSchema,
		)
	}
	// Validate the measurement and timestamp columns in table schema mode.
	if z.SchemaMode == schemaModeTableSchema &&
		z.MeasurementColumn != "" &&
		z.MeasurementColumn == z.TimestampColumn {
		return errors.New(`options "measurement_column" and "timestamp_column" must be different`)
	}
	// Validate the required configurations.
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

	if z.ConcurrentStreams < 0 {
		return errors.New(`option "concurrent_streams" cannot be negative`)
	}
	if z.ConcurrentStreams > maxConcurrentStreams {
		return fmt.Errorf(
			`option "concurrent_streams" must not exceed %d`,
			maxConcurrentStreams,
		)
	}
	if z.ConcurrentStreams == 0 {
		z.ConcurrentStreams = 1
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
	if z.ConnectTimeout < 0 {
		return errors.New(`option "connect_timeout" cannot be negative`)
	}
	if z.ConnectTimeout == 0 {
		z.ConnectTimeout = config.Duration(defaultConnectTimeout)
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

	// Create a new SDK if one is not already set.
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

// Connect to the Zerobus server.
func (z *Zerobus) Connect() error {
	secret, err := z.ClientSecret.Get()
	if err != nil {
		return fmt.Errorf("resolving client secret failed: %w", err)
	}
	defer secret.Destroy()

	applicationName := internal.ProductToken()
	if name := strings.TrimSpace(z.ApplicationName); name != "" {
		applicationName += " " + name
	}
	sdkOptions := []sdkzerobus.Option{sdkzerobus.WithApplicationName(applicationName)}
	if z.SchemaMode == schemaModeTableSchema {
		if z.SchemaFetchTimeout > 0 {
			sdkOptions = append(
				sdkOptions,
				sdkzerobus.WithProtoDescriptorFetchTimeout(time.Duration(z.SchemaFetchTimeout)),
			)
		}
	}
	// Create a new SDK client.
	sdk, err := z.newSDK(z.ServerEndpoint, z.WorkspaceURL, sdkOptions...)
	if err != nil {
		return fmt.Errorf("creating Zerobus SDK failed: %w", err)
	}
	z.sdk = sdk

	// Create a context with a timeout for the connection.
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(z.ConnectTimeout))
	defer cancel()
	// Open one stream per configured writer. The first stream fetches the table
	// schema descriptor and the rest reuse it.
	writers := make([]*writer, 0, z.ConcurrentStreams)
	for range z.ConcurrentStreams {
		w := &writer{}
		if err := z.openStream(ctx, w, secret.String()); err != nil {
			// If the stream creation fails, close everything opened so far.
			z.writers = writers
			closeErr := z.Close()
			startupErr := &internal.StartupError{
				Err:   fmt.Errorf("creating Zerobus stream failed: %w", err),
				Retry: sdkzerobus.Retryable(err),
			}
			return errors.Join(startupErr, closeErr)
		}
		writers = append(writers, w)
	}
	z.writers = writers

	return nil
}

// Write the metrics to the Zerobus server.
func (z *Zerobus) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}
	if !z.connected() {
		return internal.ErrNotConnected
	}

	// If there are pending metrics, try to ingest them again.
	if z.hasPending() {
		pendingOriginal := z.original
		// If the pending metrics cannot be ingested, return an error.
		if err := z.flushWriters(); err != nil {
			return err
		}
		// If the pending metrics were ingested successfully, clear the confirmed metrics.
		z.confirmed = pendingOriginal
		z.original = nil
	}

	// Prepare the metrics for the Zerobus server.
	prepared := z.prepareMetrics(metrics)
	records := prepared.records
	if len(records) == 0 {
		// If there are no metrics to ingest, clear the confirmed metrics and return the result.
		z.confirmed = nil
		return prepared.result()
	}
	// Store the original records for retry if the ingestion fails.
	original := records

	// If there are confirmed metrics, check if the new metrics have the same prefix.
	if z.confirmed != nil {
		// Check if the new metrics contain the already confirmed metrics.
		if recordsHavePrefix(records, z.confirmed) {
			// Remove the confirmed metrics.
			records = records[len(z.confirmed):]
			// If there is nothing to ingest return the ingestion result.
			if len(records) == 0 {
				z.confirmed = nil
				return prepared.result()
			}
		} else {
			z.confirmed = nil
		}
	}

	// Spread the records over the writers and chunk each share.
	if err := z.assignRecords(records); err != nil {
		return err
	}
	z.original = original
	z.confirmed = nil
	// Try to ingest the pending metrics.
	if err := z.flushWriters(); err != nil {
		return err
	}
	z.original = nil
	// Return the result of the ingestion.
	return prepared.result()
}

// Report whether a stream is open or a pending write can still be resumed.
func (z *Zerobus) connected() bool {
	for _, w := range z.writers {
		if w.stream != nil {
			return true
		}
	}
	return z.sdk != nil && z.hasPending()
}

// Report whether any writer was left mid-write by an earlier flush failure.
func (z *Zerobus) hasPending() bool {
	for _, w := range z.writers {
		if w.pending != nil {
			return true
		}
	}
	return false
}

// Split the records into contiguous shares, one per writer, and chunk each
// share into batches the stream accepts.
func (z *Zerobus) assignRecords(records [][]byte) error {
	shares := partitionRecords(records, len(z.writers))
	chunked := make([][]recordBatch, len(shares))
	for i, share := range shares {
		chunks, err := z.chunkRecords(share)
		if err != nil {
			return err
		}
		chunked[i] = chunks
	}
	for i, chunks := range chunked {
		if len(chunks) > 0 {
			z.writers[i].pending = &pendingWrite{remaining: chunks}
		}
	}
	return nil
}

// Split the records into at most count contiguous shares of near-equal size.
func partitionRecords(records [][]byte, count int) [][][]byte {
	if count <= 1 {
		return [][][]byte{records}
	}
	size := (len(records) + count - 1) / count
	shares := make([][][]byte, 0, count)
	for start := 0; start < len(records); start += size {
		shares = append(shares, records[start:min(start+size, len(records))])
	}
	return shares
}

// Flush every writer holding pending records and join their errors. Telegraf
// keeps the whole batch when any writer fails, so a retry resumes the writers
// that did not finish and skips the records the others already acknowledged.
func (z *Zerobus) flushWriters() error {
	pending := make([]*writer, 0, len(z.writers))
	for _, w := range z.writers {
		if w.pending != nil {
			pending = append(pending, w)
		}
	}
	// Stay on the calling goroutine unless there is work to parallelize.
	if len(pending) == 1 {
		return z.processPending(pending[0])
	}

	// Each writer reports into its own slot, keeping the joined error stable.
	var wg sync.WaitGroup
	errs := make([]error, len(pending))
	for i, w := range pending {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = z.processPending(w)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Close the Zerobus connection.
func (z *Zerobus) Close() error {
	// Close the streams.
	errs := make([]error, 0, len(z.writers)+1)
	for _, w := range z.writers {
		if w.stream != nil {
			errs = append(errs, w.stream.Close())
			w.stream = nil
		}
	}
	z.writers = nil
	// Clear the pending and confirmed metrics.
	z.original = nil
	z.confirmed = nil
	// Clear the cached descriptor.
	z.descriptor = nil
	z.descriptorReused = false
	// Close the SDK client.
	if z.sdk != nil {
		errs = append(errs, z.sdk.Close())
		z.sdk = nil
	}
	// Return the errors from the streams and SDK client.
	return errors.Join(errs...)
}

// Open a stream to the Zerobus server.
func (z *Zerobus) openStream(ctx context.Context, w *writer, clientSecret string) error {
	// Get the stream options.
	options := z.streamOptions()
	var (
		stream ingestStream
		err    error
	)
	// Create a table schema stream if the schema mode is table schema.
	if z.SchemaMode == schemaModeTableSchema {
		descriptor, fetchErr := z.tableDescriptor(ctx, clientSecret)
		if fetchErr != nil {
			return fetchErr
		}
		// Add the protobuf descriptor to the stream options.
		options = append([]sdkzerobus.StreamOption{sdkzerobus.WithProto(descriptor)}, options...)
		stream, err = z.sdk.CreateTableSchemaStream(
			ctx,
			z.TableName,
			z.ClientID,
			clientSecret,
			options...,
		)
	} else {
		// Create a static schema stream if the schema mode is static.
		descriptor, descriptorErr := messageDescriptor()
		if descriptorErr != nil {
			return fmt.Errorf("building protobuf descriptor failed: %w", descriptorErr)
		}
		// Add the protobuf descriptor to the stream options.
		options = append([]sdkzerobus.StreamOption{sdkzerobus.WithProto(descriptor)}, options...)
		stream, err = z.sdk.CreateStaticSchemaStream(
			ctx,
			z.TableName,
			z.ClientID,
			clientSecret,
			options...,
		)
	}
	if err != nil {
		return err
	}
	w.stream = stream
	return nil
}

// Return the descriptor to open a table-schema stream with, fetching it only
// when no reusable one is cached. Concurrent openers share one fetch.
func (z *Zerobus) tableDescriptor(ctx context.Context, clientSecret string) ([]byte, error) {
	z.descriptorMu.Lock()
	defer z.descriptorMu.Unlock()
	if z.descriptor != nil {
		return z.descriptor, nil
	}
	descriptor, err := z.sdk.FetchProtoDescriptor(ctx, z.TableName, z.ClientID, clientSecret)
	if err != nil {
		return nil, err
	}
	z.descriptor = descriptor
	z.descriptorReused = false
	return descriptor, nil
}

// Recreate the stream.
func (z *Zerobus) recreateStream(w *writer) error {
	// Get the unacknowledged batches.
	unacked, err := w.stream.GetUnackedBatches()
	// If the unacknowledged batches cannot be retrieved, return an error.
	if err != nil {
		return z.writeError("retrieving unacknowledged batches", err)
	}
	// Close the stream.
	_ = w.stream.Close()
	// Clear the stream.
	w.stream = nil
	// If the schema mode is table schema, rebuild the table schema replay batches.
	if z.SchemaMode == schemaModeTableSchema {
		z.ageDescriptor()
		if len(unacked) > len(w.pending.admitted) {
			return fmt.Errorf(
				"rebuilding table-schema replay batches failed: SDK returned %d "+
					"unacknowledged batches for %d admitted batches",
				len(unacked),
				len(w.pending.admitted),
			)
		}
		// If there are unacknowledged batches, rebuild the table schema replay batches.
		if len(unacked) > 0 {
			start := len(w.pending.admitted) - len(unacked)
			replay := slices.Clone(w.pending.admitted[start:])
			w.pending.remaining = append(replay, w.pending.remaining...)
		}
	} else {
		// If the schema mode is static, rebuild the static schema replay batches.
		replay := make([]recordBatch, 0, len(unacked)+len(w.pending.remaining))
		for _, batch := range unacked {
			replay = append(replay, recordBatch{records: batch, encoded: true})
		}
		w.pending.remaining = append(replay, w.pending.remaining...)
	}
	// Clear the admitted batches.
	w.pending.admitted = nil
	// Reset the waiting flag.
	w.pending.waiting = false

	return z.openStreamFromSecret(w)
}

// Decide whether the cached descriptor survives the next stream. The first
// recreation reuses it and the second recreation discards it.
func (z *Zerobus) ageDescriptor() {
	z.descriptorMu.Lock()
	defer z.descriptorMu.Unlock()
	switch {
	case z.descriptor == nil:
	case z.descriptorReused:
		z.descriptor = nil
		z.descriptorReused = false
	default:
		z.descriptorReused = true
	}
}

// Mark the cached descriptor as current after a stream accepted records.
func (z *Zerobus) freshenDescriptor() {
	z.descriptorMu.Lock()
	defer z.descriptorMu.Unlock()
	z.descriptorReused = false
}

// Open a stream from the client secret.
func (z *Zerobus) openStreamFromSecret(w *writer) error {
	// Get the client secret.
	secret, err := z.ClientSecret.Get()
	if err != nil {
		return fmt.Errorf("resolving client secret for stream recovery failed: %w", err)
	}
	defer secret.Destroy()
	// Create a context with a timeout for the connection.
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(z.ConnectTimeout))
	defer cancel()
	// Open a stream to the Zerobus server.
	if err := z.openStream(ctx, w, secret.String()); err != nil {
		return z.writeError("recreating stream", err)
	}
	return nil
}

// Process the pending metrics.
func (z *Zerobus) processPending(w *writer) error {
	// If the stream is nil, open a new stream.
	if w.stream == nil {
		if err := z.openStreamFromSecret(w); err != nil {
			return err
		}
	}
	// If the stream is closed, recreate it.
	if w.stream.IsClosed() {
		// If the stream is closed, recreate it.
		if err := z.recreateStream(w); err != nil {
			return err
		}
	}
	// If there are pending metrics, flush the stream.
	if w.pending.waiting {
		if err := w.stream.Flush(); err != nil {
			return z.writeError("flushing previously admitted batch", err)
		}
		// Reset the waiting flag.
		w.pending.waiting = false
		// Clear the admitted batches.
		w.pending.admitted = nil
	}

	// Loop while there are remaining chunks.
	for len(w.pending.remaining) > 0 {
		// Get the first chunk.
		chunk := w.pending.remaining[0]
		// Ingest the chunk.
		if _, err := w.stream.IngestRecordsOffset(chunk.records, chunk.encoded); err != nil {
			return z.writeError("admitting batch", err)
		}
		// Remove the chunk from the remaining chunks & add it to the admitted batches.
		w.pending.remaining = w.pending.remaining[1:]
		w.pending.admitted = append(w.pending.admitted, chunk)
		// Set the waiting flag.
		w.pending.waiting = true
	}

	// If there are still pending metrics, flush the stream.
	if w.pending.waiting {
		// Flush the stream.
		if err := w.stream.Flush(); err != nil {
			return z.writeError("flushing batch", err)
		}
	}
	// The stream accepted records, so the descriptor it opened with is current.
	z.freshenDescriptor()
	// Reset the pending metrics.
	w.pending = nil
	return nil
}

// Chunk the records into smaller batches.
func (z *Zerobus) chunkRecords(records [][]byte) ([]recordBatch, error) {
	// Get the maximum payload bytes.
	maxBytes := int(z.MaxPayloadBytes)
	if maxBytes <= 0 {
		maxBytes = defaultMaxPayloadBytes
	}
	// Calculate the payload budget.
	payloadBudget := maxBytes - batchEnvelopeReserve
	// If the payload budget is too small, return an error.
	if payloadBudget <= 0 {
		return nil, fmt.Errorf(
			"max_payload_bytes=%d is too small; it must exceed %d bytes",
			maxBytes,
			batchEnvelopeReserve,
		)
	}

	// Create a slice to store the chunks.
	chunks := make([]recordBatch, 0, (len(records)+z.MaxBatchRecords-1)/z.MaxBatchRecords)
	// Loop while there are records to chunk.
	for len(records) > 0 {
		// Initialize the count and size.
		count, size := 0, 0
		// Loop while there are records to chunk and the count is less than the maximum batch records.
		for count < len(records) && count < z.MaxBatchRecords {
			recordSize := protowire.SizeTag(1) + protowire.SizeBytes(len(records[count]))
			if err := z.validateRecordSize(recordSize, payloadBudget); err != nil {
				return nil, fmt.Errorf(
					"serialized metric %d cannot be admitted: %w",
					count,
					err,
				)
			}
			if size+recordSize > payloadBudget {
				break
			}
			if z.MaxBufferedPayloadBytes > 0 &&
				retainedPayloadSize(size+recordSize, count+1) >
					int64(z.MaxBufferedPayloadBytes) {
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

// Prepare the metrics for the Zerobus server.
func (z *Zerobus) prepareMetrics(metrics []telegraf.Metric) preparedWrite {
	// Create a prepared write.
	prepared := preparedWrite{
		records: make([][]byte, 0, len(metrics)),
		accept:  make([]int, 0, len(metrics)),
	}
	// Get the maximum payload bytes.
	maxBytes := int(z.MaxPayloadBytes)
	if maxBytes <= 0 {
		maxBytes = defaultMaxPayloadBytes
	}
	payloadBudget := maxBytes - batchEnvelopeReserve

	// Loop through the metrics.
	for i, metric := range metrics {
		// Serialize the metric.
		record, err := z.serializeMetric(metric)
		// If the serialization is successful, validate the record size.
		if err == nil {
			recordSize := protowire.SizeTag(1) + protowire.SizeBytes(len(record))
			err = z.validateRecordSize(recordSize, payloadBudget)
		}
		// If the validation is not successful, add the metric to the rejected metrics.
		if err != nil {
			prepared.reject = append(prepared.reject, i)
			prepared.rejectErrors = append(prepared.rejectErrors, err)
			continue
		}
		// Add the metric to the accepted metrics.
		prepared.records = append(prepared.records, record)
		prepared.accept = append(prepared.accept, i)
	}
	return prepared
}

// Return the result of the prepared write.
func (p *preparedWrite) result() error {
	if len(p.reject) == 0 {
		return nil
	}
	return &internal.PartialWriteError{
		Err: fmt.Errorf(
			"Zerobus rejected %d metric(s): %w",
			len(p.reject),
			errors.Join(p.rejectErrors...),
		),
		MetricsAccept:       p.accept,
		MetricsReject:       p.reject,
		MetricsRejectErrors: p.rejectErrors,
	}
}

// Validate the record size.
func (z *Zerobus) validateRecordSize(recordSize, payloadBudget int) error {
	// If the record size is greater than the payload budget, return an error.
	if recordSize > payloadBudget {
		return fmt.Errorf(
			"requires %d bytes, exceeding the payload budget of %d bytes",
			recordSize,
			payloadBudget,
		)
	}
	if z.MaxBufferedPayloadBytes > 0 {
		retained := retainedPayloadSize(recordSize, 1)
		if retained > int64(z.MaxBufferedPayloadBytes) {
			return fmt.Errorf(
				"requires approximately %d buffered bytes, exceeding "+
					"max_buffered_payload_bytes=%d",
				retained,
				z.MaxBufferedPayloadBytes,
			)
		}
	}
	return nil
}

// Calculate the retained payload size.
func retainedPayloadSize(recordBytes, recordCount int) int64 {
	return int64(recordBytes) +
		int64(recordCount)*bufferedRecordOverhead +
		bufferedRequestOverhead
}

// Serialize a metric for static mode.
func serializeStaticMetric(metric telegraf.Metric) ([]byte, error) {
	record, err := metricToProto(metric)
	if err != nil {
		return nil, fmt.Errorf("serializing metric failed: %w", err)
	}
	serialized, err := (proto.MarshalOptions{Deterministic: true}).Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshaling protobuf record failed: %w", err)
	}
	return serialized, nil
}

// Serialize a metric based on the schema mode.
func (z *Zerobus) serializeMetric(metric telegraf.Metric) ([]byte, error) {
	if z.SchemaMode == schemaModeTableSchema {
		return metricToTableSchemaJSON(metric, z.TimestampColumn, z.MeasurementColumn)
	}
	return serializeStaticMetric(metric)
}

// Check if the records contain another set of records as a prefix.
func recordsHavePrefix(records, prefix [][]byte) bool {
	return len(records) >= len(prefix) &&
		slices.EqualFunc(records[:len(prefix)], prefix, slices.Equal)
}

// Get the stream options.
func (z *Zerobus) streamOptions() []sdkzerobus.StreamOption {
	options := []sdkzerobus.StreamOption{sdkzerobus.WithWaitForReady()}
	if z.MaxInflight > 0 {
		options = append(options, sdkzerobus.WithMaxInflight(z.MaxInflight))
	}
	if z.MaxBufferedPayloadBytes > 0 {
		options = append(
			options,
			sdkzerobus.WithMaxBufferedPayloadBytes(int64(z.MaxBufferedPayloadBytes)),
		)
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

// Write an error.
func (z *Zerobus) writeError(operation string, err error) error {
	retryable := sdkzerobus.Retryable(err)
	return fmt.Errorf("Zerobus %s failed (retryable=%t): %w", operation, retryable, err)
}

// Get the message descriptor.
func messageDescriptor() ([]byte, error) {
	descriptor := protodesc.ToDescriptorProto((&TelegrafMetric{}).ProtoReflect().Descriptor())
	return proto.Marshal(descriptor)
}

// Register the Zerobus output plugin.
func init() {
	outputs.Add("zerobus", func() telegraf.Output {
		return &Zerobus{
			SchemaMode:        schemaModeStatic,
			TimestampColumn:   "timestamp",
			ConnectTimeout:    config.Duration(defaultConnectTimeout),
			ConcurrentStreams: 1,
			MaxBatchRecords:   defaultMaxBatchRecords,
		}
	})
}
