package zerobus

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/influxdata/telegraf"
)

// Converts a Telegraf Metric into a protobuf message.
func metricToProto(metric telegraf.Metric) (*TelegrafMetric, error) {
	fields, err := metricFieldsJSON(metric)
	if err != nil {
		return nil, err
	}
	return &TelegrafMetric{
		Measurement: proto.String(metric.Name()),
		TimestampNs: proto.Int64(metric.Time().UnixNano()),
		Tags:        metric.Tags(),
		Fields:      proto.String(string(fields)),
	}, nil
}

// Turns the fields of the Telegraf Metric into a JSON string.
func metricFieldsJSON(metric telegraf.Metric) ([]byte, error) {
	values := make(map[string]interface{}, len(metric.FieldList()))
	for _, field := range metric.FieldList() {
		if field == nil {
			return nil, fmt.Errorf("metric %q contains a nil field", metric.Name())
		}
		value, err := fieldToVariant(field)
		if err != nil {
			return nil, fmt.Errorf(
				"converting field %q of metric %q failed: %w",
				field.Key,
				metric.Name(),
				err,
			)
		}
		values[field.Key] = value
	}

	// Marshaling a map sorts the keys, so equal metrics produce equal records.
	fields, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("marshaling fields of metric %q failed: %w", metric.Name(), err)
	}
	return fields, nil
}

// Converts one field to a variant encoding.
func fieldToVariant(field *telegraf.Field) (interface{}, error) {
	switch value := field.Value.(type) {
	// The types that are supported by the VARIANT column.
	case int64, bool, string:
		return value, nil
	// The uint64 type is encoded as a Delta BIGINT.
	case uint64:
		if value > math.MaxInt64 {
			return nil, fmt.Errorf(
				"uint64 value %d exceeds Delta BIGINT maximum %d",
				value,
				int64(math.MaxInt64),
			)
		}
		return int64(value), nil
	// The float64 type is encoded as a DOUBLE.
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, errors.New("non-finite float cannot be represented in JSON")
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported field type %T", field.Value)
	}
}
