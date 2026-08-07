package zerobus

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/influxdata/telegraf"
)

func metricToTableSchemaJSON(
	metric telegraf.Metric,
	timestampColumn, measurementColumn string,
) ([]byte, error) {
	// The values are stored in a map to be marshaled into a JSON string.
	values := make(map[string]interface{}, len(metric.TagList())+len(metric.FieldList())+2)
	// Timestamp column is optional.
	if timestampColumn != "" {
		values[timestampColumn] = metric.Time().UnixMicro()
	}
	// Measurement column is optional, most tables are already per-measurement.
	if measurementColumn != "" {
		values[measurementColumn] = metric.Name()
	}

	// Tags and fields are mapped to columns in the destination table.
	for _, tag := range metric.TagList() {
		if tag == nil {
			return nil, fmt.Errorf("metric contains a nil tag")
		}
		// Tags must be unique.
		if _, found := values[tag.Key]; found {
			return nil, fmt.Errorf("tag %q conflicts with another table column", tag.Key)
		}
		values[tag.Key] = tag.Value
	}

	for _, field := range metric.FieldList() {
		if field == nil {
			return nil, fmt.Errorf("metric contains a nil field")
		}
		// Fields must be unique.
		if _, found := values[field.Key]; found {
			return nil, fmt.Errorf("field %q conflicts with another table column", field.Key)
		}
		switch value := field.Value.(type) {
		// The types that are supported by the table schema.
		case int64, bool, string:
			values[field.Key] = value
		// The uint64 type is encoded as a Delta BIGINT.
		case uint64:
			if value > math.MaxInt64 {
				return nil, fmt.Errorf(
					"field %q contains uint64 value %d exceeding Delta BIGINT maximum %d",
					field.Key,
					value,
					int64(math.MaxInt64),
				)
			}
			values[field.Key] = int64(value)
		// The float64 type is encoded as a DOUBLE.
		case float64:
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("field %q contains a non-finite float", field.Key)
			}
			values[field.Key] = value
		default:
			return nil, fmt.Errorf("field %q has unsupported type %T", field.Key, field.Value)
		}
	}

	// Turns the values into a JSON string.
	record, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("marshaling table-schema JSON record failed: %w", err)
	}
	return record, nil
}
