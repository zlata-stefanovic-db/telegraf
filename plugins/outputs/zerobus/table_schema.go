package zerobus

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/influxdata/telegraf"
)

func serializeTableSchemaMetrics(
	metrics []telegraf.Metric,
	timestampColumn, measurementColumn string,
) ([][]byte, error) {
	records := make([][]byte, 0, len(metrics))
	for i, metric := range metrics {
		record, err := metricToTableSchemaJSON(metric, timestampColumn, measurementColumn)
		if err != nil {
			return nil, fmt.Errorf(
				"serializing metric %d (%q) for table schema failed: %w",
				i,
				metric.Name(),
				err,
			)
		}
		records = append(records, record)
	}
	return records, nil
}

func metricToTableSchemaJSON(
	metric telegraf.Metric,
	timestampColumn, measurementColumn string,
) ([]byte, error) {
	values := make(map[string]interface{}, len(metric.TagList())+len(metric.FieldList())+2)
	if timestampColumn != "" {
		values[timestampColumn] = metric.Time().UnixMicro()
	}
	if measurementColumn != "" {
		values[measurementColumn] = metric.Name()
	}

	for _, tag := range metric.TagList() {
		if tag == nil {
			return nil, fmt.Errorf("metric contains a nil tag")
		}
		if _, found := values[tag.Key]; found {
			return nil, fmt.Errorf("tag %q conflicts with another table column", tag.Key)
		}
		values[tag.Key] = tag.Value
	}

	for _, field := range metric.FieldList() {
		if field == nil {
			return nil, fmt.Errorf("metric contains a nil field")
		}
		if _, found := values[field.Key]; found {
			return nil, fmt.Errorf("field %q conflicts with another table column", field.Key)
		}
		switch value := field.Value.(type) {
		case int64, bool, string:
			values[field.Key] = value
		case uint64:
			values[field.Key] = strconv.FormatUint(value, 10)
		case float64:
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("field %q contains a non-finite float", field.Key)
			}
			values[field.Key] = value
		default:
			return nil, fmt.Errorf("field %q has unsupported type %T", field.Key, field.Value)
		}
	}

	record, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("marshaling table-schema JSON record failed: %w", err)
	}
	return record, nil
}
