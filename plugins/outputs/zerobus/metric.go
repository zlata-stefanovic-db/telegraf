package zerobus

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/influxdata/telegraf"
)

func metricToProto(metric telegraf.Metric) (*TelegrafMetric, error) {
	fields := slices.Clone(metric.FieldList())
	for _, field := range fields {
		if field == nil {
			return nil, fmt.Errorf("metric %q contains a nil field", metric.Name())
		}
	}
	slices.SortFunc(fields, func(a, b *telegraf.Field) int {
		return cmp.Compare(a.Key, b.Key)
	})

	values := make([]*TelegrafMetric_FieldValue, 0, len(fields))
	for _, field := range fields {
		value, err := fieldToProto(field)
		if err != nil {
			return nil, fmt.Errorf(
				"converting field %q of metric %q failed: %w",
				field.Key,
				metric.Name(),
				err,
			)
		}
		values = append(values, value)
	}

	return &TelegrafMetric{
		Measurement: proto.String(metric.Name()),
		TimestampNs: proto.Int64(metric.Time().UnixNano()),
		Tags:        metric.Tags(),
		Fields:      values,
	}, nil
}

func fieldToProto(field *telegraf.Field) (*TelegrafMetric_FieldValue, error) {
	value := &TelegrafMetric_FieldValue{Key: proto.String(field.Key)}

	switch v := field.Value.(type) {
	case int64:
		value.Type = proto.String("int")
		value.IntValue = proto.Int64(v)
	case uint64:
		value.Type = proto.String("uint")
		value.UintValue = proto.String(strconv.FormatUint(v, 10))
	case float64:
		value.Type = proto.String("float")
		value.FloatValue = proto.Float64(v)
	case bool:
		value.Type = proto.String("bool")
		value.BoolValue = proto.Bool(v)
	case string:
		value.Type = proto.String("string")
		value.StringValue = proto.String(v)
	default:
		return nil, fmt.Errorf("unsupported field type %T", field.Value)
	}

	return value, nil
}
