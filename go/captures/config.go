package captures

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// TODO: move to partition_range.go
// PartitionRange is the parsed shard labels that determine the range of partitions that this shard
// will be responsible for.
type PartitionRange struct {
	BeginInclusive uint32
	EndExclusive   uint32
}

// TODO: this is gross, but I'm not sure if there's a better way
func (pr *PartitionRange) UnmarshalJSON(bytes []byte) error {
	// This map will be the whole object if
	var tmp = make(map[string]string)
	var err = json.Unmarshal(bytes, &tmp)
	if err != nil {
		return err
	}
	if begin, ok := tmp["begin"]; ok {
		b, err := strconv.ParseUint(begin, 16, 32)
		if err != nil {
			return fmt.Errorf("parsing partition range 'begin': %w", err)
		}
		pr.BeginInclusive = uint32(b)
	}
	if end, ok := tmp["end"]; ok {
		b, err := strconv.ParseUint(end, 16, 32)
		if err != nil {
			return fmt.Errorf("parsing partition range 'end': %w", err)
		}
		pr.EndExclusive = uint32(b)
	}
	return nil
}

func (r PartitionRange) Includes(_partitionID []byte) bool {
	// TODO: hash the id and see if the range overlaps
	return true
}
