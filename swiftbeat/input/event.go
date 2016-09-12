package input

import (
	"strconv"
	"strings"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

// Event represends the data generated from indexer and to be sent to the output
type Event interface {
	ToMapStr() common.MapStr
	Bytes() int
}

var knownObjectMetaKey = []string{
	"name",
	"Content-Type",
	"Content-Length",
	"X-Object-Meta-Mtime",
	"X-Timestamp",
	"ETag",
}

var str2intObjectFields = []string{
	"content-length",
	"partition",
}

type ObjectEvent struct {
	common.EventMetadata
	Object swift.Object
}

func NewObjectEvent(object swift.Object) *ObjectEvent {
	return &ObjectEvent{
		Object: object,
	}
}

func (ev *ObjectEvent) ToMapStr() common.MapStr {
	event := common.MapStr{
		"@timestamp":   common.Time(ev.Object.Mtime),
		"type":         "object",
		"handoff":      ev.Object.Handoff,
		"device":       ev.Object.Device,
		"ip":           ev.Object.Ip,
		"partition":    ev.Object.Partition,
		"hash":         ev.Object.Hash,
		"path":         ev.Object.Path,
		"peer_devices": ev.Object.PeerDevices,
		"peer_ips":     ev.Object.PeerIps,
	}

	// copy object metadata key / values to event
	for _, k := range knownObjectMetaKey {
		if v, ok := ev.Object.Metadata[k]; ok {
			event[strings.ToLower(k)] = v
		}
	}

	for _, k := range str2intObjectFields {
		if v, ok := event[k]; ok {
			if vInt, err := strconv.ParseInt(v.(string), 10, 64); err == nil {
				event[k] = vInt
			}
		}
	}

	return event
}

func (ev *ObjectEvent) Bytes() int {
	return 1
}

type PartitionEvent struct {
	common.EventMetadata
	Partition swift.Partition
}

func NewPartitionEvent(partition swift.Partition) *PartitionEvent {
	return &PartitionEvent{
		Partition: partition,
	}
}

func (ev *PartitionEvent) ToMapStr() common.MapStr {
	partInt := int64(-1)
	if i, err := strconv.ParseInt(ev.Partition.Name, 10, 64); err == nil {
		partInt = i
	}

	event := common.MapStr{
		"@timestamp":     common.Time(ev.Partition.Mtime),
		"type":           "partition",
		"resource_type":  ev.Partition.ResourceType,
		"device":         ev.Partition.Device,
		"ip":             ev.Partition.Ip,
		"ring_mtime":     common.Time(ev.Partition.RingMtime),
		"partition":      partInt,
		"handoff":        ev.Partition.Handoff,
		"num_datafiles":  ev.Partition.NumDatafiles,
		"num_tomestones": ev.Partition.NumTomstones,
		"bytes_total_mb": ev.Partition.BytesMBTotal,
		"peer_devices":   ev.Partition.PeerDevices,
		"peer_ips":       ev.Partition.PeerIps,
		"last_indexed":   common.Time(ev.Partition.LastIndexed),
	}

	return event
}

func (ev *PartitionEvent) Bytes() int {
	return 1
}
