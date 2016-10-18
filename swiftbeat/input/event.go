package input

import (
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

// Event represends the data generated from indexer and to be sent to the output
type Event interface {
	ToMapStr() common.MapStr
	ResourceType() string
	ToPartition() *swift.Partition
	Bytes() int
	SetTTL(time.Duration)
	GetTTL() time.Duration
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

func (ev *ObjectEvent) ResourceType() string {
	return "object"
}

func (ev *ObjectEvent) ToPartition() *swift.Partition {
	return nil
}

func (ev *ObjectEvent) GetTTL() time.Duration {
	return -1 * time.Second
}

func (ev *ObjectEvent) SetTTL(ttl time.Duration) {
	return
}

type ObjectPartitionEvent struct {
	common.EventMetadata
	ObjPart swift.ObjectPartition
	ttl     time.Duration
}

func NewObjectPartitionEvent(objPart swift.ObjectPartition) *ObjectPartitionEvent {
	return &ObjectPartitionEvent{
		ObjPart: objPart,
		ttl:     -1 * time.Second,
	}
}

func (ev *ObjectPartitionEvent) ToMapStr() common.MapStr {
	event := common.MapStr{
		"@timestamp":     common.Time(ev.ObjPart.IndexedAt),
		"type":           "obj_partition",
		"mtime":          common.Time(ev.ObjPart.Mtime),
		"partition":      ev.ObjPart.PartId,
		"num_datafiles":  ev.ObjPart.NumDatafiles,
		"num_tombstones": ev.ObjPart.NumTombstones,
		"bytes_total_mb": ev.ObjPart.BytesTotalMB,
		"indexed_at":     common.Time(ev.ObjPart.IndexedAt),
		"resource_type":  ev.ObjPart.ResourceType,
		"device":         ev.ObjPart.Device,
		"ip":             ev.ObjPart.Ip,
		"ring_mtime":     common.Time(ev.ObjPart.RingMtime),
		"handoff":        ev.ObjPart.Handoff,
		"replica_id":     ev.ObjPart.ReplicaId,
		"peer_devices":   ev.ObjPart.PeerDevices,
		"peer_ips":       ev.ObjPart.PeerIps,
		"ring_cksum":     ev.ObjPart.RingCKSum,
	}

	return event
}

func (ev *ObjectPartitionEvent) Bytes() int {
	return 1
}

func (ev *ObjectPartitionEvent) ResourceType() string {
	return ev.ObjPart.ResourceType
}

func (ev *ObjectPartitionEvent) ToPartition() *swift.Partition {
	return ev.ObjPart.Partition
}

func (ev *ObjectPartitionEvent) GetTTL() time.Duration {
	return ev.ttl
}

func (ev *ObjectPartitionEvent) SetTTL(ttl time.Duration) {
	ev.ttl = ttl
}

type ContainerEvent struct {
	common.EventMetadata
	Container swift.Container
	ttl       time.Duration
}

func NewContainerEvent(container swift.Container) *ContainerEvent {
	return &ContainerEvent{
		Container: container,
		ttl:       -1 * time.Second,
	}
}

func (ev *ContainerEvent) ToMapStr() common.MapStr {

	event := common.MapStr{
		"@timestamp":    common.Time(ev.Container.IndexedAt),
		"type":          "db",
		"mtime":         common.Time(ev.Container.Mtime),
		"path":          ev.Container.Path,
		"db_size_kb":    ev.Container.SizeKB,
		"account":       ev.Container.Account,
		"container":     ev.Container.Container,
		"status":        ev.Container.Status,
		"object_count":  ev.Container.ObjectCount,
		"bytes_used_mb": ev.Container.BytesUsedMB,
		"policy_index":  ev.Container.PolicyIndex,
		"indexed_at":    common.Time(ev.Container.IndexedAt),
		"resource_type": ev.Container.ResourceType,
		"partition":     ev.Container.PartId,
		"device":        ev.Container.Device,
		"ip":            ev.Container.Ip,
		"ring_mtime":    common.Time(ev.Container.RingMtime),
		"handoff":       ev.Container.Handoff,
		"replica_id":    ev.Container.ReplicaId,
		"peer_devices":  ev.Container.PeerDevices,
		"peer_ips":      ev.Container.PeerIps,
		"ring_cksum":    ev.Container.RingCKSum,
	}

	return event
}

func (ev *ContainerEvent) Bytes() int {
	return 1
}

func (ev *ContainerEvent) ResourceType() string {
	return ev.Container.ResourceType
}

func (ev *ContainerEvent) ToPartition() *swift.Partition {
	return ev.Container.Partition
}

func (ev *ContainerEvent) GetTTL() time.Duration {
	return ev.ttl
}

func (ev *ContainerEvent) SetTTL(ttl time.Duration) {
	ev.ttl = ttl
}

type AccountEvent struct {
	common.EventMetadata
	Account swift.Account
	ttl     time.Duration
}

func NewAccountEvent(account swift.Account) *AccountEvent {
	return &AccountEvent{
		Account: account,
		ttl:     -1 * time.Second,
	}
}

func (ev *AccountEvent) ToMapStr() common.MapStr {

	event := common.MapStr{
		"@timestamp":      common.Time(ev.Account.IndexedAt),
		"type":            "db",
		"mtime":           common.Time(ev.Account.Mtime),
		"path":            ev.Account.Path,
		"db_size_kb":      ev.Account.SizeKB,
		"account":         ev.Account.Account,
		"status":          ev.Account.Status,
		"container_count": ev.Account.ContainerCount,
		"object_count":    ev.Account.ObjectCount,
		"bytes_used_mb":   ev.Account.BytesUsedMB,
		"indexed_at":      common.Time(ev.Account.IndexedAt),
		"resource_type":   ev.Account.ResourceType,
		"partition":       ev.Account.PartId,
		"device":          ev.Account.Device,
		"ip":              ev.Account.Ip,
		"ring_mtime":      common.Time(ev.Account.RingMtime),
		"handoff":         ev.Account.Handoff,
		"replica_id":      ev.Account.ReplicaId,
		"peer_devices":    ev.Account.PeerDevices,
		"peer_ips":        ev.Account.PeerIps,
		"ring_cksum":      ev.Account.RingCKSum,
	}

	return event
}

func (ev *AccountEvent) Bytes() int {
	return 1
}

func (ev *AccountEvent) ResourceType() string {
	return ev.Account.ResourceType
}

func (ev *AccountEvent) ToPartition() *swift.Partition {
	return ev.Account.Partition
}

func (ev *AccountEvent) GetTTL() time.Duration {
	return ev.ttl
}

func (ev *AccountEvent) SetTTL(ttl time.Duration) {
	ev.ttl = ttl
}
