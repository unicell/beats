package input

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

var knownObjectMetaKey = []string{
	"name",
	"Content-Type",
	"Content-Length",
	"X-Object-Meta-Mtime",
	"X-Timestamp",
	"ETag",
}

// Event represends the data generated from indexer and to be sent to the output
type Event interface {
	ToMapStr() common.MapStr
	Bytes() int
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
		"@timestamp": common.Time(ev.Object.Mtime),
		"type":       "object",
		"partition":  ev.Object.Partition,
		"hash":       ev.Object.Hash,
		"path":       ev.Object.Path,
	}

	for _, k := range knownObjectMetaKey {
		if v, ok := ev.Object.Metadata[k]; ok {
			event[strings.ToLower(k)] = v
		}
	}

	if v, ok := event["content-length"]; ok {
		if vInt, err := strconv.ParseInt(v.(string), 10, 64); err == nil {
			event["content-length"] = vInt
		}
	}

	event["message"] = fmt.Sprintf("%s", ev.Object.Metadata)
	return event
}

func (ev *ObjectEvent) Bytes() int {
	return 1
}
