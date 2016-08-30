package input

import (
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/swiftbeat/input/swift"
)

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
		"@timestamp": common.Time(time.Now()),
		"type":       "object",
	}
	event["message"] = ev.Object.Path
	return event
}

func (ev *ObjectEvent) Bytes() int {
	return 1
}
