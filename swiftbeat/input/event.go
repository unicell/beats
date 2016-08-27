package input

import (
	"github.com/elastic/beats/libbeat/common"
	"time"
)

// Event represends the data generated from indexer and to be sent to the output
type Event struct {
	common.EventMetadata
	Bytes int
	Text  *string
	path  string
	//*IndexRecord
	//name      string
	//suffix    *Suffix
}

func NewEvent(path string) *Event {
	return &Event{
		path:  path,
		Bytes: 1,
	}
}

func (f *Event) ToMapStr() common.MapStr {
	event := common.MapStr{
		"@timestamp": common.Time(time.Now()),
		"type":       "swift",
	}
	event["message"] = f.path
	return event
}
