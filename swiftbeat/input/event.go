package input

import (
	"github.com/elastic/beats/libbeat/common"
)

// Event represends the data generated from indexer and to be sent to the output
type Event struct {
	path string
}

func NewEvent(path string) *Event {
	return &Event{
		path: path,
	}
}

func (f *Event) ToMapStr() common.MapStr {
	event := common.MapStr{
		"something": f.path,
	}
	return event
}
