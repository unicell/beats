package indexer

import (
	"time"

	"github.com/elastic/beats/swiftbeat/input"
)

// IndexRecord is a struct to be embedded in all indexable structs
type IndexRecord struct {
	Name  string
	Path  string
	Mtime time.Time
}

// Indexer interface for all level indexable resources
type Indexer interface {
	BuildIndex()
	GetEvents() <-chan input.Event
}
