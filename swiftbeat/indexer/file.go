package indexer

import (
	"os"
	"path/filepath"
	"time"

	"github.com/elastic/beats/swiftbeat/input"
)

type FileRecord struct {
	*IndexRecord
	*Hash
	Size        int64
	LastIndexed time.Time
}

type FileRecordSorter []*FileRecord

func (files FileRecordSorter) Len() int {
	return len(files)
}

func (files FileRecordSorter) Less(i, j int) bool {
	return files[i].Mtime.After(files[j].Mtime)
}

func (files FileRecordSorter) Swap(i, j int) {
	files[i], files[j] = files[j], files[i]
}

// NewFileRecord returns a new FileRecord object
func NewFileRecord(
	h *Hash,
	file os.FileInfo,
) (*FileRecord, error) {
	f := &FileRecord{
		IndexRecord: &IndexRecord{
			Name:  file.Name(),
			Path:  filepath.Join(h.Path, file.Name()),
			Mtime: file.ModTime(),
		},
		Size: file.Size(),
		Hash: h,
	}
	return f, nil
}

type IndexableFile interface {
	Index()
	ToEvent() input.Event
	Mtime() time.Time
}

type IndexableFileSorter []IndexableFile

func (files IndexableFileSorter) Len() int {
	return len(files)
}

func (files IndexableFileSorter) Less(i, j int) bool {
	return files[i].Mtime().Before(files[j].Mtime())
}

func (files IndexableFileSorter) Swap(i, j int) {
	files[i], files[j] = files[j], files[i]
}
