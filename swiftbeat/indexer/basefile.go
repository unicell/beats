package indexer

import (
	"os"
	"path/filepath"
)

type IndexableFile struct {
	*IndexRecord
	*Hash
	Size int64
}

type IndexableFileSorter []*IndexableFile

func (files IndexableFileSorter) Len() int {
	return len(files)
}

func (files IndexableFileSorter) Less(i, j int) bool {
	return files[i].Mtime.After(files[j].Mtime)
}

func (files IndexableFileSorter) Swap(i, j int) {
	files[i], files[j] = files[j], files[i]
}

// NewIndexableFile returns a new IndexableFile object
func NewIndexableFile(
	h *Hash,
	file os.FileInfo,
) (*IndexableFile, error) {
	f := &IndexableFile{
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
