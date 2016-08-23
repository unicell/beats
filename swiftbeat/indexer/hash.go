package indexer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/elastic/beats/libbeat/logp"
)

type Hash struct {
	*IndexRecord
	name      string
	suffix    *Suffix
	datafiles []*Datafile
}

type HashSorter []*Hash

func (hashes HashSorter) Len() int {
	return len(hashes)
}

func (hashes HashSorter) Less(i, j int) bool {
	return hashes[i].mtime.After(hashes[j].mtime)
}

func (hashes HashSorter) Swap(i, j int) {
	hashes[i], hashes[j] = hashes[j], hashes[i]
}

func NewHash(s *Suffix, file os.FileInfo) (*Hash, error) {
	hash := &Hash{
		IndexRecord: &IndexRecord{
			path:  filepath.Join(s.path, file.Name()),
			mtime: file.ModTime(),
		},
		name:      file.Name(),
		suffix:    s,
		datafiles: nil,
	}
	return hash, nil
}

func (h *Hash) init() {
	path := h.path
	logp.Debug("hash", "Init hash layout: %s", path)

	var dfiles DatafileSorter

	files, err := ioutil.ReadDir(path)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", path, err)
		return
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}

		dfile, _ := NewDatafile(h, file)
		dfiles = append(dfiles, dfile)
	}

	sort.Sort(dfiles)
	h.datafiles = dfiles
}

// BuildIndex builds index for one hash dir
// TODO: move away from naive linear scanning to better strategy
func (h *Hash) BuildIndex() {
	logp.Debug("hash", "Build index for hash: %s", h.path)

	// load file list for the hash
	h.init()

	for _, dfile := range h.datafiles {
		dfile.BuildIndex()
	}
}
