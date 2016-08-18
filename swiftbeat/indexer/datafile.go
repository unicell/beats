package indexer

//import (
//"github.com/elastic/beats/libbeat/logp"
//)

type Datafile struct {
	*IndexRecord
}

type DatafileSorter []Datafile

func (dfiles DatafileSorter) Len() int {
	return len(dfiles)
}

func (dfiles DatafileSorter) Less(i, j int) bool {
	return dfiles[i].mtime.After(dfiles[j].mtime)
}

func (dfiles DatafileSorter) Swap(i, j int) {
	dfiles[i], dfiles[j] = dfiles[j], dfiles[i]
}

//func (dfile *Datafile) init() {
//path := dfile.path
//logp.Debug("hash", "Init datafile info: %s", path)
//}

// BuildIndex builds index for one datafile
// TODO: move away from naive linear scanning to better strategy
func (f *Datafile) BuildIndex() {

	// load file list for the hash
	//h.init()

	//for _, dfile := range h.datafiles {
	//logp.Debug("datafile", "Build index for datafile: %s", dfile.path)
	//dfile.BuildIndex()
	//}
}
