package indexer

import (
//"io/ioutil"
//"os"
//"path/filepath"
//"time"
//"github.com/elastic/beats/libbeat/logp"
)

type Partition struct {
	*IndexRecord
	// todo: hashes.pkl
	suffixes []Suffix
}

type PartitionSorter []Partition

func (parts PartitionSorter) Len() int {
	return len(parts)
}

func (parts PartitionSorter) Less(i, j int) bool {
	return parts[i].mtime.After(parts[j].mtime)
}

func (parts PartitionSorter) Swap(i, j int) {
	parts[i], parts[j] = parts[j], parts[i]
}

//func (p *Partition) init() {
//logp.debug("indexer")
//}

func (p *Partition) BuildIndex() {
	//logp.debug("indexer")
}

type Suffix struct {
	*IndexRecord
	// todo: hashes.pkl
	//suffixes []suffix
}

//func (p *Suffix) init() {
//logp.debug("indexer")
//}

func (p *Suffix) BuildIndex() {
	//logp.debug("indexer")
}
