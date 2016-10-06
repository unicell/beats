package prospector

import (
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/indexer"
)

type DiskProspector struct {
	Prospector *Prospector
	config     prospectorConfig
	devName    string
	devPath    string
	disk       *indexer.Disk
}

func NewDiskProspector(p *Prospector, name string, path string) *DiskProspector {

	prospectorer := &DiskProspector{
		Prospector: p,
		config:     p.config,
		devName:    name,
		devPath:    path,
	}

	return prospectorer
}

func (p *DiskProspector) Init() error {

	disk, err := indexer.NewDisk(p.devName, p.devPath,
		p.Prospector.harvesterChan, p.Prospector.done)
	if err != nil {
		return err
	}
	p.disk = disk

	return nil
}

func (p *DiskProspector) Run() {
	logp.Debug("prospector", "Start next scan")

	p.scan()
}

// Scan starts a scanGlob for each provided path/glob
func (p *DiskProspector) scan() {
	p.disk.BuildIndex()
}
