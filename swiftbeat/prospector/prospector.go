package prospector

import (
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"

	//swift "github.com/openstack/swift/go/hummingbird"

	//"github.com/elastic/beats/filebeat/harvester"
	"github.com/elastic/beats/swiftbeat/input"
	//"github.com/elastic/beats/filebeat/input/file"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
)

type Prospector struct {
	cfg           *common.Config // Raw config
	config        prospectorConfig
	prospectorers []Prospectorer
	spoolerChan   chan input.Event
	harvesterChan chan input.Event
	done          chan struct{}
	//states        *file.States
	wg sync.WaitGroup
}

type Prospectorer interface {
	Init() error
	Run()
}

// TODO
//func NewProspector(cfg *common.Config, states file.States, spoolerChan chan *input.Event) (*Prospector, error) {
func NewProspector(cfg *common.Config, spoolerChan chan input.Event) (*Prospector, error) {
	prospector := &Prospector{
		cfg:           cfg,
		config:        defaultConfig,
		spoolerChan:   spoolerChan,
		harvesterChan: make(chan input.Event),
		done:          make(chan struct{}),
		//states:        states.Copy(),
		wg: sync.WaitGroup{},
	}

	if err := cfg.Unpack(&prospector.config); err != nil {
		return nil, err
	}
	if err := prospector.config.Validate(); err != nil {
		return nil, err
	}

	err := prospector.Init()
	if err != nil {
		return nil, err
	}

	logp.Debug("prospector", "Device Configs: %v", prospector.config.DeviceDir)

	return prospector, nil
}

// Init sets up default config for prospector
func (p *Prospector) Init() error {

	files, err := ioutil.ReadDir(p.config.DeviceDir)
	if err != nil {
		logp.Err("list dir(%s) failed: %v", p.config.DeviceDir, err)
		return err
	}

	for _, file := range files {
		name := file.Name()
		path := filepath.Join(p.config.DeviceDir, name)
		//if mounted, _ := swift.IsMount(path); mounted {
		prospectorer := NewDiskProspector(p, name, path)

		err := prospectorer.Init()
		if err != nil {
			logp.Warn("Prospector: failed to initialize prospector for: %s", path)
			continue
		}

		p.prospectorers = append(p.prospectorers, prospectorer)
		//} else {
		//logp.Warn("Prospector: device path: %s not mounted", path)
		//continue
		//}
	}

	// Create empty harvester to check if configs are fine
	//_, err = p.createHarvester(file.State{})
	//if err != nil {
	//return err
	//}

	return nil
}

// Starts scanning through all the file paths and fetch the related files. Start a harvester for each file
func (p *Prospector) Run() {

	logp.Info("Starting prospector")
	p.wg.Add(2)
	defer p.wg.Done()

	// Open channel to receive events from harvester and forward them to spooler
	// Here potential filtering can happen
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-p.done:
				logp.Info("Prospector channel stopped")
				return
			case event := <-p.harvesterChan:
				//Add ttl if cleanOlder is enabled
				//if p.config.CleanInactive > 0 {
				//event.State.TTL = p.config.CleanInactive
				//}
				select {
				case <-p.done:
					logp.Info("Prospector channel stopped")
					return
				case p.spoolerChan <- event:
					//p.states.Update(event.State)
				}
			}
		}
	}()

	// Initial prospector run
	for _, prospectorer := range p.prospectorers {
		prospectorer.Run()
	}

	// TODO
	return

	for {
		select {
		case <-p.done:
			logp.Info("Prospector ticker stopped")
			return
		case <-time.After(p.config.ScanFrequency):
			logp.Debug("prospector", "Run prospector")
			// TODO
			for _, prospectorer := range p.prospectorers {
				prospectorer.Run()
			}
		}
	}
}

func (p *Prospector) Stop() {
	logp.Info("Stopping Prospector")
	close(p.done)
	p.wg.Wait()
}

// createHarvester creates a new harvester instance from the given state
//func (p *Prospector) createHarvester(state file.State) (*harvester.Harvester, error) {

//h, err := harvester.NewHarvester(
//p.cfg,
//state,
//p.harvesterChan,
//p.done,
//)

//return h, err
//}

//func (p *Prospector) startHarvester(state file.State, offset int64) (*harvester.Harvester, error) {
//state.Offset = offset
//// Create harvester with state
//h, err := p.createHarvester(state)
//if err != nil {
//return nil, err
//}

//p.wg.Add(1)
//go func() {
//defer p.wg.Done()
//// Starts harvester and picks the right type. In case type is not set, set it to defeault (log)
//h.Harvest()
//}()

//return h, nil
//}
