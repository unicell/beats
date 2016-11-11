package prospector

import (
	"io/ioutil"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/swiftbeat/input"
)

type Prospector struct {
	cfg           *common.Config // Raw config
	config        prospectorConfig
	prospectorers []Prospectorer
	spoolerChan   chan input.Event
	harvesterChan chan input.Event
	done          chan struct{}
	states        *input.States
	wg            sync.WaitGroup
}

type Prospectorer interface {
	Init() error
	Run()
}

func NewProspector(cfg *common.Config, states input.States, spoolerChan chan input.Event) (*Prospector, error) {
	prospector := &Prospector{
		cfg:           cfg,
		config:        defaultConfig,
		spoolerChan:   spoolerChan,
		harvesterChan: make(chan input.Event),
		done:          make(chan struct{}),
		states:        states.Copy(),
		wg:            sync.WaitGroup{},
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
		prospectorer := NewDiskProspector(p, name, path)

		err := prospectorer.Init()
		if err != nil {
			logp.Warn("Prospector: failed to initialize prospector for: %s", path)
			continue
		}

		p.prospectorers = append(p.prospectorers, prospectorer)
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

				// Add ttl if RescanOlder is enabled
				if p.config.RescanOlder > 0 {
					event.SetTTL(p.config.RescanOlder)
				}

				if !p.states.IsNewEvent(event) {
					continue
				}

				//part := event.ToPartition()
				//logp.Debug("hack", "66--> : %s - %s", event.ToMapStr()["path"], part.Mtime)

				select {
				case <-p.done:
					logp.Info("Prospector channel stopped")
					return
				case p.spoolerChan <- event:
					p.states.Update(event)
				}
			}
		}
	}()

	// Initial prospector run
	for _, prospectorer := range p.prospectorers {
		prospectorer.Run()
	}

	for {
		select {
		case <-p.done:
			logp.Info("Prospector ticker stopped")
			return
		case <-time.After(p.config.ScanFrequency):
			// force GC and return memory to OS to keep low mem profile
			debug.FreeOSMemory()
			logp.Debug("prospector", "Run prospector")
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
