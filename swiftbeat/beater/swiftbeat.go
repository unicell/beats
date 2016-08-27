package beater

import (
	"fmt"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	cfg "github.com/elastic/beats/swiftbeat/config"
	"github.com/elastic/beats/swiftbeat/crawler"
	"github.com/elastic/beats/swiftbeat/input"
	"github.com/elastic/beats/swiftbeat/publish"
	//"github.com/elastic/beats/filebeat/registrar"
	"github.com/elastic/beats/swiftbeat/spooler"
)

// Swiftbeat is a beater object. Contains all objects needed to run the beat
type Swiftbeat struct {
	config *cfg.Config
	done   chan struct{}
}

// New creates a new Swiftbeat pointer instance.
func New(b *beat.Beat, rawConfig *common.Config) (beat.Beater, error) {
	config := cfg.DefaultConfig
	if err := rawConfig.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}
	if err := config.FetchConfigs(); err != nil {
		return nil, err
	}

	sb := &Swiftbeat{
		done:   make(chan struct{}),
		config: &config,
	}
	return sb, nil
}

// Run allows the beater to be run as a beat.
func (sb *Swiftbeat) Run(b *beat.Beat) error {
	//var err error
	config := sb.config

	// Setup registrar to persist state
	//registrar, err := registrar.New(config.RegistryFile)
	//if err != nil {
	//logp.Err("Could not init registrar: %v", err)
	//return err
	//}

	// Channel from harvesters to spooler
	publisherChan := make(chan []*input.Event, 1)

	// TODO
	registrarChan := make(chan []*input.Event, 1)
	// Publishes event to output
	publisher := publish.New(config.PublishAsync,
		publisherChan, registrarChan, b.Publisher)
	//publisher := publish.New(config.PublishAsync,
	//publisherChan, registrar.Channel, b.Publisher)

	// Init and Start spooler: Harvesters dump events into the spooler.
	spooler, err := spooler.New(config, publisherChan)
	if err != nil {
		logp.Err("Could not init spooler: %v", err)
		return err
	}

	crawler, err := crawler.New(spooler, config.Prospectors)
	if err != nil {
		logp.Err("Could not init crawler: %v", err)
		return err
	}

	// The order of starting and stopping is important. Stopping is inverted to the starting order.
	// The current order is: registrar, publisher, spooler, crawler
	// That means, crawler is stopped first.

	// Start the registrar
	//err = registrar.Start()
	//if err != nil {
	//logp.Err("Could not start registrar: %v", err)
	//}
	// Stopping registrar will write last state
	//defer registrar.Stop()

	// Start publisher
	publisher.Start()
	// Stopping publisher (might potentially drop items)
	defer publisher.Stop()

	// Starting spooler
	spooler.Start()
	// Stopping spooler will flush items
	defer spooler.Stop()

	// TODO
	//err = crawler.Start(registrar.GetStates())
	err = crawler.Start()
	if err != nil {
		return err
	}
	// Stop crawler -> stop prospectors -> stop harvesters
	defer crawler.Stop()

	// Blocks progressing. As soon as channel is closed, all defer statements come into play
	<-sb.done

	return nil
}

// Stop is called on exit to stop the crawling, spooling and registration processes.
func (sb *Swiftbeat) Stop() {
	logp.Info("Stopping swiftbeat")

	// Stop Swiftbeat
	close(sb.done)
}
