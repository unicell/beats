package registrar

import (
	"encoding/json"
	"expvar"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/paths"
	"github.com/elastic/beats/swiftbeat/input"
)

type Registrar struct {
	Channel      chan []input.Event
	done         chan struct{}
	registryFile string        // Path to the Registry File
	states       *input.States // Map of states fo all resources
	wg           sync.WaitGroup
}

var (
	statesUpdated = expvar.NewInt("registrar.state_updates")
	statesTotal   = expvar.NewInt("registar.states.total")
)

func New(registryFile string) (*Registrar, error) {

	r := &Registrar{
		registryFile: registryFile,
		done:         make(chan struct{}),
		states:       input.NewStates(),
		Channel:      make(chan []input.Event, 1),
		wg:           sync.WaitGroup{},
	}
	err := r.Init()

	return r, err
}

// Init sets up the Registrar and make sure the registry file is setup correctly
func (r *Registrar) Init() error {

	// The registry file is opened in the data path
	r.registryFile = paths.Resolve(paths.Data, r.registryFile)

	// Create directory if it does not already exist.
	registryPath := filepath.Dir(r.registryFile)
	err := os.MkdirAll(registryPath, 0755)
	if err != nil {
		return fmt.Errorf("Failed to created registry file dir %s: %v",
			registryPath, err)
	}

	logp.Info("Registry file set to: %s", r.registryFile)

	return nil
}

// GetStates return the registrar states
func (r *Registrar) GetStates() input.States {
	return *r.states
}

// loadStates fetches the previous reading state from the configure RegistryFile file
// The default file is `registry` in the data path.
func (r *Registrar) loadStates() error {

	// Check if files exists
	_, err := os.Stat(r.registryFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Error means no file found
	if err != nil {
		logp.Info("No registry file found under: %s. Creating a new registry file.", r.registryFile)
		return nil
	}

	f, err := os.Open(r.registryFile)
	if err != nil {
		return err
	}

	defer f.Close()

	logp.Info("Loading registrar data from %s", r.registryFile)

	var states map[string]*input.DiskState
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&states)
	if err != nil {
		logp.Err("Error decoding states: %s", err)
		return err
	}

	r.states.SetStates(states)
	logp.Info("States Loaded from registrar: %+v", len(states))

	return nil
}

func (r *Registrar) Start() error {

	// Load the previous log file locations now, for use in prospector
	err := r.loadStates()
	if err != nil {
		logp.Err("Error loading state: %v", err)
		return err
	}

	r.wg.Add(1)
	go r.Run()

	return nil
}

func (r *Registrar) Run() {
	logp.Info("Starting Registrar")
	// Writes registry on shutdown
	defer func() {
		r.writeRegistry()
		r.wg.Done()
	}()

	for {
		select {
		case <-r.done:
			logp.Info("Ending Registrar")
			return
		case events := <-r.Channel:
			r.processEventStates(events)
		}

		// TODO
		//beforeCount := r.states.Count()
		//r.states.Cleanup()
		//logp.Debug("registrar", "Registrar states cleaned up. Before: %d , After: %d", beforeCount, r.states.Count())
		//if err := r.writeRegistry(); err != nil {
		//logp.Err("Writing of registry returned error: %v. Continuing...", err)
		//}
	}
}

// processEventStates gets the states from the events and writes them to the registrar state
func (r *Registrar) processEventStates(events []input.Event) {
	logp.Debug("registrar", "Processing %d events", len(events))

	// Take the last event found for each file source
	for _, event := range events {
		//part := event.ToPartition()
		//logp.Debug("hack", "33--> : %s - %s", event.ToMapStr()["path"], part.Mtime)
		r.states.Update(event)
	}
}

// Stop stops the registry. It waits until Run function finished.
func (r *Registrar) Stop() {
	logp.Info("Stopping Registrar")
	close(r.done)
	r.wg.Wait()
}

// writeRegistry writes the new json registry file to disk.
func (r *Registrar) writeRegistry() error {
	logp.Debug("registrar", "Write registry file: %s", r.registryFile)

	tempfile := r.registryFile + ".new"
	f, err := os.Create(tempfile)
	if err != nil {
		logp.Err("Failed to create tempfile (%s) for writing: %s", tempfile, err)
		return err
	}

	// First clean up states
	states := r.states.GetStatesCopy()

	encoder := json.NewEncoder(f)
	err = encoder.Encode(states)
	if err != nil {
		logp.Err("Error when encoding the states: %s", err)
		return err
	}

	// Directly close file because of windows
	f.Close()

	logp.Debug("registrar", "Registry file updated. %d states written.", len(states))
	statesUpdated.Add(int64(len(states)))
	statesTotal.Set(int64(len(states)))

	return r.safeFileRotate(r.registryFile, tempfile)
}

// SafeFileRotate safely rotates an existing file under path and replaces it with the tempfile
func (r *Registrar) safeFileRotate(path, tempfile string) error {
	if e := os.Rename(tempfile, path); e != nil {
		logp.Err("Rotate error: %s", e)
		return e
	}
	return nil
}
