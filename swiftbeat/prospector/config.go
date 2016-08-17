package prospector

import (
	"fmt"
	"regexp"
	"time"

	cfg "github.com/elastic/beats/swiftbeat/config"
)

var (
	defaultConfig = prospectorConfig{
		IgnoreOlder:   0,
		ScanFrequency: 10 * time.Second,
		ResourceType:  cfg.DefaultResourceType,
		CleanInactive: 0,
		CleanRemoved:  false,
	}
)

type prospectorConfig struct {
	ExcludeFiles  []*regexp.Regexp `config:"exclude_files"`
	IgnoreOlder   time.Duration    `config:"ignore_older"`
	Paths         []string         `config:"paths"`
	ScanFrequency time.Duration    `config:"scan_frequency" validate:"min=0,nonzero"`
	ResourceType  string           `config:"resource_type"`
	CleanInactive time.Duration    `config:"clean_inactive" validate:"min=0"`
	CleanRemoved  bool             `config:"clean_removed"`
}

func (config *prospectorConfig) Validate() error {

	if config.ResourceType == cfg.ObjectResourceType && len(config.Paths) == 0 {
		return fmt.Errorf("No paths were defined for prospector")
	}

	if config.CleanInactive != 0 && config.IgnoreOlder == 0 {
		return fmt.Errorf("ignore_older must be enabled when clean_older is used.")
	}

	if config.CleanInactive != 0 && config.CleanInactive <= config.IgnoreOlder+config.ScanFrequency {
		return fmt.Errorf("clean_older must be > ignore_older + scan_frequency to make sure only files which are not monitored anymore are removed.")
	}

	return nil
}
