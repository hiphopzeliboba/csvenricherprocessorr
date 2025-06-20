package csvenricherprocessor

import (
	"errors"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"time"
)

// Config defines configuration for CSV Enricher processor.
type Config struct {
	// Path to the CSV file
	CSVPath string `mapstructure:"csv_path"`

	// Field in span attributes to match against CSV's slug column
	MatchField string `mapstructure:"match_field"`

	// Columns from CSV to add as span attributes
	EnrichColumns []string `mapstructure:"enrich_columns"`

	//DefaultSlug string `mapstructure:"default_slug"`

	// Time interval for update CSV file with new values
	ReloadInterval time.Duration `mapstructure:"reload_interval"`
}

// Validate checks that all required configuration fields are set correctly.
func (cfg *Config) Validate() error {
	if cfg.CSVPath == "" {
		return errors.New("missing required field: csv_path")
	}

	if cfg.MatchField == "" {
		return errors.New("missing required field: match_field")
	}

	if len(cfg.EnrichColumns) == 0 {
		return errors.New("missing required field: enrich_columns")
	}

	//if cfg.DefaultSlug == "" {
	//	return errors.New("missing required field: default_slug")
	//}

	if cfg.ReloadInterval < 0 {
		return fmt.Errorf("reload_interval must be non-negative, got: %s", cfg.ReloadInterval)
	}

	return nil
}

var _ component.Config = (*Config)(nil)
