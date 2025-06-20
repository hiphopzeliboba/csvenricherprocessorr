package csvenricherprocessor

import (
	"context"
	"fmt"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

var (
	strType = component.MustNewType("csvenricherprocessor")
)

const (
	// The stability level of the processor.
	stability = component.StabilityLevelBeta
)

// NewFactory returns a new factory for the CSV Enricher processor.
func NewFactory() processor.Factory {

	return processor.NewFactory(
		strType,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		MatchField:    "slug",                   // default match field
		EnrichColumns: []string{"id", "obj_id"}, // default columns to enrich
	}
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {

	processorCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}

	// Create the processor
	proc, err := newProcessor(processorCfg, set.Logger)
	if err != nil {
		return nil, fmt.Errorf("cannot create csv_enricher processor: %w", err)
	}

	set.Logger.Info("CSV Enricher Reload Interval", zap.Duration("reload_interval", processorCfg.ReloadInterval))

	return processorhelper.NewTraces(
		ctx,
		set,
		cfg,
		nextConsumer,
		proc.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(proc.start),
		processorhelper.WithShutdown(proc.shutdown),
	)
}
