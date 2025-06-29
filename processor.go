package csvenricherprocessor

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type csvEnricherProcessor struct {
	logger     *zap.Logger
	config     *Config
	csvData    []map[string]string
	matchIndex map[string]int // maps value to index in csvData
	mu         sync.RWMutex

	ticker   *time.Ticker
	stopChan chan struct{}
}

func newProcessor(cfg *Config, logger *zap.Logger) (*csvEnricherProcessor, error) {
	p := &csvEnricherProcessor{
		logger:     logger,
		config:     cfg,
		matchIndex: make(map[string]int),
		stopChan:   make(chan struct{}),
	}

	// Load CSV data on creation
	if err := p.loadCSVData(); err != nil {
		return nil, fmt.Errorf("failed to load CSV data: %w", err)
	}

	return p, nil
}

func (p *csvEnricherProcessor) start(ctx context.Context, host component.Host) error {
	p.logger.Info("Starting CSV Enricher Processor")

	// Если интервал <= 0, просто загружаем один раз и выходим
	if p.config.ReloadInterval <= 0 {
		p.logger.Info("Reload interval not set or <= 0 — CSV will be loaded only once")
		err := p.loadCSVData()
		if err != nil {
			p.logger.Error("Failed to load CSV data on startup", zap.Error(err))
			return err
		}
		return nil
	}

	p.logger.Info("Starting background CSV reload loop", zap.Duration("interval", p.config.ReloadInterval))
	go func() {
		ticker := time.NewTicker(p.config.ReloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.logger.Info("Reloading CSV data")
				if err := p.loadCSVData(); err != nil {
					p.logger.Warn("Failed to reload CSV data", zap.Error(err))
				}
			case <-ctx.Done():
				p.logger.Info("CSV reload context done, stopping reload loop")
				return
			}
		}
	}()

	return nil
}

func (p *csvEnricherProcessor) shutdown(ctx context.Context) error {
	p.logger.Info("Shutting down CSV Enricher Processor")

	if p.ticker != nil {
		p.ticker.Stop()
	}
	close(p.stopChan)

	return nil
}

func (p *csvEnricherProcessor) loadCSVData() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, err := os.Open(p.config.CSVPath)
	if err != nil {
		return fmt.Errorf("error opening CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error reading CSV file: %w", err)
	}

	if len(records) == 0 {
		return errors.New("CSV file is empty")
	}

	// Get header
	headers := records[0]
	matchCol := -1
	for i, h := range headers {
		if h == p.config.MatchField {
			matchCol = i
			break
		}
	}
	if matchCol == -1 {
		return fmt.Errorf("CSV file must contain column matching 'match_field': %q", p.config.MatchField)
	}

	// Process records
	p.csvData = make([]map[string]string, 0, len(records)-1)
	p.matchIndex = make(map[string]int)

	for i, record := range records[1:] {
		if len(record) != len(headers) {
			p.logger.Warn("CSV record has wrong number of fields", zap.Int("line", i+2))
			continue
		}

		row := make(map[string]string)
		for j, header := range headers {
			row[header] = record[j]
		}

		matchValue := record[matchCol]
		if matchValue == "" {
			p.logger.Warn("Empty match field value in CSV record", zap.Int("line", i+2), zap.String("field", p.config.MatchField))
			continue
		}

		p.csvData = append(p.csvData, row)
		p.matchIndex[matchValue] = len(p.csvData) - 1
	}

	p.logger.Info("Loaded CSV data", zap.Int("records", len(p.csvData)))
	return nil
}

func (p *csvEnricherProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		resourceSpan := resourceSpans.At(i)

		// --- добавлено: enrichment на уровне resource
		resourceAttrs := resourceSpan.Resource().Attributes()
		p.enrichResource(resourceAttrs)

		// --- удалено: enrichment на уровне span
		/*
			ilSpans := resourceSpan.ScopeSpans()
			for j := 0; j < ilSpans.Len(); j++ {
				spans := ilSpans.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					span := spans.At(k)
					p.enrichSpan(span)
				}
			}
		*/

	}
	return td, nil
}

// --- добавлено: новая функция, обогащает Resource.Attributes
func (p *csvEnricherProcessor) enrichResource(resourceAttrs pcommon.Map) {
	matchValue, exists := resourceAttrs.Get(p.config.MatchField)
	if !exists || matchValue.Type() != pcommon.ValueTypeStr {
		return
	}

	matchKey := matchValue.Str()
	if matchKey == "" {
		return
	}

	recordIdx, found := p.matchIndex[matchKey]
	if !found {
		p.logger.Debug("No matching value found in CSV", zap.String("value for match", matchKey))
		return
	}

	record := p.csvData[recordIdx]

	for _, column := range p.config.EnrichColumns {
		if value, ok := record[column]; ok {
			resourceAttrs.PutStr(column, value)
		} else {
			p.logger.Warn("Column not found in CSV record", zap.String("column", column))
		}
	}
}

// --- deprecated: функция обогащения Span.Attributes
func (p *csvEnricherProcessor) enrichSpan(span ptrace.Span) {
	attrs := span.Attributes()

	// Get the match value from span attributes
	matchValue, exists := attrs.Get(p.config.MatchField)
	if !exists {
		p.logger.Debug("Span does not contain match field", zap.String("field", p.config.MatchField))
		return
	}

	if matchValue.Type() != pcommon.ValueTypeStr {
		p.logger.Debug("Match field is not a string", zap.String("field", p.config.MatchField))
		return
	}

	matchKey := matchValue.Str()
	if matchKey == "" {
		p.logger.Debug("Match field is empty", zap.String("field", p.config.MatchField))
		return
	}

	// Find matching record in CSV
	recordIdx, found := p.matchIndex[matchKey]
	if !found {
		p.logger.Debug("No matching slug found in CSV", zap.String("match_value", matchKey))
		return
	}

	record := p.csvData[recordIdx]

	// Add specified columns as attributes
	for _, column := range p.config.EnrichColumns {
		if value, ok := record[column]; ok {
			attrs.PutStr(column, value)
		} else {
			p.logger.Warn("Column not found in CSV record", zap.String("column", column))
		}
	}
}
