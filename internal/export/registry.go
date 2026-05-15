package export

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
)

type Exporter interface {
	Type() string
	Aliases() []string
	Count(ctx context.Context, params url.Values) (int64, error)
	Preview(ctx context.Context, params url.Values, limit int) ([]map[string]any, error)
	StreamCSV(ctx context.Context, target io.Writer, params url.Values) (int64, error)
	// Future Excel exports must use a streaming writer; never load all rows into memory.
}

type ExportRegistry struct {
	exporters map[string]Exporter
}

func NewRegistry() *ExportRegistry {
	return &ExportRegistry{exporters: make(map[string]Exporter)}
}

func (r *ExportRegistry) Register(exporter Exporter) {
	r.exporters[exporter.Type()] = exporter
	for _, alias := range exporter.Aliases() {
		r.exporters[NormalizeType(alias)] = exporter
	}
}

func (r *ExportRegistry) Resolve(exportType string) (Exporter, error) {
	normalized := NormalizeType(exportType)
	exporter, ok := r.exporters[normalized]
	if !ok {
		return nil, fmt.Errorf("unsupported export type %q", exportType)
	}
	return exporter, nil
}

func (r *ExportRegistry) Types() []string {
	seen := make(map[string]bool)
	types := make([]string, 0, len(r.exporters))
	for _, exporter := range r.exporters {
		if seen[exporter.Type()] {
			continue
		}
		seen[exporter.Type()] = true
		types = append(types, exporter.Type())
	}
	return types
}

func NormalizeType(exportType string) string {
	return strings.TrimSpace(strings.ToLower(strings.ReplaceAll(exportType, "-", "_")))
}
