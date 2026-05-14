package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	var cliFilters cliFilterValues

	mode := flag.String("mode", "http", "run mode: http or export")
	exportType := flag.String("type", exportMemberOrder, "export type, currently supports member_order/member/member-orders")
	envFile := flag.String("env-file", ".env", "env file path")
	envName := flag.String("env", "", "environment name, overrides APP_ENV")
	addr := flag.String("addr", "", "http listen address, overrides APP_ADDR")
	output := flag.String("output", "", "csv output path when mode=export")
	query := flag.String("query", "", "URL query string filters, for example 'time_options=payment_at&time_keyword[]=2025-05-01&time_keyword[]=2026-04-30'")
	params := flag.String("params", "", "JSON object filters, for example '{\"options\":\"mobile\",\"keyword\":\"138\"}'")
	flag.Var(&cliFilters, "filter", "export filter as key=value, repeatable. Example: -filter time_options=payment_at -filter time_keyword[]=2025-05-01")
	flag.Parse()

	if err := loadEnvFile(*envFile); err != nil {
		log.Fatalf("load env failed: %v", err)
	}

	if strings.TrimSpace(*envName) != "" {
		_ = os.Setenv("APP_ENV", strings.TrimSpace(*envName))
	}

	cfg, err := loadConfigFromEnv()
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	runtime, err := buildRuntime(cfg)
	if err != nil {
		log.Fatalf("init runtime failed: %v", err)
	}
	defer runtime.Close()

	queryTimeout, err := time.ParseDuration(firstNonEmpty(cfg.Export.QueryTimeout, "5m"))
	if err != nil {
		log.Fatalf("invalid EXPORT_QUERY_TIMEOUT: %v", err)
	}

	app := newApp(runtime, cfg.Export.MaxConcurrency, cfg.Export.MaxRetries, queryTimeout)
	filterValues, err := filtersFromCLI(*query, *params, cliFilters)
	if err != nil {
		log.Fatalf("parse filters failed: %v", err)
	}

	switch *mode {
	case "export":
		exporter, err := app.registry.Resolve(*exportType)
		if err != nil {
			log.Fatal(err)
		}
		outputPath := strings.TrimSpace(*output)
		if outputPath == "" {
			outputPath = defaultOutputPath(cfg.Export.OutputDir, exporter.Type())
		}
		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
		defer cancel()
		total, err := exportCSVFile(ctx, outputPath, exporter, filterValues)
		if err != nil {
			log.Fatalf("export failed: %v", err)
		}
		slog.Info("cli export completed", "export_type", exporter.Type(), "status", "completed", "output", outputPath, "total", total, "source", runtime.SourceName)
	case "http":
		listenAddr := firstNonEmpty(strings.TrimSpace(*addr), cfg.Server.Addr, ":8088")
		mux := app.routes(cfg)
		slog.Info("export service listening", "addr", listenAddr, "source", runtime.SourceName)
		if err := http.ListenAndServe(listenAddr, mux); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unsupported mode: %s", *mode)
	}
}
