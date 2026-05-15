package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	callbackpkg "export-service/internal/callback"
	configpkg "export-service/internal/config"
	exportpkg "export-service/internal/export"
	"export-service/internal/exporters/memberorder"
	"export-service/internal/storage"
	taskpkg "export-service/internal/task"

	_ "github.com/go-sql-driver/mysql"
)

type Runtime struct {
	MemberOrders   memberorder.MemberOrderRepository
	Registry       *exportpkg.ExportRegistry
	TaskManager    *taskpkg.TaskManager
	Executor       *exportpkg.ExportExecutor
	TaskStore      taskpkg.TaskStore
	OSSUploader    exportpkg.Uploader
	CallbackClient exportpkg.CallbackClient
	SourceName     string
	OutputDir      string
	OSSExpireDays  int
	OSSMaxRetries  int
	Close          func()

	callbackCancel context.CancelFunc
}

func BuildRuntime(cfg configpkg.Config) (*Runtime, error) {
	queryTimeout, err := time.ParseDuration(firstNonEmpty(cfg.Export.QueryTimeout, "5m"))
	if err != nil {
		return nil, fmt.Errorf("invalid EXPORT_QUERY_TIMEOUT: %w", err)
	}

	batchSize := cfg.Export.MySQLBatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	if !cfg.Database.HasConnection() {
		if !cfg.UseMemoryWhenNoDB {
			return nil, fmt.Errorf("APP_ENV=%q has no database config", cfg.AppEnv)
		}
		seedSize := cfg.Export.SeedSize
		if seedSize <= 0 {
			seedSize = 30000
		}
		runtime := &Runtime{
			MemberOrders:  memberorder.NewMemoryRepository(seedSize),
			SourceName:    fmt.Sprintf("memory(%d)", seedSize),
			OutputDir:     firstNonEmpty(cfg.Export.OutputDir, "./exports"),
			OSSExpireDays: cfg.OSS.URLExpireDays,
			OSSMaxRetries: cfg.Export.OSSMaxRetries,
			Close:         func() {},
		}
		return runtime.withComponents(cfg), nil
	}

	db, err := sql.Open("mysql", cfg.Database.BuildDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(defaultInt(cfg.Database.MaxOpenConns, 10))
	db.SetMaxIdleConns(defaultInt(cfg.Database.MaxIdleConns, 5))
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	tables := buildTableNames(firstNonEmpty(cfg.Database.TablePrefix, "h_"))
	taskStore := taskpkg.NewMySQLStore(db)
	if err := taskStore.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	runtime := &Runtime{
		MemberOrders:  memberorder.NewMySQLRepository(db, batchSize, tables),
		TaskStore:     taskStore,
		SourceName:    fmt.Sprintf("mysql(env=%s,database=%s,batch=%d)", cfg.AppEnv, cfg.Database.Database, batchSize),
		OutputDir:     firstNonEmpty(cfg.Export.OutputDir, "./exports"),
		OSSExpireDays: cfg.OSS.URLExpireDays,
		OSSMaxRetries: cfg.Export.OSSMaxRetries,
		Close:         func() { _ = db.Close() },
	}
	return runtime.withComponents(cfg), nil
}

func (r *Runtime) withComponents(cfg configpkg.Config) *Runtime {
	r.OSSUploader = storage.NewAliyunOSSUploader(cfg.OSS)
	r.CallbackClient = callbackpkg.NewHTTPClient()
	r.Registry = exportpkg.NewRegistry()
	r.Registry.Register(memberorder.NewExporter(r.MemberOrders))
	r.TaskManager = taskpkg.NewManager(r.OutputDir, r.TaskStore)
	r.Executor = exportpkg.NewExecutor(r.TaskManager, r.OSSUploader, r.CallbackClient, r.OSSMaxRetries, r.OSSExpireDays)
	r.startCallbackCompensator(cfg)
	return r
}

func (r *Runtime) startCallbackCompensator(cfg configpkg.Config) {
	interval, err := time.ParseDuration(firstNonEmpty(cfg.Export.CallbackRetryInterval, "1m"))
	if err != nil {
		interval = time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.callbackCancel = cancel
	previousClose := r.Close
	r.Close = func() {
		cancel()
		if previousClose != nil {
			previousClose()
		}
	}
	r.Executor.StartCallbackCompensator(ctx, interval, cfg.Export.CallbackMaxRetries)
}

func buildTableNames(prefix string) memberorder.TableNames {
	return memberorder.TableNames{
		Orders:            prefix + "orders",
		ViewUsers:         prefix + "view_users",
		OrderInstallments: prefix + "order_installments",
		OrderItems:        prefix + "order_items",
		ViewServiceVIPs:   prefix + "view_service_vips",
		MarketChannels:    prefix + "view_market_channels",
		MarketLinks:       prefix + "market_links",
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func defaultInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
