package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Runtime struct {
	MemberOrders   MemberOrderRepository
	TaskStore      TaskStore
	OSSUploader    OSSUploader
	CallbackClient CallbackClient
	SourceName     string
	OutputDir      string
	OSSExpireDays  int
	OSSMaxRetries  int
	Close          func()
}

type tableNames struct {
	Orders            string
	ViewUsers         string
	OrderInstallments string
	OrderItems        string
	ViewServiceVIPs   string
	MarketChannels    string
	MarketLinks       string
}

func buildRuntime(cfg Config) (*Runtime, error) {
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
		return &Runtime{
			MemberOrders:   &MemoryMemberOrderRepository{orders: seedMemberOrders(seedSize)},
			OSSUploader:    newOSSUploader(cfg.OSS),
			CallbackClient: newCallbackClient(),
			SourceName:     fmt.Sprintf("memory(%d)", seedSize),
			OutputDir:      firstNonEmpty(cfg.Export.OutputDir, "./exports"),
			OSSExpireDays:  cfg.OSS.URLExpireDays,
			OSSMaxRetries:  cfg.Export.OSSMaxRetries,
			Close:          func() {},
		}, nil
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
	taskStore := newMySQLTaskStore(db)
	if err := taskStore.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Runtime{
		MemberOrders:   &MySQLMemberOrderRepository{db: db, batchSize: batchSize, tables: tables},
		TaskStore:      taskStore,
		OSSUploader:    newOSSUploader(cfg.OSS),
		CallbackClient: newCallbackClient(),
		SourceName:     fmt.Sprintf("mysql(env=%s,database=%s,batch=%d)", cfg.AppEnv, cfg.Database.Database, batchSize),
		OutputDir:      firstNonEmpty(cfg.Export.OutputDir, "./exports"),
		OSSExpireDays:  cfg.OSS.URLExpireDays,
		OSSMaxRetries:  cfg.Export.OSSMaxRetries,
		Close:          func() { _ = db.Close() },
	}, nil
}

func buildTableNames(prefix string) tableNames {
	return tableNames{
		Orders:            prefix + "orders",
		ViewUsers:         prefix + "view_users",
		OrderInstallments: prefix + "order_installments",
		OrderItems:        prefix + "order_items",
		ViewServiceVIPs:   prefix + "view_service_vips",
		MarketChannels:    prefix + "view_market_channels",
		MarketLinks:       prefix + "market_links",
	}
}
