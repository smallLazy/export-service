package main

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	AppEnv            string
	Server            ServerConfig
	Export            ExportConfig
	Database          DatabaseConfig
	OSS               OSSConfig
	UseMemoryWhenNoDB bool
}

type ServerConfig struct {
	Addr string
}

type ExportConfig struct {
	OutputDir      string
	QueryTimeout   string
	MySQLBatchSize int
	SeedSize       int
	MaxConcurrency int
	MaxRetries     int
	OSSMaxRetries  int
}

type OSSConfig struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
	BasePath        string
	CDNDomain       string
	RoleSessionName string
	RoleARN         string
	URLExpireDays   int
}

type DatabaseConfig struct {
	DSN          string
	Host         string
	Port         int
	Database     string
	Username     string
	Password     string
	Charset      string
	ParseTime    bool
	Loc          string
	TablePrefix  string
	MaxOpenConns int
	MaxIdleConns int
}

func loadEnvFile(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, value)
	}
	return scanner.Err()
}

func loadConfigFromEnv() (Config, error) {
	appEnv := envString("APP_ENV", "local")
	cfg := Config{
		AppEnv: appEnv,
		Server: ServerConfig{
			Addr: envString("APP_ADDR", ":8088"),
		},
		Export: ExportConfig{
			OutputDir:      envString("EXPORT_OUTPUT_DIR", "./exports"),
			QueryTimeout:   envString("EXPORT_QUERY_TIMEOUT", "5m"),
			MySQLBatchSize: envInt("EXPORT_MYSQL_BATCH_SIZE", 1000),
			SeedSize:       envInt("EXPORT_SEED_SIZE", 30000),
			MaxConcurrency: envInt("EXPORT_MAX_CONCURRENCY", 3),
			MaxRetries:     envInt("EXPORT_MAX_RETRIES", 2),
			OSSMaxRetries:  envInt("EXPORT_OSS_MAX_RETRIES", 3),
		},
		OSS: OSSConfig{
			Endpoint:        envString("OSS_ENDPOINT", ""),
			Bucket:          envString("OSS_BUCKET", ""),
			AccessKeyID:     envString("OSS_ACCESS_KEY_ID", ""),
			AccessKeySecret: envString("OSS_ACCESS_KEY_SECRET", ""),
			BasePath:        envString("OSS_BASE_PATH", "exports"),
			CDNDomain:       envString("OSS_CDN_DOMAIN", ""),
			RoleSessionName: envString("OSS_ROLE_SESSION_NAME", ""),
			RoleARN:         envString("OSS_ROLE_ARN", ""),
			URLExpireDays:   envInt("OSS_URL_EXPIRE_DAYS", 7),
		},
		UseMemoryWhenNoDB: envBool("EXPORT_USE_MEMORY_WHEN_NO_DB", true),
		Database: DatabaseConfig{
			DSN:          envFor(appEnv, "DB_DSN", ""),
			Host:         envFor(appEnv, "DB_HOST", "127.0.0.1"),
			Port:         envIntFor(appEnv, "DB_PORT", 3306),
			Database:     firstNonEmpty(envFor(appEnv, "TRADE_DB_DATABASE", ""), envFor(appEnv, "DB_DATABASE", "")),
			Username:     envFor(appEnv, "DB_USERNAME", ""),
			Password:     envFor(appEnv, "DB_PASSWORD", ""),
			Charset:      envFor(appEnv, "DB_CHARSET", "utf8mb4"),
			ParseTime:    envBoolFor(appEnv, "DB_PARSE_TIME", true),
			Loc:          envFor(appEnv, "DB_LOC", "Local"),
			TablePrefix:  firstNonEmpty(envFor(appEnv, "DB_TABLE_PREFIX", ""), envFor(appEnv, "DB_PREFIX", "h_")),
			MaxOpenConns: envIntFor(appEnv, "DB_MAX_OPEN_CONNS", 10),
			MaxIdleConns: envIntFor(appEnv, "DB_MAX_IDLE_CONNS", 5),
		},
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.AppEnv) == "" {
		return fmt.Errorf("APP_ENV is required")
	}
	if strings.TrimSpace(c.Server.Addr) == "" {
		return fmt.Errorf("APP_ADDR is required")
	}
	if strings.TrimSpace(c.Export.OutputDir) == "" {
		return fmt.Errorf("EXPORT_OUTPUT_DIR is required")
	}
	if strings.TrimSpace(c.Export.QueryTimeout) == "" {
		return fmt.Errorf("EXPORT_QUERY_TIMEOUT is required")
	}
	if c.Export.MySQLBatchSize <= 0 {
		return fmt.Errorf("EXPORT_MYSQL_BATCH_SIZE must be greater than 0")
	}
	if c.Export.MaxConcurrency < 0 {
		return fmt.Errorf("EXPORT_MAX_CONCURRENCY must be greater than or equal to 0")
	}
	if c.Export.MaxRetries < 0 {
		return fmt.Errorf("EXPORT_MAX_RETRIES must be greater than or equal to 0")
	}
	if c.Export.OSSMaxRetries < 0 {
		return fmt.Errorf("EXPORT_OSS_MAX_RETRIES must be greater than or equal to 0")
	}
	if c.OSS.Enabled() {
		if strings.TrimSpace(c.OSS.Endpoint) == "" {
			return fmt.Errorf("OSS_ENDPOINT is required when OSS is enabled")
		}
		if strings.TrimSpace(c.OSS.Bucket) == "" {
			return fmt.Errorf("OSS_BUCKET is required when OSS is enabled")
		}
		if strings.TrimSpace(c.OSS.AccessKeyID) == "" {
			return fmt.Errorf("OSS_ACCESS_KEY_ID is required when OSS is enabled")
		}
		if strings.TrimSpace(c.OSS.AccessKeySecret) == "" {
			return fmt.Errorf("OSS_ACCESS_KEY_SECRET is required when OSS is enabled")
		}
		if c.OSS.URLExpireDays <= 0 {
			return fmt.Errorf("OSS_URL_EXPIRE_DAYS must be greater than 0")
		}
	}
	if c.Database.HasConnection() {
		if strings.TrimSpace(c.Database.DSN) == "" {
			if strings.TrimSpace(c.Database.Host) == "" {
				return fmt.Errorf("DB_HOST is required when DB_DSN is not set")
			}
			if c.Database.Port <= 0 {
				return fmt.Errorf("DB_PORT must be greater than 0")
			}
			if strings.TrimSpace(c.Database.Database) == "" {
				return fmt.Errorf("DB_DATABASE is required when DB_DSN is not set")
			}
			if strings.TrimSpace(c.Database.Username) == "" {
				return fmt.Errorf("DB_USERNAME is required when DB_DSN is not set")
			}
		}
		if c.Database.MaxOpenConns < 0 {
			return fmt.Errorf("DB_MAX_OPEN_CONNS must be greater than or equal to 0")
		}
		if c.Database.MaxIdleConns < 0 {
			return fmt.Errorf("DB_MAX_IDLE_CONNS must be greater than or equal to 0")
		}
	}
	if !c.Database.HasConnection() && !c.UseMemoryWhenNoDB {
		return fmt.Errorf("database config is required when EXPORT_USE_MEMORY_WHEN_NO_DB=false")
	}
	return nil
}

func (c OSSConfig) Enabled() bool {
	return strings.TrimSpace(c.Endpoint) != "" ||
		strings.TrimSpace(c.Bucket) != "" ||
		strings.TrimSpace(c.AccessKeyID) != "" ||
		strings.TrimSpace(c.AccessKeySecret) != ""
}

func (c DatabaseConfig) HasConnection() bool {
	return strings.TrimSpace(c.DSN) != "" || (strings.TrimSpace(c.Host) != "" && strings.TrimSpace(c.Database) != "" && strings.TrimSpace(c.Username) != "")
}

func (c DatabaseConfig) BuildDSN() string {
	if strings.TrimSpace(c.DSN) != "" {
		return c.DSN
	}
	params := url.Values{}
	params.Set("charset", firstNonEmpty(c.Charset, "utf8mb4"))
	params.Set("parseTime", strconv.FormatBool(c.ParseTime))
	params.Set("loc", firstNonEmpty(c.Loc, "Local"))
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?%s",
		c.Username,
		c.Password,
		net.JoinHostPort(firstNonEmpty(c.Host, "127.0.0.1"), strconv.Itoa(defaultInt(c.Port, 3306))),
		c.Database,
		params.Encode(),
	)
}

func envFor(appEnv, key, fallback string) string {
	prefixed := strings.ToUpper(appEnv) + "_" + key
	return envString(prefixed, envString(key, fallback))
}

func envIntFor(appEnv, key string, fallback int) int {
	prefixed := strings.ToUpper(appEnv) + "_" + key
	return envInt(prefixed, envInt(key, fallback))
}

func envBoolFor(appEnv, key string, fallback bool) bool {
	prefixed := strings.ToUpper(appEnv) + "_" + key
	return envBool(prefixed, envBool(key, fallback))
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
