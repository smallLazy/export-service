package task

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const exportTasksTable = "export_tasks"

type TaskStore interface {
	EnsureSchema(ctx context.Context) error
	Load(ctx context.Context) ([]*Task, error)
	Save(ctx context.Context, row taskStoreRow) error
}

type MySQLTaskStore struct {
	db *sql.DB
}

func NewMySQLStore(db *sql.DB) *MySQLTaskStore {
	return &MySQLTaskStore{db: db}
}

func (s *MySQLTaskStore) EnsureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS export_tasks (
  id VARCHAR(64) NOT NULL PRIMARY KEY,
  export_type VARCHAR(64) NOT NULL,
  filters_json JSON NOT NULL,
  status VARCHAR(32) NOT NULL,
  output_path VARCHAR(1024) NOT NULL,
  retry_count INT NOT NULL DEFAULT 0,
  max_retries INT NOT NULL DEFAULT 0,
  error_message TEXT NULL,
  user_id VARCHAR(64) NULL,
  company_uuid VARCHAR(64) NULL,
  request_id VARCHAR(128) NULL,
  file_name VARCHAR(255) NULL,
  file_size BIGINT NOT NULL DEFAULT 0,
  oss_key VARCHAR(1024) NULL,
  oss_url TEXT NULL,
  oss_status VARCHAR(32) NULL,
  oss_error TEXT NULL,
  expired_at DATETIME(6) NULL,
  callback_url VARCHAR(1024) NULL,
  callback_status VARCHAR(32) NULL,
  callback_error TEXT NULL,
  callback_retry_count INT NOT NULL DEFAULT 0,
  source_system VARCHAR(64) NULL,
  created_at DATETIME(6) NOT NULL,
  started_at DATETIME(6) NULL,
  completed_at DATETIME(6) NULL,
  updated_at DATETIME(6) NOT NULL,
  KEY idx_export_tasks_status_updated_at (status, updated_at),
  KEY idx_export_tasks_export_type_created_at (export_type, created_at),
  KEY idx_export_tasks_user_created_at (user_id, created_at),
  KEY idx_export_tasks_request_id (request_id),
  KEY idx_export_tasks_callback_status_updated_at (callback_status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	if err != nil {
		return err
	}
	columns := []string{
		"ADD COLUMN user_id VARCHAR(64) NULL",
		"ADD COLUMN company_uuid VARCHAR(64) NULL",
		"ADD COLUMN request_id VARCHAR(128) NULL",
		"ADD COLUMN file_name VARCHAR(255) NULL",
		"ADD COLUMN file_size BIGINT NOT NULL DEFAULT 0",
		"ADD COLUMN oss_key VARCHAR(1024) NULL",
		"ADD COLUMN oss_url TEXT NULL",
		"ADD COLUMN oss_status VARCHAR(32) NULL",
		"ADD COLUMN oss_error TEXT NULL",
		"ADD COLUMN expired_at DATETIME(6) NULL",
		"ADD COLUMN callback_url VARCHAR(1024) NULL",
		"ADD COLUMN callback_status VARCHAR(32) NULL",
		"ADD COLUMN callback_error TEXT NULL",
		"ADD COLUMN callback_retry_count INT NOT NULL DEFAULT 0",
		"ADD COLUMN source_system VARCHAR(64) NULL",
	}
	for _, alter := range columns {
		if err := s.ignoreDuplicateSchemaError(s.db.ExecContext(ctx, "ALTER TABLE export_tasks "+alter)); err != nil {
			return err
		}
	}
	indexes := []string{
		"ADD KEY idx_export_tasks_user_created_at (user_id, created_at)",
		"ADD KEY idx_export_tasks_request_id (request_id)",
		"ADD KEY idx_export_tasks_callback_status_updated_at (callback_status, updated_at)",
	}
	for _, alter := range indexes {
		if err := s.ignoreDuplicateSchemaError(s.db.ExecContext(ctx, "ALTER TABLE export_tasks "+alter)); err != nil {
			return err
		}
	}
	return nil
}

func (s *MySQLTaskStore) Load(ctx context.Context) ([]*Task, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, export_type, filters_json, status, output_path, retry_count, max_retries,
       error_message, user_id, company_uuid, request_id, file_name, file_size,
       oss_key, oss_url, oss_status, oss_error, expired_at, callback_url,
       callback_status, callback_error, callback_retry_count, source_system,
       created_at, started_at, completed_at
FROM export_tasks`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		var (
			task           Task
			filtersJSON    []byte
			status         string
			errorMessage   sql.NullString
			userID         sql.NullString
			companyUUID    sql.NullString
			requestID      sql.NullString
			fileName       sql.NullString
			ossKey         sql.NullString
			ossURL         sql.NullString
			ossStatus      sql.NullString
			ossError       sql.NullString
			callbackURL    sql.NullString
			callbackStatus sql.NullString
			callbackError  sql.NullString
			sourceSystem   sql.NullString
			startedAt      sql.NullTime
			completedAt    sql.NullTime
			expiredAt      sql.NullTime
		)
		if err := rows.Scan(
			&task.ID,
			&task.Type,
			&filtersJSON,
			&status,
			&task.OutputFile,
			&task.RetryCount,
			&task.MaxRetries,
			&errorMessage,
			&userID,
			&companyUUID,
			&requestID,
			&fileName,
			&task.FileSize,
			&ossKey,
			&ossURL,
			&ossStatus,
			&ossError,
			&expiredAt,
			&callbackURL,
			&callbackStatus,
			&callbackError,
			&task.CallbackRetryCount,
			&sourceSystem,
			&task.CreatedAt,
			&startedAt,
			&completedAt,
		); err != nil {
			return nil, err
		}

		task.State = TaskState(status)
		if errorMessage.Valid {
			task.Error = errorMessage.String
		}
		task.UserID = nullStringValue(userID)
		task.CompanyUUID = nullStringValue(companyUUID)
		task.RequestID = nullStringValue(requestID)
		task.FileName = nullStringValue(fileName)
		task.OSSKey = nullStringValue(ossKey)
		task.OSSURL = nullStringValue(ossURL)
		task.OSSStatus = nullStringValue(ossStatus)
		task.OSSError = nullStringValue(ossError)
		task.CallbackURL = nullStringValue(callbackURL)
		task.CallbackStatus = nullStringValue(callbackStatus)
		task.CallbackError = nullStringValue(callbackError)
		task.SourceSystem = nullStringValue(sourceSystem)
		if expiredAt.Valid {
			task.ExpiredAt = expiredAt.Time
		}
		if startedAt.Valid {
			task.StartedAt = startedAt.Time
		}
		if completedAt.Valid {
			task.CompletedAt = completedAt.Time
		}
		if err := json.Unmarshal(filtersJSON, &task.filters); err != nil {
			return nil, fmt.Errorf("decode filters for task %s: %w", task.ID, err)
		}
		task.filters = CloneFilters(task.filters)
		tasks = append(tasks, &task)
	}
	return tasks, rows.Err()
}

func (s *MySQLTaskStore) Save(ctx context.Context, row taskStoreRow) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO export_tasks (
  id, export_type, filters_json, status, output_path, retry_count, max_retries,
  error_message, user_id, company_uuid, request_id, file_name, file_size,
  oss_key, oss_url, oss_status, oss_error, expired_at, callback_url,
  callback_status, callback_error, callback_retry_count, source_system,
  created_at, started_at, completed_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  export_type = VALUES(export_type),
  filters_json = VALUES(filters_json),
  status = VALUES(status),
  output_path = VALUES(output_path),
  retry_count = VALUES(retry_count),
  max_retries = VALUES(max_retries),
  error_message = VALUES(error_message),
  user_id = VALUES(user_id),
  company_uuid = VALUES(company_uuid),
  request_id = VALUES(request_id),
  file_name = VALUES(file_name),
  file_size = VALUES(file_size),
  oss_key = VALUES(oss_key),
  oss_url = VALUES(oss_url),
  oss_status = VALUES(oss_status),
  oss_error = VALUES(oss_error),
  expired_at = VALUES(expired_at),
  callback_url = VALUES(callback_url),
  callback_status = VALUES(callback_status),
  callback_error = VALUES(callback_error),
  callback_retry_count = VALUES(callback_retry_count),
  source_system = VALUES(source_system),
  started_at = VALUES(started_at),
  completed_at = VALUES(completed_at),
  updated_at = VALUES(updated_at)`,
		row.ID,
		row.ExportType,
		row.FiltersJSON,
		row.Status,
		row.OutputPath,
		row.RetryCount,
		row.MaxRetries,
		nullableString(row.ErrorMessage),
		nullableString(row.UserID),
		nullableString(row.CompanyUUID),
		nullableString(row.RequestID),
		nullableString(row.FileName),
		row.FileSize,
		nullableString(row.OSSKey),
		nullableString(row.OSSURL),
		nullableString(row.OSSStatus),
		nullableString(row.OSSError),
		nullableTime(row.ExpiredAt),
		nullableString(row.CallbackURL),
		nullableString(row.CallbackStatus),
		nullableString(row.CallbackError),
		row.CallbackRetryCount,
		nullableString(row.SourceSystem),
		row.CreatedAt,
		nullableTime(row.StartedAt),
		nullableTime(row.CompletedAt),
		row.UpdatedAt,
	)
	return err
}

type taskStoreRow struct {
	ID                 string
	ExportType         string
	FiltersJSON        string
	Status             string
	OutputPath         string
	RetryCount         int
	MaxRetries         int
	ErrorMessage       string
	UserID             string
	CompanyUUID        string
	RequestID          string
	FileName           string
	FileSize           int64
	OSSKey             string
	OSSURL             string
	OSSStatus          string
	OSSError           string
	ExpiredAt          time.Time
	CallbackURL        string
	CallbackStatus     string
	CallbackError      string
	CallbackRetryCount int
	SourceSystem       string
	CreatedAt          time.Time
	StartedAt          time.Time
	CompletedAt        time.Time
	UpdatedAt          time.Time
}

func (t *Task) storeRow() (taskStoreRow, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.storeRowLocked()
}

func (t *Task) storeRowLocked() (taskStoreRow, error) {
	filtersJSON, err := json.Marshal(CloneFilters(t.filters))
	if err != nil {
		return taskStoreRow{}, err
	}
	return taskStoreRow{
		ID:                 t.ID,
		ExportType:         t.Type,
		FiltersJSON:        string(filtersJSON),
		Status:             string(t.State),
		OutputPath:         t.OutputFile,
		RetryCount:         t.RetryCount,
		MaxRetries:         t.MaxRetries,
		ErrorMessage:       t.Error,
		UserID:             t.UserID,
		CompanyUUID:        t.CompanyUUID,
		RequestID:          t.RequestID,
		FileName:           t.FileName,
		FileSize:           t.FileSize,
		OSSKey:             t.OSSKey,
		OSSURL:             t.OSSURL,
		OSSStatus:          t.OSSStatus,
		OSSError:           t.OSSError,
		ExpiredAt:          t.ExpiredAt,
		CallbackURL:        t.CallbackURL,
		CallbackStatus:     t.CallbackStatus,
		CallbackError:      t.CallbackError,
		CallbackRetryCount: t.CallbackRetryCount,
		SourceSystem:       t.SourceSystem,
		CreatedAt:          t.CreatedAt,
		StartedAt:          t.StartedAt,
		CompletedAt:        t.CompletedAt,
		UpdatedAt:          time.Now(),
	}, nil
}

func nullableString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}

func nullableTime(value time.Time) sql.NullTime {
	return sql.NullTime{Time: value, Valid: !value.IsZero()}
}

func nullStringValue(value sql.NullString) string {
	if value.Valid {
		return value.String
	}
	return ""
}

func (s *MySQLTaskStore) ignoreDuplicateSchemaError(_ sql.Result, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "Duplicate column name") || strings.Contains(msg, "Duplicate key name") {
		return nil
	}
	return err
}
