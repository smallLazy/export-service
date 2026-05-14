package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type TaskState string

const (
	TaskPending   TaskState = "pending"
	TaskRunning   TaskState = "running"
	TaskCompleted TaskState = "completed"
	TaskFailed    TaskState = "failed"
	TaskCanceled  TaskState = "canceled"
)

type Task struct {
	ID                 string    `json:"id"`
	Type               string    `json:"type"`
	State              TaskState `json:"state"`
	RetryCount         int       `json:"retry_count"`
	MaxRetries         int       `json:"max_retries"`
	OutputFile         string    `json:"output_file,omitempty"`
	Total              int64     `json:"total,omitempty"`
	Error              string    `json:"error,omitempty"`
	UserID             string    `json:"user_id,omitempty"`
	CompanyUUID        string    `json:"company_uuid,omitempty"`
	RequestID          string    `json:"request_id,omitempty"`
	FileName           string    `json:"file_name,omitempty"`
	FileSize           int64     `json:"file_size,omitempty"`
	OSSKey             string    `json:"oss_key,omitempty"`
	OSSURL             string    `json:"oss_url,omitempty"`
	OSSStatus          string    `json:"oss_status,omitempty"`
	OSSError           string    `json:"oss_error,omitempty"`
	ExpiredAt          time.Time `json:"expired_at,omitempty"`
	CallbackURL        string    `json:"callback_url,omitempty"`
	CallbackStatus     string    `json:"callback_status,omitempty"`
	CallbackError      string    `json:"callback_error,omitempty"`
	CallbackRetryCount int       `json:"callback_retry_count,omitempty"`
	SourceSystem       string    `json:"source_system,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	StartedAt          time.Time `json:"started_at,omitempty"`
	CompletedAt        time.Time `json:"completed_at,omitempty"`

	mu      sync.RWMutex
	cancel  context.CancelFunc
	filters url.Values
}

type TaskMetadata struct {
	UserID       string
	CompanyUUID  string
	RequestID    string
	FileName     string
	CallbackURL  string
	SourceSystem string
}

// TaskManager is the shared export task manager for all export types.
// Business exporters should keep their own query/filter logic outside this layer.
type TaskManager struct {
	tasks         map[string]*Task
	mu            sync.RWMutex
	outputDir     string
	store         TaskStore
	ossUploader   OSSUploader
	callback      CallbackClient
	ossMaxRetries int
	ossExpireDays int
}

func newTaskManager(outputDir string, store TaskStore, uploader OSSUploader, callback CallbackClient, ossMaxRetries, ossExpireDays int) *TaskManager {
	manager := &TaskManager{
		tasks:         make(map[string]*Task),
		outputDir:     outputDir,
		store:         store,
		ossUploader:   uploader,
		callback:      callback,
		ossMaxRetries: ossMaxRetries,
		ossExpireDays: defaultInt(ossExpireDays, 7),
	}
	manager.loadPersistedTasks()
	return manager
}

func taskID(exportType string, filters url.Values, meta TaskMetadata) string {
	h := sha256.New()
	h.Write([]byte(exportType))
	if strings.TrimSpace(meta.RequestID) != "" {
		h.Write([]byte("request_id"))
		h.Write([]byte{0})
		h.Write([]byte(strings.TrimSpace(meta.RequestID)))
		h.Write([]byte{0})
		h.Write([]byte("source_system"))
		h.Write([]byte{0})
		h.Write([]byte(strings.TrimSpace(meta.SourceSystem)))
		h.Write([]byte{0})
		return hex.EncodeToString(h.Sum(nil))[:16]
	}
	keys := make([]string, 0, len(filters))
	for k := range filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		values := append([]string(nil), filters[k]...)
		sort.Strings(values)
		for _, v := range values {
			h.Write([]byte(v))
			h.Write([]byte{0})
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func (m *TaskManager) submit(exportType string, filters url.Values, meta TaskMetadata, maxRetries int) (*Task, bool, error) {
	id := taskID(exportType, filters, meta)

	m.mu.Lock()
	existing, ok := m.tasks[id]
	if ok {
		m.mu.Unlock()
		return existing, false, nil
	}

	now := time.Now()
	task := &Task{
		ID:           id,
		Type:         exportType,
		State:        TaskPending,
		MaxRetries:   maxRetries,
		CreatedAt:    now,
		OutputFile:   filepath.Join(m.outputDir, id+".csv"),
		UserID:       strings.TrimSpace(meta.UserID),
		CompanyUUID:  strings.TrimSpace(meta.CompanyUUID),
		RequestID:    strings.TrimSpace(meta.RequestID),
		FileName:     strings.TrimSpace(meta.FileName),
		CallbackURL:  strings.TrimSpace(meta.CallbackURL),
		SourceSystem: strings.TrimSpace(meta.SourceSystem),
		filters:      cloneFilters(filters),
	}
	if err := m.saveTask(task); err != nil {
		m.mu.Unlock()
		return nil, false, err
	}
	m.tasks[id] = task
	m.mu.Unlock()
	return task, true, nil
}

func (m *TaskManager) find(exportType string, filters url.Values, meta TaskMetadata) (*Task, bool) {
	id := taskID(exportType, filters, meta)
	m.mu.RLock()
	task, ok := m.tasks[id]
	m.mu.RUnlock()
	return task, ok
}

func (m *TaskManager) get(id string) (*Task, error) {
	m.mu.RLock()
	task, ok := m.tasks[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("task %q not found", id)
	}
	return task, nil
}

func (m *TaskManager) retry(id string) (*Task, error) {
	m.mu.RLock()
	task, ok := m.tasks[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("task %q not found", id)
	}

	task.mu.Lock()
	if task.State != TaskFailed {
		task.mu.Unlock()
		return nil, fmt.Errorf("task %q is %s, only failed tasks can be retried", id, task.State)
	}
	task.State = TaskPending
	task.Error = ""
	task.OSSStatus = ""
	task.OSSError = ""
	task.CallbackStatus = ""
	task.CallbackError = ""
	task.CompletedAt = time.Time{}
	task.StartedAt = time.Time{}
	row, err := task.storeRowLocked()
	task.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := m.saveTaskRow(row); err != nil {
		return nil, err
	}
	return task, nil
}

func (t *Task) canRetryUploadOnly(forceRebuild bool) bool {
	if forceRebuild {
		return false
	}
	t.mu.RLock()
	state := t.State
	ossStatus := t.OSSStatus
	outputFile := t.OutputFile
	t.mu.RUnlock()
	if state != TaskFailed || ossStatus != "failed" || strings.TrimSpace(outputFile) == "" {
		return false
	}
	info, err := os.Stat(outputFile)
	return err == nil && !info.IsDir() && info.Size() > 0
}

func (m *TaskManager) cancel(id string) (*Task, error) {
	task, err := m.get(id)
	if err != nil {
		return nil, err
	}

	task.mu.Lock()
	switch task.State {
	case TaskCompleted:
		task.mu.Unlock()
		return nil, fmt.Errorf("task %q is completed, cannot be canceled", id)
	case TaskCanceled:
		task.mu.Unlock()
		return task, nil
	case TaskPending, TaskRunning:
		if task.cancel != nil {
			task.cancel()
		}
		task.State = TaskCanceled
		task.Error = "canceled"
		task.CompletedAt = time.Now()
		row, err := task.storeRowLocked()
		task.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if err := m.saveTaskRow(row); err != nil {
			return nil, err
		}
		return task, nil
	default:
		state := task.State
		task.mu.Unlock()
		return nil, fmt.Errorf("task %q is %s, cannot be canceled", id, state)
	}
}

func (m *TaskManager) execute(ctx context.Context, task *Task, fn func(ctx context.Context) (int64, error)) {
	startedAt := time.Now()
	task.mu.Lock()
	if task.State == TaskCanceled {
		task.mu.Unlock()
		return
	}
	task.State = TaskRunning
	task.StartedAt = time.Now()
	task.Error = ""
	row, err := task.storeRowLocked()
	task.mu.Unlock()
	if err != nil {
		slog.Error("export task row build failed", "task_id", task.ID, "export_type", task.Type, "status", TaskRunning, "duration", time.Since(startedAt), "error", err)
	} else if err := m.saveTaskRow(row); err != nil {
		slog.Error("export task persist failed", "task_id", task.ID, "export_type", task.Type, "status", TaskRunning, "duration", time.Since(startedAt), "error", err)
	}
	slog.Info("export task started", "task_id", task.ID, "export_type", task.Type, "status", TaskRunning, "duration", time.Since(startedAt))

	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		task.mu.Lock()
		task.cancel = nil
		row, err := task.storeRowLocked()
		task.mu.Unlock()
		if err != nil {
			slog.Error("export task cleanup row build failed", "task_id", task.ID, "export_type", task.Type, "status", taskState(task), "duration", time.Since(startedAt), "error", err)
		} else if err := m.saveTaskRow(row); err != nil {
			slog.Error("export task cleanup persist failed", "task_id", task.ID, "export_type", task.Type, "status", taskState(task), "duration", time.Since(startedAt), "error", err)
		}
	}()
	task.mu.Lock()
	if task.State == TaskCanceled {
		task.mu.Unlock()
		return
	}
	task.cancel = cancel
	task.mu.Unlock()

	for attempt := 0; attempt <= task.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * 2 * time.Second
			slog.Info("export task retry waiting", "task_id", task.ID, "export_type", task.Type, "status", taskState(task), "attempt", attempt, "max_retries", task.MaxRetries, "delay", delay, "duration", time.Since(startedAt))
			select {
			case <-execCtx.Done():
				if m.isCanceled(task) {
					return
				}
				m.failTask(task, execCtx.Err().Error(), attempt)
				return
			case <-time.After(delay):
			}
		}

		total, err := fn(execCtx)
		if err == nil {
			if uploadErr := m.uploadTaskOutput(execCtx, task); uploadErr != nil {
				task.mu.Lock()
				if task.State == TaskCanceled {
					task.mu.Unlock()
					return
				}
				task.State = TaskFailed
				task.Error = "upload oss failed"
				task.OSSError = uploadErr.Error()
				task.OSSStatus = "failed"
				task.CompletedAt = time.Now()
				task.RetryCount = attempt
				row, rowErr := task.storeRowLocked()
				task.mu.Unlock()
				if rowErr != nil {
					slog.Error("export task upload failed row build failed", "task_id", task.ID, "export_type", task.Type, "status", TaskFailed, "duration", time.Since(startedAt), "error", rowErr)
				} else if err := m.saveTaskRow(row); err != nil {
					slog.Error("export task upload failed persist failed", "task_id", task.ID, "export_type", task.Type, "status", TaskFailed, "duration", time.Since(startedAt), "error", err)
				}
				slog.Error("export task oss upload failed", "task_id", task.ID, "export_type", task.Type, "status", TaskFailed, "duration", time.Since(startedAt), "error", uploadErr)
				m.notifyCallback(context.Background(), task)
				return
			}

			task.mu.Lock()
			if task.State == TaskCanceled {
				task.mu.Unlock()
				return
			}
			task.State = TaskCompleted
			task.Total = total
			task.CompletedAt = time.Now()
			task.RetryCount = attempt
			task.Error = ""
			row, err := task.storeRowLocked()
			task.mu.Unlock()
			if err != nil {
				slog.Error("export task row build failed", "task_id", task.ID, "export_type", task.Type, "status", TaskCompleted, "duration", time.Since(startedAt), "error", err)
			} else if err := m.saveTaskRow(row); err != nil {
				slog.Error("export task persist failed", "task_id", task.ID, "export_type", task.Type, "status", TaskCompleted, "duration", time.Since(startedAt), "error", err)
			}
			slog.Info("export task completed", "task_id", task.ID, "export_type", task.Type, "status", TaskCompleted, "total", total, "retry_count", attempt, "duration", time.Since(startedAt))
			m.notifyCallback(context.Background(), task)
			return
		}

		slog.Error("export task attempt failed", "task_id", task.ID, "export_type", task.Type, "status", taskState(task), "attempt", attempt+1, "max_attempts", task.MaxRetries+1, "duration", time.Since(startedAt), "error", err)

		if execCtx.Err() != nil {
			if m.isCanceled(task) {
				return
			}
			m.failTask(task, execCtx.Err().Error(), attempt)
			return
		}

		if attempt >= task.MaxRetries {
			m.failTask(task, err.Error(), attempt)
			return
		}

		task.mu.Lock()
		task.RetryCount = attempt + 1
		row, err := task.storeRowLocked()
		task.mu.Unlock()
		if err != nil {
			slog.Error("export task retry row build failed", "task_id", task.ID, "export_type", task.Type, "status", taskState(task), "duration", time.Since(startedAt), "error", err)
		} else if err := m.saveTaskRow(row); err != nil {
			slog.Error("export task retry persist failed", "task_id", task.ID, "export_type", task.Type, "status", taskState(task), "duration", time.Since(startedAt), "error", err)
		}
	}
}

func (m *TaskManager) failTask(task *Task, errMsg string, attempt int) {
	task.mu.Lock()
	task.State = TaskFailed
	task.Error = errMsg
	task.CompletedAt = time.Now()
	task.RetryCount = attempt
	row, err := task.storeRowLocked()
	task.mu.Unlock()
	if err != nil {
		slog.Error("export task failed row build failed", "task_id", task.ID, "export_type", task.Type, "status", TaskFailed, "duration", taskDuration(task), "error", err)
	} else if err := m.saveTaskRow(row); err != nil {
		slog.Error("export task failed persist failed", "task_id", task.ID, "export_type", task.Type, "status", TaskFailed, "duration", taskDuration(task), "error", err)
	}
	m.notifyCallback(context.Background(), task)
}

func (m *TaskManager) uploadTaskOutput(ctx context.Context, task *Task) error {
	if m.ossUploader == nil {
		return nil
	}
	task.mu.Lock()
	if task.State == TaskCanceled {
		task.mu.Unlock()
		return context.Canceled
	}
	task.OSSStatus = "uploading"
	task.OSSError = ""
	row, err := task.storeRowLocked()
	outputFile := task.OutputFile
	exportType := task.Type
	taskID := task.ID
	task.mu.Unlock()
	if err != nil {
		return err
	}
	if err := m.saveTaskRow(row); err != nil {
		slog.Error("export task oss status persist failed", "task_id", taskID, "export_type", exportType, "status", "uploading", "error", err)
	}

	var lastErr error
	for attempt := 0; attempt <= m.ossMaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * 2 * time.Second
			slog.Info("export task oss retry waiting", "task_id", taskID, "export_type", exportType, "status", "uploading", "attempt", attempt, "max_retries", m.ossMaxRetries, "delay", delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		expiresAt := time.Now().AddDate(0, 0, m.ossExpireDays)
		result, err := m.ossUploader.Upload(ctx, outputFile, exportType, taskID, expiresAt)
		if err == nil {
			task.mu.Lock()
			if task.State == TaskCanceled {
				task.mu.Unlock()
				return context.Canceled
			}
			task.OSSStatus = "completed"
			task.OSSError = ""
			task.OSSKey = result.Key
			task.OSSURL = result.URL
			task.FileSize = result.FileSize
			task.ExpiredAt = expiresAt
			row, rowErr := task.storeRowLocked()
			task.mu.Unlock()
			if rowErr != nil {
				return rowErr
			}
			if err := m.saveTaskRow(row); err != nil {
				return err
			}
			slog.Info("export task oss uploaded", "task_id", taskID, "export_type", exportType, "status", "completed", "file_size", result.FileSize, "oss_key", result.Key)
			return nil
		}
		lastErr = err
		slog.Error("export task oss upload attempt failed", "task_id", taskID, "export_type", exportType, "status", "uploading", "attempt", attempt+1, "max_attempts", m.ossMaxRetries+1, "error", err)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return lastErr
}

func (m *TaskManager) notifyCallback(ctx context.Context, task *Task) {
	if m.callback == nil {
		return
	}
	payload, callbackURL := task.callbackPayload()
	if strings.TrimSpace(callbackURL) == "" {
		return
	}
	task.mu.Lock()
	task.CallbackStatus = "pending"
	task.CallbackError = ""
	row, err := task.storeRowLocked()
	task.mu.Unlock()
	if err != nil {
		slog.Error("export task callback pending row build failed", "task_id", task.ID, "export_type", task.Type, "status", taskState(task), "error", err)
	} else if err := m.saveTaskRow(row); err != nil {
		slog.Error("export task callback pending persist failed", "task_id", task.ID, "export_type", task.Type, "status", taskState(task), "error", err)
	}

	err = m.callback.Notify(ctx, callbackURL, payload)
	task.mu.Lock()
	if err != nil {
		task.CallbackStatus = "failed"
		task.CallbackError = err.Error()
	} else {
		task.CallbackStatus = "completed"
		task.CallbackError = ""
	}
	row, rowErr := task.storeRowLocked()
	task.mu.Unlock()
	if rowErr != nil {
		slog.Error("export task callback row build failed", "task_id", task.ID, "export_type", task.Type, "status", payload.Status, "error", rowErr)
		return
	}
	if saveErr := m.saveTaskRow(row); saveErr != nil {
		slog.Error("export task callback persist failed", "task_id", task.ID, "export_type", task.Type, "status", payload.Status, "error", saveErr)
	}
	if err != nil {
		slog.Error("export task callback failed", "task_id", task.ID, "export_type", task.Type, "status", payload.Status, "callback_url", callbackURL, "error", err)
		return
	}
	slog.Info("export task callback completed", "task_id", task.ID, "export_type", task.Type, "status", payload.Status, "callback_url", callbackURL)
}

func (m *TaskManager) isCanceled(task *Task) bool {
	task.mu.RLock()
	defer task.mu.RUnlock()
	return task.State == TaskCanceled
}

func (t *Task) snapshot() map[string]any {
	t.mu.RLock()
	defer t.mu.RUnlock()
	m := map[string]any{
		"id":          t.ID,
		"type":        t.Type,
		"state":       t.State,
		"retry_count": t.RetryCount,
		"max_retries": t.MaxRetries,
		"created_at":  t.CreatedAt,
	}
	if !t.StartedAt.IsZero() {
		m["started_at"] = t.StartedAt
	}
	if !t.CompletedAt.IsZero() {
		m["completed_at"] = t.CompletedAt
	}
	if t.State == TaskCompleted {
		m["total"] = t.Total
		m["output_file"] = t.OutputFile
	}
	if t.UserID != "" {
		m["user_id"] = t.UserID
	}
	if t.CompanyUUID != "" {
		m["company_uuid"] = t.CompanyUUID
	}
	if t.RequestID != "" {
		m["request_id"] = t.RequestID
	}
	if t.FileName != "" {
		m["file_name"] = t.FileName
	}
	if t.SourceSystem != "" {
		m["source_system"] = t.SourceSystem
	}
	if t.FileSize > 0 {
		m["file_size"] = t.FileSize
	}
	if t.OSSKey != "" {
		m["oss_key"] = t.OSSKey
	}
	if t.OSSURL != "" {
		m["oss_url"] = t.OSSURL
	}
	if t.OSSStatus != "" {
		m["oss_status"] = t.OSSStatus
	}
	if t.OSSError != "" {
		m["oss_error"] = t.OSSError
	}
	if !t.ExpiredAt.IsZero() {
		m["expired_at"] = t.ExpiredAt
	}
	if t.CallbackStatus != "" {
		m["callback_status"] = t.CallbackStatus
	}
	if t.CallbackError != "" {
		m["callback_error"] = t.CallbackError
	}
	if t.CallbackRetryCount > 0 {
		m["callback_retry_count"] = t.CallbackRetryCount
	}
	if t.State == TaskFailed || t.State == TaskCanceled {
		m["error"] = t.Error
	}
	return m
}

func (t *Task) callbackPayload() (ExportCallbackPayload, string) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	payload := ExportCallbackPayload{
		TaskID:       t.ID,
		RequestID:    t.RequestID,
		UserID:       t.UserID,
		CompanyUUID:  t.CompanyUUID,
		SourceSystem: t.SourceSystem,
		ExportType:   t.Type,
		FileName:     t.FileName,
		Status:       string(t.State),
		FileSize:     t.FileSize,
		OSSKey:       t.OSSKey,
		OSSURL:       t.OSSURL,
		ExpiredAt:    t.ExpiredAt,
		ErrorMessage: t.Error,
	}
	return payload, t.CallbackURL
}

func (t *Task) filtersSnapshot() url.Values {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(url.Values, len(t.filters))
	for k, vv := range t.filters {
		out[k] = append([]string(nil), vv...)
	}
	return out
}

func cloneFilters(in url.Values) url.Values {
	out := make(url.Values, len(in))
	for k, vv := range in {
		copied := append([]string(nil), vv...)
		sort.Strings(copied)
		out[k] = copied
	}
	return out
}

func (m *TaskManager) loadPersistedTasks() {
	if m.store == nil {
		return
	}
	tasks, err := m.store.load(context.Background())
	if err != nil {
		slog.Error("load persisted export tasks failed", "error", err)
		return
	}
	now := time.Now()
	for _, task := range tasks {
		if task.State == TaskPending || task.State == TaskRunning {
			task.State = TaskFailed
			task.Error = "task interrupted by service restart"
			task.CompletedAt = now
			row, err := task.storeRow()
			if err != nil {
				slog.Error("interrupted export task row build failed", "task_id", task.ID, "export_type", task.Type, "status", task.State, "error", err)
			} else if err := m.saveTaskRow(row); err != nil {
				slog.Error("interrupted export task persist failed", "task_id", task.ID, "export_type", task.Type, "status", task.State, "error", err)
			}
		}
		m.tasks[task.ID] = task
	}
}

func (m *TaskManager) saveTask(task *Task) error {
	if m.store == nil {
		return nil
	}
	row, err := task.storeRow()
	if err != nil {
		return err
	}
	return m.saveTaskRow(row)
}

func (m *TaskManager) saveTaskRow(row taskStoreRow) error {
	if m.store == nil {
		return nil
	}
	return m.store.save(context.Background(), row)
}

func taskState(task *Task) TaskState {
	task.mu.RLock()
	defer task.mu.RUnlock()
	return task.State
}

func taskDuration(task *Task) time.Duration {
	task.mu.RLock()
	defer task.mu.RUnlock()
	if task.StartedAt.IsZero() {
		return 0
	}
	if task.CompletedAt.IsZero() {
		return time.Since(task.StartedAt)
	}
	return task.CompletedAt.Sub(task.StartedAt)
}

func taskTotal(task *Task) int64 {
	task.mu.RLock()
	defer task.mu.RUnlock()
	return task.Total
}
