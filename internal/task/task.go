package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type RetryStage string

const (
	RetryStageRebuild  RetryStage = "rebuild"
	RetryStageUpload   RetryStage = "upload"
	RetryStageCallback RetryStage = "callback"
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

	retryInFlight bool
}

type TaskMetadata struct {
	UserID       string
	CompanyUUID  string
	RequestID    string
	FileName     string
	CallbackURL  string
	SourceSystem string
}

type OSSUploadResult struct {
	Key      string
	URL      string
	FileSize int64
}

type ExportCallbackPayload struct {
	TaskID       string    `json:"task_id"`
	RequestID    string    `json:"request_id,omitempty"`
	UserID       string    `json:"user_id,omitempty"`
	CompanyUUID  string    `json:"company_uuid,omitempty"`
	SourceSystem string    `json:"source_system,omitempty"`
	ExportType   string    `json:"export_type"`
	FileName     string    `json:"file_name,omitempty"`
	Status       string    `json:"status"`
	FileSize     int64     `json:"file_size,omitempty"`
	OSSKey       string    `json:"oss_key,omitempty"`
	OSSURL       string    `json:"oss_url,omitempty"`
	ExpiredAt    time.Time `json:"expired_at,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
}

// TaskManager is the shared export task manager for all export types.
// Business exporters should keep their own query/filter logic outside this layer.
type TaskManager struct {
	tasks     map[string]*Task
	mu        sync.RWMutex
	outputDir string
	store     TaskStore
}

func NewManager(outputDir string, store TaskStore) *TaskManager {
	manager := &TaskManager{
		tasks:     make(map[string]*Task),
		outputDir: outputDir,
		store:     store,
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

func (m *TaskManager) Submit(exportType string, filters url.Values, meta TaskMetadata, maxRetries int) (*Task, bool, error) {
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
		filters:      CloneFilters(filters),
	}
	if err := m.saveTask(task); err != nil {
		m.mu.Unlock()
		return nil, false, err
	}
	m.tasks[id] = task
	m.mu.Unlock()
	return task, true, nil
}

func (m *TaskManager) Find(exportType string, filters url.Values, meta TaskMetadata) (*Task, bool) {
	id := taskID(exportType, filters, meta)
	m.mu.RLock()
	task, ok := m.tasks[id]
	m.mu.RUnlock()
	return task, ok
}

func (m *TaskManager) Get(id string) (*Task, error) {
	m.mu.RLock()
	task, ok := m.tasks[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("task %q not found", id)
	}
	return task, nil
}

func (m *TaskManager) Retry(id string, stage RetryStage) (*Task, error) {
	m.mu.RLock()
	task, ok := m.tasks[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("task %q not found", id)
	}

	task.mu.Lock()
	if task.retryInFlight || task.State == TaskRunning || task.OSSStatus == "uploading" || task.CallbackStatus == "pending" {
		task.mu.Unlock()
		return nil, fmt.Errorf("task %q is already executing", id)
	}
	canRetry := task.State == TaskFailed || (stage == RetryStageCallback && task.CallbackStatus == "failed")
	state := task.State
	if !canRetry {
		task.mu.Unlock()
		return nil, fmt.Errorf("task %q is %s, only failed tasks or failed callbacks can be retried", id, state)
	}
	task.retryInFlight = true
	applyRetryPreparedLocked(task, stage)
	row, err := task.storeRowLocked()
	taskID := task.ID
	exportType := task.Type
	status := task.State
	ossStatus := task.OSSStatus
	callbackStatus := task.CallbackStatus
	task.mu.Unlock()
	if err != nil {
		m.FinishRetry(task)
		return nil, err
	}
	if err := m.saveTaskRow(row); err != nil {
		m.FinishRetry(task)
		slog.Error("export task retry prepared persist failed", "task_id", taskID, "export_type", exportType, "status", status, "oss_status", ossStatus, "callback_status", callbackStatus, "error", err)
		return nil, err
	}
	slog.Info("export task retry prepared", "task_id", taskID, "export_type", exportType, "status", status, "oss_status", ossStatus, "callback_status", callbackStatus)
	return task, nil
}

func (m *TaskManager) FinishRetry(task *Task) {
	task.mu.Lock()
	task.retryInFlight = false
	task.mu.Unlock()
}

func (m *TaskManager) Cancel(id string) (*Task, error) {
	task, err := m.Get(id)
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
		task.mu.Unlock()
		if err := m.MarkCanceled(task); err != nil {
			return nil, err
		}
		return task, nil
	default:
		state := task.State
		task.mu.Unlock()
		return nil, fmt.Errorf("task %q is %s, cannot be canceled", id, state)
	}
}

func (m *TaskManager) MarkRunning(task *Task) error {
	return m.updateTask(task, "export task started", func(t *Task) {
		t.State = TaskRunning
		t.StartedAt = time.Now()
		t.CompletedAt = time.Time{}
		t.Error = ""
	})
}

func (m *TaskManager) MarkRetryPrepared(task *Task, stage RetryStage) error {
	return m.updateTask(task, "export task retry prepared", func(t *Task) {
		applyRetryPreparedLocked(t, stage)
	})
}

func applyRetryPreparedLocked(t *Task, stage RetryStage) {
	if stage != RetryStageCallback {
		t.State = TaskPending
		t.Error = ""
		t.CompletedAt = time.Time{}
		t.StartedAt = time.Time{}
	}
	switch stage {
	case RetryStageRebuild:
		t.OSSStatus = ""
		t.OSSError = ""
		t.CallbackStatus = ""
		t.CallbackError = ""
	case RetryStageUpload:
		t.OSSError = ""
		t.CallbackStatus = ""
		t.CallbackError = ""
	case RetryStageCallback:
		t.CallbackStatus = ""
		t.CallbackError = ""
	}
}

func (m *TaskManager) MarkRetryCount(task *Task, retryCount int) error {
	return m.updateTask(task, "export task retry count updated", func(t *Task) {
		t.RetryCount = retryCount
	})
}

func (m *TaskManager) MarkExportFailed(task *Task, errMsg string, attempt int) error {
	return m.updateTask(task, "export task failed", func(t *Task) {
		t.State = TaskFailed
		t.Error = errMsg
		t.CompletedAt = time.Now()
		t.RetryCount = attempt
	})
}

func (m *TaskManager) MarkUploadStarted(task *Task) error {
	return m.updateTask(task, "export task oss upload started", func(t *Task) {
		t.OSSStatus = "uploading"
		t.OSSError = ""
	})
}

func (m *TaskManager) MarkUploadCompleted(task *Task, result OSSUploadResult, expiresAt time.Time) error {
	return m.updateTask(task, "export task oss uploaded", func(t *Task) {
		t.OSSStatus = "completed"
		t.OSSError = ""
		t.OSSKey = result.Key
		t.OSSURL = result.URL
		t.FileSize = result.FileSize
		t.ExpiredAt = expiresAt
	})
}

func (m *TaskManager) MarkUploadFailed(task *Task, errMsg string, attempt int) error {
	return m.updateTask(task, "export task oss upload failed", func(t *Task) {
		t.State = TaskFailed
		t.Error = "upload oss failed"
		t.OSSError = errMsg
		t.OSSStatus = "failed"
		t.CompletedAt = time.Now()
		t.RetryCount = attempt
	})
}

func (m *TaskManager) MarkCallbackPending(task *Task) error {
	return m.updateTask(task, "export task callback pending", func(t *Task) {
		t.CallbackStatus = "pending"
		t.CallbackError = ""
	})
}

func (m *TaskManager) MarkCallbackCompleted(task *Task) error {
	return m.updateTask(task, "export task callback completed", func(t *Task) {
		t.CallbackStatus = "completed"
		t.CallbackError = ""
	})
}

func (m *TaskManager) MarkCallbackFailed(task *Task, errMsg string) error {
	return m.updateTask(task, "export task callback failed", func(t *Task) {
		t.CallbackStatus = "failed"
		t.CallbackError = errMsg
	})
}

func (m *TaskManager) MarkCallbackRetryFailed(task *Task, errMsg string) error {
	return m.updateTask(task, "export task callback retry failed", func(t *Task) {
		t.CallbackStatus = "failed"
		t.CallbackError = errMsg
		t.CallbackRetryCount++
	})
}

func (m *TaskManager) MarkCompleted(task *Task, total int64, attempt int) error {
	return m.updateTask(task, "export task completed", func(t *Task) {
		t.State = TaskCompleted
		t.Total = total
		t.CompletedAt = time.Now()
		t.RetryCount = attempt
		t.Error = ""
	})
}

func (m *TaskManager) MarkCanceled(task *Task) error {
	return m.updateTask(task, "export task canceled", func(t *Task) {
		t.State = TaskCanceled
		t.Error = "canceled"
		t.CompletedAt = time.Now()
	})
}

func (m *TaskManager) SetCancel(task *Task, cancel context.CancelFunc) bool {
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.State == TaskCanceled {
		return false
	}
	task.cancel = cancel
	return true
}

func (m *TaskManager) ClearCancel(task *Task) {
	task.mu.Lock()
	task.cancel = nil
	task.mu.Unlock()
}

func (m *TaskManager) updateTask(task *Task, message string, update func(*Task)) error {
	task.mu.Lock()
	update(task)
	row, err := task.storeRowLocked()
	taskID := task.ID
	exportType := task.Type
	status := task.State
	ossStatus := task.OSSStatus
	callbackStatus := task.CallbackStatus
	task.mu.Unlock()
	if err != nil {
		slog.Error(message+" row build failed", "task_id", taskID, "export_type", exportType, "status", status, "error", err)
		return err
	}
	if err := m.saveTaskRow(row); err != nil {
		slog.Error(message+" persist failed", "task_id", taskID, "export_type", exportType, "status", status, "oss_status", ossStatus, "callback_status", callbackStatus, "error", err)
		return err
	}
	slog.Info(message, "task_id", taskID, "export_type", exportType, "status", status, "oss_status", ossStatus, "callback_status", callbackStatus)
	return nil
}

func (m *TaskManager) IsCanceled(task *Task) bool {
	task.mu.RLock()
	defer task.mu.RUnlock()
	return task.State == TaskCanceled
}

func (m *TaskManager) CallbackRetryCandidates(maxRetries int) []*Task {
	if maxRetries <= 0 {
		return nil
	}
	m.mu.RLock()
	tasks := make([]*Task, 0, len(m.tasks))
	for _, task := range m.tasks {
		tasks = append(tasks, task)
	}
	m.mu.RUnlock()

	candidates := make([]*Task, 0, len(tasks))
	for _, task := range tasks {
		task.mu.RLock()
		ok := !task.retryInFlight &&
			task.CallbackStatus == "failed" &&
			task.OSSStatus == "completed" &&
			(task.State == TaskCompleted || task.State == TaskFailed) &&
			task.CallbackRetryCount < maxRetries &&
			strings.TrimSpace(task.CallbackURL) != ""
		task.mu.RUnlock()
		if ok {
			candidates = append(candidates, task)
		}
	}
	return candidates
}

func (t *Task) Snapshot() map[string]any {
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

func (t *Task) CallbackPayload() (ExportCallbackPayload, string) {
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

func (t *Task) FiltersSnapshot() url.Values {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(url.Values, len(t.filters))
	for k, vv := range t.filters {
		out[k] = append([]string(nil), vv...)
	}
	return out
}

func CloneFilters(in url.Values) url.Values {
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
	tasks, err := m.store.Load(context.Background())
	if err != nil {
		slog.Error("load persisted export tasks failed", "error", err)
		return
	}
	now := time.Now()
	for _, task := range tasks {
		if task.State == TaskPending || task.State == TaskRunning {
			_ = m.updateTask(task, "interrupted export task marked failed", func(t *Task) {
				t.State = TaskFailed
				t.Error = "task interrupted by service restart"
				t.CompletedAt = now
			})
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
	return m.store.Save(context.Background(), row)
}

func State(task *Task) TaskState {
	task.mu.RLock()
	defer task.mu.RUnlock()
	return task.State
}

func Duration(task *Task) time.Duration {
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

func Total(task *Task) int64 {
	task.mu.RLock()
	defer task.mu.RUnlock()
	return task.Total
}
