package export

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	taskpkg "export-service/internal/task"
	"export-service/internal/writer"
)

type Uploader interface {
	Upload(ctx context.Context, localPath, exportType, taskID string, expiresAt time.Time) (taskpkg.OSSUploadResult, error)
}

type CallbackClient interface {
	Notify(ctx context.Context, callbackURL string, payload taskpkg.ExportCallbackPayload) error
}

type ExportExecutor struct {
	tasks         *taskpkg.TaskManager
	uploader      Uploader
	callback      CallbackClient
	ossMaxRetries int
	ossExpireDays int
}

func NewExecutor(tasks *taskpkg.TaskManager, uploader Uploader, callback CallbackClient, ossMaxRetries, ossExpireDays int) *ExportExecutor {
	return &ExportExecutor{
		tasks:         tasks,
		uploader:      uploader,
		callback:      callback,
		ossMaxRetries: ossMaxRetries,
		ossExpireDays: defaultInt(ossExpireDays, 7),
	}
}

func (e *ExportExecutor) RunExport(ctx context.Context, task *taskpkg.Task, exporter Exporter, filters url.Values) {
	e.runExport(ctx, task, func(ctx context.Context) (int64, error) {
		return writer.ExportCSVFile(ctx, task.OutputFile, exporter, filters)
	})
}

func (e *ExportExecutor) Retry(ctx context.Context, task *taskpkg.Task, exporter Exporter, filters url.Values, stage taskpkg.RetryStage) {
	defer e.tasks.FinishRetry(task)
	switch stage {
	case taskpkg.RetryStageCallback:
		e.Callback(ctx, task)
	case taskpkg.RetryStageUpload:
		e.runUploadOnly(ctx, task)
	default:
		e.RunExport(ctx, task, exporter, filters)
	}
}

func (e *ExportExecutor) RetryStage(task *taskpkg.Task, forceRebuild bool) taskpkg.RetryStage {
	if forceRebuild {
		return taskpkg.RetryStageRebuild
	}
	snapshot := task.Snapshot()
	state, _ := snapshot["state"].(taskpkg.TaskState)
	ossStatus, _ := snapshot["oss_status"].(string)
	callbackStatus, _ := snapshot["callback_status"].(string)
	outputFile := task.OutputFile
	if callbackStatus == "failed" && (state == taskpkg.TaskCompleted || ossStatus == "completed") {
		return taskpkg.RetryStageCallback
	}
	if state == taskpkg.TaskFailed && ossStatus == "failed" && usableLocalFile(outputFile) {
		return taskpkg.RetryStageUpload
	}
	return taskpkg.RetryStageRebuild
}

func (e *ExportExecutor) runExport(ctx context.Context, task *taskpkg.Task, fn func(context.Context) (int64, error)) {
	startedAt := time.Now()
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := e.tasks.MarkRunning(task); err != nil {
		return
	}
	if !e.tasks.SetCancel(task, cancel) {
		return
	}
	defer e.tasks.ClearCancel(task)

	maxRetries := taskMaxRetries(task)
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * 2 * time.Second
			slog.Info("export task retry waiting", "task_id", task.ID, "export_type", task.Type, "status", taskpkg.State(task), "attempt", attempt, "max_retries", maxRetries, "delay", delay, "duration", time.Since(startedAt))
			select {
			case <-execCtx.Done():
				if e.tasks.IsCanceled(task) {
					return
				}
				_ = e.tasks.MarkExportFailed(task, execCtx.Err().Error(), attempt)
				e.Callback(context.Background(), task)
				return
			case <-time.After(delay):
			}
		}

		total, err := fn(execCtx)
		if err == nil {
			if err := e.Upload(execCtx, task); err != nil {
				if e.tasks.IsCanceled(task) {
					return
				}
				_ = e.tasks.MarkUploadFailed(task, err.Error(), attempt)
				e.Callback(context.Background(), task)
				return
			}
			_ = e.tasks.MarkCompleted(task, total, attempt)
			e.Callback(context.Background(), task)
			return
		}

		slog.Error("export task attempt failed", "task_id", task.ID, "export_type", task.Type, "status", taskpkg.State(task), "attempt", attempt+1, "max_attempts", maxRetries+1, "duration", time.Since(startedAt), "error", err)
		if execCtx.Err() != nil {
			if e.tasks.IsCanceled(task) {
				return
			}
			_ = e.tasks.MarkExportFailed(task, execCtx.Err().Error(), attempt)
			e.Callback(context.Background(), task)
			return
		}
		if attempt >= maxRetries {
			_ = e.tasks.MarkExportFailed(task, err.Error(), attempt)
			e.Callback(context.Background(), task)
			return
		}
		_ = e.tasks.MarkRetryCount(task, attempt+1)
	}
}

func (e *ExportExecutor) runUploadOnly(ctx context.Context, task *taskpkg.Task) {
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := e.tasks.MarkRunning(task); err != nil {
		return
	}
	if !e.tasks.SetCancel(task, cancel) {
		return
	}
	defer e.tasks.ClearCancel(task)

	if err := e.Upload(execCtx, task); err != nil {
		if e.tasks.IsCanceled(task) {
			return
		}
		_ = e.tasks.MarkUploadFailed(task, err.Error(), taskRetryCount(task))
		e.Callback(context.Background(), task)
		return
	}
	_ = e.tasks.MarkCompleted(task, taskpkg.Total(task), taskRetryCount(task))
	e.Callback(context.Background(), task)
}

func (e *ExportExecutor) Upload(ctx context.Context, task *taskpkg.Task) error {
	if e.uploader == nil {
		return nil
	}
	if err := e.tasks.MarkUploadStarted(task); err != nil {
		return err
	}
	outputFile, exportType, taskID := taskOutputSnapshot(task)
	var lastErr error
	for attempt := 0; attempt <= e.ossMaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * 2 * time.Second
			slog.Info("export task oss retry waiting", "task_id", taskID, "export_type", exportType, "status", "uploading", "attempt", attempt, "max_retries", e.ossMaxRetries, "delay", delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		expiresAt := time.Now().AddDate(0, 0, e.ossExpireDays)
		result, err := e.uploader.Upload(ctx, outputFile, exportType, taskID, expiresAt)
		if err == nil {
			return e.tasks.MarkUploadCompleted(task, result, expiresAt)
		}
		lastErr = err
		slog.Error("export task oss upload attempt failed", "task_id", taskID, "export_type", exportType, "status", "uploading", "attempt", attempt+1, "max_attempts", e.ossMaxRetries+1, "error", err)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return lastErr
}

func (e *ExportExecutor) Callback(ctx context.Context, task *taskpkg.Task) {
	e.callbackTask(ctx, task, false)
}

func (e *ExportExecutor) callbackTask(ctx context.Context, task *taskpkg.Task, retry bool) {
	if e.callback == nil {
		return
	}
	payload, callbackURL := task.CallbackPayload()
	if strings.TrimSpace(callbackURL) == "" {
		return
	}
	if err := e.tasks.MarkCallbackPending(task); err != nil {
		return
	}
	if err := e.callback.Notify(ctx, callbackURL, payload); err != nil {
		if retry {
			_ = e.tasks.MarkCallbackRetryFailed(task, err.Error())
			return
		}
		_ = e.tasks.MarkCallbackFailed(task, err.Error())
		return
	}
	_ = e.tasks.MarkCallbackCompleted(task)
}

func (e *ExportExecutor) StartCallbackCompensator(ctx context.Context, interval time.Duration, maxRetries int) {
	if e.callback == nil || interval <= 0 || maxRetries <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.runCallbackCompensation(ctx, maxRetries)
			}
		}
	}()
}

func (e *ExportExecutor) runCallbackCompensation(ctx context.Context, maxRetries int) {
	for _, candidate := range e.tasks.CallbackRetryCandidates(maxRetries) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		task, err := e.tasks.Retry(candidate.ID, taskpkg.RetryStageCallback)
		if err != nil {
			slog.Info("callback compensation skipped", "task_id", candidate.ID, "error", err)
			continue
		}
		e.callbackTask(ctx, task, true)
		e.tasks.FinishRetry(task)
	}
}

func usableLocalFile(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Size() > 0
}

func taskMaxRetries(task *taskpkg.Task) int {
	return task.MaxRetries
}

func taskRetryCount(task *taskpkg.Task) int {
	return task.RetryCount
}

func taskOutputSnapshot(task *taskpkg.Task) (string, string, string) {
	return task.OutputFile, task.Type, task.ID
}

func defaultInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
