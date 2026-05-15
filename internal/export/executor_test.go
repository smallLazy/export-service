package export

import (
	"context"
	"errors"
	"net/url"
	"testing"

	taskpkg "export-service/internal/task"
)

type callbackFunc func(context.Context, string, taskpkg.ExportCallbackPayload) error

func (f callbackFunc) Notify(ctx context.Context, callbackURL string, payload taskpkg.ExportCallbackPayload) error {
	return f(ctx, callbackURL, payload)
}

func newCallbackRetryTask(t *testing.T, manager *taskpkg.TaskManager) *taskpkg.Task {
	t.Helper()
	task, _, err := manager.Submit("member_order", url.Values{"time_options": []string{"payment_at"}}, taskpkg.TaskMetadata{RequestID: t.Name(), SourceSystem: "trade", CallbackURL: "http://callback"}, 2)
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	if err := manager.MarkCompleted(task, 1, 0); err != nil {
		t.Fatalf("mark completed: %v", err)
	}
	if err := manager.MarkUploadCompleted(task, taskpkg.OSSUploadResult{Key: "k", URL: "u", FileSize: 1}, task.CreatedAt); err != nil {
		t.Fatalf("mark upload completed: %v", err)
	}
	if err := manager.MarkCallbackFailed(task, "callback failed"); err != nil {
		t.Fatalf("mark callback failed: %v", err)
	}
	return task
}

func TestRunCallbackCompensationCompletesCallback(t *testing.T) {
	manager := taskpkg.NewManager(t.TempDir(), nil)
	task := newCallbackRetryTask(t, manager)
	executor := NewExecutor(manager, nil, callbackFunc(func(context.Context, string, taskpkg.ExportCallbackPayload) error {
		return nil
	}), 0, 7)

	executor.runCallbackCompensation(context.Background(), 3)

	snapshot := task.Snapshot()
	if got := snapshot["callback_status"]; got != "completed" {
		t.Fatalf("expected callback completed, got %v", got)
	}
	if _, ok := snapshot["callback_error"]; ok {
		t.Fatalf("expected callback error cleared, got %v", snapshot["callback_error"])
	}
}

func TestRunCallbackCompensationIncrementsRetryCountOnFailure(t *testing.T) {
	manager := taskpkg.NewManager(t.TempDir(), nil)
	task := newCallbackRetryTask(t, manager)
	executor := NewExecutor(manager, nil, callbackFunc(func(context.Context, string, taskpkg.ExportCallbackPayload) error {
		return errors.New("still down")
	}), 0, 7)

	executor.runCallbackCompensation(context.Background(), 3)

	snapshot := task.Snapshot()
	if got := snapshot["callback_status"]; got != "failed" {
		t.Fatalf("expected callback failed, got %v", got)
	}
	if got := snapshot["callback_retry_count"]; got != 1 {
		t.Fatalf("expected callback retry count 1, got %v", got)
	}
}
