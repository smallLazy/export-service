package task

import (
	"net/url"
	"strings"
	"testing"
)

func TestRetryRejectsSecondUploadRetryWhileInFlight(t *testing.T) {
	manager := NewManager(t.TempDir(), nil)
	task, _, err := manager.Submit("member_order", url.Values{"time_options": []string{"payment_at"}}, TaskMetadata{RequestID: "req-upload", SourceSystem: "trade"}, 2)
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	if err := manager.MarkUploadFailed(task, "oss failed", 0); err != nil {
		t.Fatalf("mark upload failed: %v", err)
	}
	if _, err := manager.Retry(task.ID, RetryStageUpload); err != nil {
		t.Fatalf("first retry should be accepted: %v", err)
	}
	if _, err := manager.Retry(task.ID, RetryStageUpload); err == nil || !strings.Contains(err.Error(), "already executing") {
		t.Fatalf("second retry should be rejected as executing, got %v", err)
	}
	manager.FinishRetry(task)
}

func TestRetryRejectsSecondCallbackRetryWhileInFlight(t *testing.T) {
	manager := NewManager(t.TempDir(), nil)
	task, _, err := manager.Submit("member_order", url.Values{"time_options": []string{"payment_at"}}, TaskMetadata{RequestID: "req-callback", SourceSystem: "trade"}, 2)
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	if err := manager.MarkCompleted(task, 10, 0); err != nil {
		t.Fatalf("mark completed: %v", err)
	}
	if err := manager.MarkCallbackFailed(task, "callback failed"); err != nil {
		t.Fatalf("mark callback failed: %v", err)
	}
	if _, err := manager.Retry(task.ID, RetryStageCallback); err != nil {
		t.Fatalf("first callback retry should be accepted: %v", err)
	}
	if _, err := manager.Retry(task.ID, RetryStageCallback); err == nil || !strings.Contains(err.Error(), "already executing") {
		t.Fatalf("second callback retry should be rejected as executing, got %v", err)
	}
	manager.FinishRetry(task)
}

func TestCallbackRetryCandidatesFiltersEligibleTasks(t *testing.T) {
	manager := NewManager(t.TempDir(), nil)
	eligible, _, err := manager.Submit("member_order", url.Values{"a": []string{"1"}}, TaskMetadata{RequestID: "eligible", SourceSystem: "trade", CallbackURL: "http://callback"}, 2)
	if err != nil {
		t.Fatalf("submit eligible task: %v", err)
	}
	if err := manager.MarkCompleted(eligible, 1, 0); err != nil {
		t.Fatalf("mark completed: %v", err)
	}
	if err := manager.MarkUploadCompleted(eligible, OSSUploadResult{Key: "k", URL: "u", FileSize: 1}, eligible.CreatedAt); err != nil {
		t.Fatalf("mark upload completed: %v", err)
	}
	if err := manager.MarkCallbackFailed(eligible, "failed"); err != nil {
		t.Fatalf("mark callback failed: %v", err)
	}

	noOSS, _, err := manager.Submit("member_order", url.Values{"a": []string{"2"}}, TaskMetadata{RequestID: "no-oss", SourceSystem: "trade", CallbackURL: "http://callback"}, 2)
	if err != nil {
		t.Fatalf("submit no oss task: %v", err)
	}
	if err := manager.MarkCompleted(noOSS, 1, 0); err != nil {
		t.Fatalf("mark no oss completed: %v", err)
	}
	if err := manager.MarkCallbackFailed(noOSS, "failed"); err != nil {
		t.Fatalf("mark no oss callback failed: %v", err)
	}

	candidates := manager.CallbackRetryCandidates(3)
	if len(candidates) != 1 || candidates[0].ID != eligible.ID {
		t.Fatalf("expected only eligible task, got %#v", candidates)
	}
}

func TestCallbackRetryCandidatesStopsAtMaxRetries(t *testing.T) {
	manager := NewManager(t.TempDir(), nil)
	task, _, err := manager.Submit("member_order", url.Values{"a": []string{"1"}}, TaskMetadata{RequestID: "max", SourceSystem: "trade", CallbackURL: "http://callback"}, 2)
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	if err := manager.MarkCompleted(task, 1, 0); err != nil {
		t.Fatalf("mark completed: %v", err)
	}
	if err := manager.MarkUploadCompleted(task, OSSUploadResult{Key: "k", URL: "u", FileSize: 1}, task.CreatedAt); err != nil {
		t.Fatalf("mark upload completed: %v", err)
	}
	if err := manager.MarkCallbackRetryFailed(task, "failed once"); err != nil {
		t.Fatalf("mark retry failed once: %v", err)
	}
	if len(manager.CallbackRetryCandidates(1)) != 0 {
		t.Fatal("expected no candidates once callback retry count reaches max")
	}
}
