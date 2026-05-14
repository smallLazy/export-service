package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	dateLayout        = "2006-01-02"
	timeLayout        = "2006-01-02 15:04:05"
	exportMemberOrder = "member_order"
)

type App struct {
	registry     *ExportRegistry
	source       string
	exportSem    chan struct{}
	taskManager  *TaskManager
	maxRetries   int
	queryTimeout time.Duration
}

func newApp(runtime *Runtime, maxConcurrency, maxRetries int, queryTimeout time.Duration) *App {
	registry := newExportRegistry()
	registry.Register(&MemberOrderExporter{repo: runtime.MemberOrders})
	app := &App{
		registry:     registry,
		source:       runtime.SourceName,
		taskManager:  newTaskManager(runtime.OutputDir, runtime.TaskStore, runtime.OSSUploader, runtime.CallbackClient, runtime.OSSMaxRetries, runtime.OSSExpireDays),
		maxRetries:   maxRetries,
		queryTimeout: queryTimeout,
	}
	if maxConcurrency > 0 {
		app.exportSem = make(chan struct{}, maxConcurrency)
	}
	return app
}

func (a *App) routes(cfg Config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":     true,
			"source": a.source,
			"env":    cfg.AppEnv,
			"types":  a.registry.Types(),
		})
	})
	mux.HandleFunc("/api/exports/", a.handleUnifiedExport)
	mux.HandleFunc("/api/exports/tasks/", a.handleTaskActions)
	// Deprecated: keep legacy member-order endpoints for compatibility. Prefer /api/exports/member_order/{count|preview|export}.
	mux.HandleFunc("/api/member-orders/count", a.legacyMemberOrderCount)
	mux.HandleFunc("/api/member-orders/preview", a.legacyMemberOrderPreview)
	mux.HandleFunc("/api/member-orders/export", a.legacyMemberOrderExport)
	return mux
}

func (a *App) handleUnifiedExport(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/exports/"), "/"), "/")
	if len(parts) != 2 {
		http.Error(w, "path must be /api/exports/{type}/{count|preview|export}", http.StatusNotFound)
		return
	}

	exporter, err := a.registry.Resolve(parts[0])
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "unsupported export type", err, "export_type", parts[0])
		return
	}
	filters := filtersFromRequest(r)

	switch parts[1] {
	case "count":
		total, err := exporter.Count(r.Context(), filters)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to count export data", err, "export_type", exporter.Type())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"total": total, "filters": filters})
	case "preview":
		preview, err := exporter.Preview(r.Context(), filters, previewLimit(r))
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to preview export data", err, "export_type", exporter.Type())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"preview": preview, "filters": filters})
	case "export":
		meta, exportFilters := taskMetadataFromRequest(r, filters)
		a.handleSubmitExport(w, r, exporter, exportFilters, meta)
	case "submit":
		meta, exportFilters := taskMetadataFromRequest(r, filters)
		a.handleSubmitExport(w, r, exporter, exportFilters, meta)
	default:
		http.Error(w, "action must be count, preview, export, or submit", http.StatusNotFound)
	}
}

func (a *App) legacyMemberOrderCount(w http.ResponseWriter, r *http.Request) {
	logDeprecatedEndpoint(r)
	exporter, _ := a.registry.Resolve(exportMemberOrder)
	total, err := exporter.Count(r.Context(), filtersFromRequest(r))
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, "failed to count export data", err, "export_type", exporter.Type())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total})
}

func (a *App) legacyMemberOrderPreview(w http.ResponseWriter, r *http.Request) {
	logDeprecatedEndpoint(r)
	exporter, _ := a.registry.Resolve(exportMemberOrder)
	rows, err := exporter.Preview(r.Context(), filtersFromRequest(r), previewLimit(r))
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, "failed to preview export data", err, "export_type", exporter.Type())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"preview": rows})
}

func (a *App) legacyMemberOrderExport(w http.ResponseWriter, r *http.Request) {
	logDeprecatedEndpoint(r)
	exporter, _ := a.registry.Resolve(exportMemberOrder)
	filters := filtersFromRequest(r)
	meta, exportFilters := taskMetadataFromRequest(r, filters)
	a.handleSubmitExport(w, r, exporter, exportFilters, meta)
}

func (a *App) handleSubmitExport(w http.ResponseWriter, r *http.Request, exporter Exporter, filters url.Values, meta TaskMetadata) {
	if task, ok := a.taskManager.find(exporter.Type(), filters, meta); ok {
		writeJSON(w, http.StatusAccepted, task.snapshot())
		return
	}

	release, ok := a.tryAcquireExport(r.Context())
	if !ok {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "too many concurrent exports"})
		return
	}

	task, isNew, err := a.taskManager.submit(exporter.Type(), filters, meta, a.maxRetries)
	if err != nil {
		release()
		writeHTTPError(w, r, http.StatusInternalServerError, "failed to submit export task", err, "export_type", exporter.Type())
		return
	}
	if !isNew {
		release()
		writeJSON(w, http.StatusAccepted, task.snapshot())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.queryTimeout)
	go func() {
		defer cancel()
		defer release()
		a.taskManager.execute(ctx, task, func(ctx context.Context) (int64, error) {
			return exportCSVFile(ctx, task.OutputFile, exporter, filters)
		})
	}()
	writeJSON(w, http.StatusAccepted, task.snapshot())
}

func (a *App) handleTaskActions(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/exports/tasks/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "task id required", http.StatusNotFound)
		return
	}
	taskID := parts[0]
	action := ""
	if len(parts) >= 2 {
		action = parts[1]
	}

	switch {
	case action == "" && r.Method == http.MethodGet:
		task, err := a.taskManager.get(taskID)
		if err != nil {
			writeHTTPError(w, r, http.StatusNotFound, "export task not found", err, "task_id", taskID)
			return
		}
		writeJSON(w, http.StatusOK, task.snapshot())
	case action == "retry" && r.Method == http.MethodPost:
		existing, err := a.taskManager.get(taskID)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "export task not found", err, "task_id", taskID)
			return
		}
		exporter, err := a.registry.Resolve(existing.Type)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to resolve export type", err, "task_id", taskID, "export_type", existing.Type)
			return
		}
		release, ok := a.tryAcquireExport(r.Context())
		if !ok {
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "too many concurrent exports"})
			return
		}
		task, err := a.taskManager.retry(taskID)
		if err != nil {
			release()
			writeHTTPError(w, r, http.StatusBadRequest, "failed to retry export task", err, "task_id", taskID)
			return
		}
		filters := task.filtersSnapshot()
		ctx, cancel := context.WithTimeout(context.Background(), a.queryTimeout)
		go func() {
			defer cancel()
			defer release()
			a.taskManager.execute(ctx, task, func(ctx context.Context) (int64, error) {
				return exportCSVFile(ctx, task.OutputFile, exporter, filters)
			})
		}()
		writeJSON(w, http.StatusAccepted, task.snapshot())
	case action == "cancel" && r.Method == http.MethodPost:
		task, err := a.taskManager.cancel(taskID)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "failed to cancel export task", err, "task_id", taskID)
			return
		}
		writeJSON(w, http.StatusOK, task.snapshot())
	case action == "download" && r.Method == http.MethodGet:
		task, err := a.taskManager.get(taskID)
		if err != nil {
			writeHTTPError(w, r, http.StatusNotFound, "export task not found", err, "task_id", taskID)
			return
		}
		if task.State != TaskCompleted {
			http.Error(w, fmt.Sprintf("task is %s, not completed", task.State), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(task.OutputFile)))
		http.ServeFile(w, r, task.OutputFile)
	default:
		http.Error(w, "unknown task action", http.StatusNotFound)
	}
}

func (a *App) tryAcquireExport(ctx context.Context) (release func(), ok bool) {
	if a.exportSem == nil {
		return func() {}, true
	}
	select {
	case a.exportSem <- struct{}{}:
		return func() { <-a.exportSem }, true
	case <-ctx.Done():
		return nil, false
	}
}

func previewLimit(r *http.Request) int {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 200 {
		return 20
	}
	return limit
}

func defaultOutputPath(outputDir, exportType string) string {
	dir := firstNonEmpty(strings.TrimSpace(outputDir), ".")
	return filepath.Join(dir, fmt.Sprintf("%s_%s.csv", exportType, time.Now().Format("20060102_150405")))
}

func money(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(timeLayout)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeHTTPError(w http.ResponseWriter, r *http.Request, status int, message string, err error, attrs ...any) {
	logAttrs := []any{
		"status", status,
		"method", r.Method,
		"path", r.URL.Path,
		"error", err,
	}
	logAttrs = append(logAttrs, attrs...)
	slog.Error("http request failed", logAttrs...)
	http.Error(w, message, status)
}

func logDeprecatedEndpoint(r *http.Request) {
	slog.Warn("deprecated export endpoint used", "method", r.Method, "path", r.URL.Path, "replacement", "/api/exports/member_order/{count|preview|export}")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func valueWhen(ok bool, value string) string {
	if ok {
		return value
	}
	return ""
}

func defaultInt(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
