package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func filtersFromRequest(r *http.Request) url.Values {
	values := url.Values{}
	for key, items := range r.URL.Query() {
		values[key] = append([]string(nil), items...)
	}
	return values
}

func taskMetadataFromRequest(r *http.Request, filters url.Values) (TaskMetadata, url.Values) {
	values := r.URL.Query()
	meta := TaskMetadata{
		UserID:       strings.TrimSpace(values.Get("user_id")),
		CompanyUUID:  strings.TrimSpace(values.Get("company_uuid")),
		RequestID:    strings.TrimSpace(values.Get("request_id")),
		FileName:     strings.TrimSpace(values.Get("file_name")),
		CallbackURL:  strings.TrimSpace(values.Get("callback_url")),
		SourceSystem: strings.TrimSpace(values.Get("source_system")),
	}
	exportFilters := cloneFilters(filters)
	for _, key := range []string{"user_id", "request_id", "file_name", "callback_url", "source_system"} {
		delete(exportFilters, key)
	}
	return meta, exportFilters
}

func parseTimeKeyword(values url.Values) (string, string) {
	candidates := []string{"time_keyword[]", "time_keyword"}
	for _, key := range candidates {
		items := values[key]
		if len(items) >= 2 {
			return strings.TrimSpace(items[0]), strings.TrimSpace(items[1])
		}
		if len(items) == 1 && strings.Contains(items[0], ",") {
			parts := strings.SplitN(items[0], ",", 2)
			return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		}
	}
	return "", ""
}

func normalizeSearchOption(option string) string {
	switch strings.TrimSpace(option) {
	case "order_no":
		return "no"
	case "user_id":
		return "user_number"
	default:
		return strings.TrimSpace(option)
	}
}

func parseFilterTimeRange(timeOptions, timeStart, timeEnd string) (string, time.Time, time.Time, error) {
	field := strings.TrimSpace(timeOptions)
	if field == "" {
		field = "payment_at"
	}
	if field != "payment_at" && field != "created_at" {
		return "", time.Time{}, time.Time{}, fmt.Errorf("unsupported time_options %q", field)
	}
	startAt, endAt, err := parseDateRange(timeStart, timeEnd)
	return field, startAt, endAt, err
}

func parseDateRange(start, end string) (time.Time, time.Time, error) {
	var startAt time.Time
	var endAt time.Time
	var err error
	if strings.TrimSpace(start) != "" {
		startAt, err = time.ParseInLocation(dateLayout, start, time.Local)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid time start %q: %w", start, err)
		}
	}
	if strings.TrimSpace(end) != "" {
		endAt, err = time.ParseInLocation(dateLayout, end, time.Local)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid time end %q: %w", end, err)
		}
		endAt = endAt.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}
	if !startAt.IsZero() && !endAt.IsZero() && startAt.After(endAt) {
		return time.Time{}, time.Time{}, errors.New("time start must be before time end")
	}
	return startAt, endAt, nil
}
