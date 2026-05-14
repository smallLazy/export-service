package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

type cliFilterValues []string

func (v *cliFilterValues) String() string {
	return strings.Join(*v, ",")
}

func (v *cliFilterValues) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if !strings.Contains(value, "=") {
		return fmt.Errorf("filter must be key=value, got %q", value)
	}
	*v = append(*v, value)
	return nil
}

func filtersFromCLI(rawQuery, rawParams string, pairs cliFilterValues) (url.Values, error) {
	values := url.Values{}

	if strings.TrimSpace(rawQuery) != "" {
		parsed, err := url.ParseQuery(strings.TrimSpace(rawQuery))
		if err != nil {
			return nil, err
		}
		mergeValues(values, parsed)
	}

	if strings.TrimSpace(rawParams) != "" {
		parsed, err := valuesFromJSON(strings.TrimSpace(rawParams))
		if err != nil {
			return nil, err
		}
		mergeValues(values, parsed)
	}

	for _, pair := range pairs {
		key, value, _ := strings.Cut(pair, "=")
		values.Add(strings.TrimSpace(key), strings.TrimSpace(value))
	}

	return values, nil
}

func valuesFromJSON(raw string) (url.Values, error) {
	var params map[string]any
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		return nil, err
	}

	values := url.Values{}
	for key, value := range params {
		appendJSONValue(values, key, value)
	}
	return values, nil
}

func appendJSONValue(values url.Values, key string, value any) {
	switch typed := value.(type) {
	case nil:
		return
	case []any:
		for _, item := range typed {
			appendJSONValue(values, key, item)
		}
	default:
		values.Add(key, fmt.Sprint(typed))
	}
}

func mergeValues(target, source url.Values) {
	for key, items := range source {
		for _, item := range items {
			target.Add(key, item)
		}
	}
}
