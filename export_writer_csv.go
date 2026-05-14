package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
)

func exportCSVFile(ctx context.Context, path string, exporter Exporter, params url.Values) (total int64, err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	tmpPath := path + ".part"
	file, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}

	committed := false
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("export panicked: %v", recovered)
		}
		if file != nil {
			_ = file.Close()
		}
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	total, err = exporter.StreamCSV(ctx, file, params)
	if err != nil {
		return total, err
	}
	if err := file.Sync(); err != nil {
		return total, err
	}
	if err := file.Close(); err != nil {
		file = nil
		return total, err
	}
	file = nil
	if err := os.Rename(tmpPath, path); err != nil {
		return total, err
	}
	committed = true
	return total, nil
}

func streamCSV(ctx context.Context, target io.Writer, header []string, stream func(context.Context, func([]string) error) (int64, error)) (int64, error) {
	buffered := bufio.NewWriterSize(target, 1024*1024)
	if _, err := buffered.Write([]byte("\xEF\xBB\xBF")); err != nil {
		return 0, err
	}
	writer := csv.NewWriter(buffered)
	if err := writer.Write(header); err != nil {
		return 0, err
	}

	var written int64
	total, err := stream(ctx, func(row []string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := writer.Write(row); err != nil {
			return err
		}
		written++
		if written%500 == 0 {
			writer.Flush()
			return writer.Error()
		}
		return nil
	})
	writer.Flush()
	if flushErr := writer.Error(); flushErr != nil {
		return total, flushErr
	}
	if err != nil {
		return total, err
	}
	if err := buffered.Flush(); err != nil {
		return total, err
	}
	return total, nil
}
