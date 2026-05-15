package memberorder

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type MemberOrderFilters struct {
	Type                 string
	Options              string
	Keyword              string
	TimeOptions          string
	TimeStart            string
	TimeEnd              string
	Status               string
	PaymentStatus        string
	CompanyUUID          string
	Source               string
	RefundStatus         string
	PaymentMethod        string
	PaymentTransactionNo string
	InstallmentType      string
	GoodsType            string
	MemberUUID           string
	MemberTitle          string
	Amount               string
}

func parseMemberOrderFilters(values url.Values) MemberOrderFilters {
	timeStart, timeEnd := parseTimeKeyword(values)
	timeOptions := strings.TrimSpace(values.Get("time_options"))
	if timeOptions == "" && (values.Get("payment_start") != "" || values.Get("payment_end") != "") {
		timeOptions = "payment_at"
		timeStart = strings.TrimSpace(values.Get("payment_start"))
		timeEnd = strings.TrimSpace(values.Get("payment_end"))
	}

	filters := MemberOrderFilters{
		Type:                 "member",
		Options:              normalizeSearchOption(values.Get("options")),
		Keyword:              strings.TrimSpace(values.Get("keyword")),
		TimeOptions:          timeOptions,
		TimeStart:            timeStart,
		TimeEnd:              timeEnd,
		Status:               strings.TrimSpace(values.Get("status")),
		PaymentStatus:        strings.TrimSpace(values.Get("payment_status")),
		CompanyUUID:          strings.TrimSpace(values.Get("company_uuid")),
		Source:               firstNonEmpty(strings.TrimSpace(values.Get("source")), strings.TrimSpace(values.Get("order_source"))),
		RefundStatus:         strings.TrimSpace(values.Get("refund_status")),
		PaymentMethod:        strings.TrimSpace(values.Get("payment_method")),
		PaymentTransactionNo: strings.TrimSpace(values.Get("payment_transaction_no")),
		InstallmentType:      strings.TrimSpace(values.Get("installment_type")),
		GoodsType:            strings.TrimSpace(values.Get("goods_type")),
		MemberUUID:           strings.TrimSpace(values.Get("member_uuid")),
		MemberTitle:          firstNonEmpty(strings.TrimSpace(values.Get("member_title")), valueWhen(values.Get("options") == "title", strings.TrimSpace(values.Get("keyword")))),
		Amount:               firstNonEmpty(strings.TrimSpace(values.Get("amount")), valueWhen(values.Get("options") == "order_amount", strings.TrimSpace(values.Get("keyword")))),
	}
	if filters.Keyword == "" {
		filters.Options = ""
	}
	if filters.TimeOptions == "" && (filters.TimeStart != "" || filters.TimeEnd != "") {
		filters.TimeOptions = "payment_at"
	}
	return filters
}

const dateLayout = "2006-01-02"

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
