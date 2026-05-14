package main

import (
	"net/url"
	"strings"
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
