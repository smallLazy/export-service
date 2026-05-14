package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/url"
	"strings"
	"time"
)

type MemberOrder struct {
	ID                      int64
	UserNumber              string
	UserNickname            string
	Mobile                  string
	OrderNo                 string
	Amount                  float64
	DiscountAmount          float64
	PaymentContributeAmount float64
	CouponAmount            float64
	ReceiptAmount           float64
	Status                  string
	PaymentStatus           string
	PaymentMethod           string
	PaymentTransactionNo    string
	RefundStatus            string
	RefundAmount            float64
	Source                  string
	CompanyUUID             string
	GoodsType               string
	MemberUUID              string
	MemberTitle             string
	RegisterChannelLink     string
	SourceOwner             string
	Attribution             string
	PaymentAt               time.Time
	CreatedAt               time.Time
}

type MemberOrderRepository interface {
	Count(ctx context.Context, filters MemberOrderFilters) (int64, error)
	Preview(ctx context.Context, filters MemberOrderFilters, limit int) ([]MemberOrder, error)
	Stream(ctx context.Context, filters MemberOrderFilters, consumer func(MemberOrder) error) (int64, error)
}

type MemberOrderExporter struct {
	repo MemberOrderRepository
}

type MemoryMemberOrderRepository struct {
	orders []MemberOrder
}

type MySQLMemberOrderRepository struct {
	db        *sql.DB
	batchSize int
	tables    tableNames
}

type orderCursor struct {
	ID   int64
	UUID string
}

func (e *MemberOrderExporter) Type() string {
	return exportMemberOrder
}

func (e *MemberOrderExporter) Aliases() []string {
	return []string{"member", "member_order", "member_orders", "member-order", "member-orders"}
}

func (e *MemberOrderExporter) Count(ctx context.Context, params url.Values) (int64, error) {
	return e.repo.Count(ctx, parseMemberOrderFilters(params))
}

func (e *MemberOrderExporter) Preview(ctx context.Context, params url.Values, limit int) ([]map[string]any, error) {
	orders, err := e.repo.Preview(ctx, parseMemberOrderFilters(params), limit)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(orders))
	for _, order := range orders {
		rows = append(rows, map[string]any{
			"id":             order.ID,
			"user_number":    order.UserNumber,
			"order_no":       order.OrderNo,
			"user_nickname":  order.UserNickname,
			"mobile":         order.Mobile,
			"payment_at":     formatTime(order.PaymentAt),
			"created_at":     formatTime(order.CreatedAt),
			"amount":         fmt.Sprintf("%.2f", order.Amount),
			"receipt_amount": fmt.Sprintf("%.2f", order.ReceiptAmount),
			"status":         order.Status,
			"payment_status": order.PaymentStatus,
			"member_title":   order.MemberTitle,
		})
	}
	return rows, nil
}

func (e *MemberOrderExporter) StreamCSV(ctx context.Context, target io.Writer, params url.Values) (int64, error) {
	filters := parseMemberOrderFilters(params)
	startedAt := time.Now()
	var written int64
	return streamCSV(ctx, target, memberOrderHeader, func(ctx context.Context, writeRow func([]string) error) (int64, error) {
		return e.repo.Stream(ctx, filters, func(order MemberOrder) error {
			if err := writeRow(memberOrderRow(order)); err != nil {
				return err
			}
			written++
			if written%1000 == 0 {
				slog.Info("export progress", "export_type", e.Type(), "status", "running", "rows", written, "duration", time.Since(startedAt).Round(time.Second))
			}
			return nil
		})
	})
}

func (r *MemoryMemberOrderRepository) Count(_ context.Context, filters MemberOrderFilters) (int64, error) {
	matched, err := r.filtered(filters)
	if err != nil {
		return 0, err
	}
	return int64(len(matched)), nil
}

func (r *MemoryMemberOrderRepository) Preview(_ context.Context, filters MemberOrderFilters, limit int) ([]MemberOrder, error) {
	matched, err := r.filtered(filters)
	if err != nil {
		return nil, err
	}
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched, nil
}

func (r *MemoryMemberOrderRepository) Stream(_ context.Context, filters MemberOrderFilters, consumer func(MemberOrder) error) (int64, error) {
	matched, err := r.filtered(filters)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, order := range matched {
		if err := consumer(order); err != nil {
			return total, err
		}
		total++
	}
	return total, nil
}

func (r *MemoryMemberOrderRepository) filtered(filters MemberOrderFilters) ([]MemberOrder, error) {
	timeField, startAt, endAt, err := parseFilterTimeRange(filters.TimeOptions, filters.TimeStart, filters.TimeEnd)
	if err != nil {
		return nil, err
	}
	matched := make([]MemberOrder, 0, len(r.orders))
	for _, order := range r.orders {
		if !matchTimeRange(order, timeField, startAt, endAt) {
			continue
		}
		if !matchMemberOrderFields(order, filters) {
			continue
		}
		matched = append(matched, order)
	}
	return matched, nil
}

func (r *MySQLMemberOrderRepository) Count(ctx context.Context, filters MemberOrderFilters) (int64, error) {
	timeField, startAt, endAt, err := parseFilterTimeRange(filters.TimeOptions, filters.TimeStart, filters.TimeEnd)
	if err != nil {
		return 0, err
	}
	query, args := r.buildCountSQL(filters, timeField, startAt, endAt)
	var total int64
	err = r.db.QueryRowContext(ctx, query, args...).Scan(&total)
	return total, err
}

func (r *MySQLMemberOrderRepository) Preview(ctx context.Context, filters MemberOrderFilters, limit int) ([]MemberOrder, error) {
	timeField, startAt, endAt, err := parseFilterTimeRange(filters.TimeOptions, filters.TimeStart, filters.TimeEnd)
	if err != nil {
		return nil, err
	}
	query, args := r.buildBaseSQL(filters, timeField, startAt, endAt, false)
	query += "\nLIMIT ?"
	args = append(args, limit)
	return r.queryOrders(ctx, query, args, limit)
}

func (r *MySQLMemberOrderRepository) Stream(ctx context.Context, filters MemberOrderFilters, consumer func(MemberOrder) error) (int64, error) {
	timeField, startAt, endAt, err := parseFilterTimeRange(filters.TimeOptions, filters.TimeStart, filters.TimeEnd)
	if err != nil {
		return 0, err
	}
	startedAt := time.Now()
	slog.Info("member order stream started", "export_type", exportMemberOrder, "status", "running", "batch_size", r.batchSize)
	defer func() {
		slog.Info("member order stream finished", "export_type", exportMemberOrder, "status", "finished", "batch_size", r.batchSize, "duration", time.Since(startedAt))
	}()
	if canUseOrderIDPage(filters) {
		return r.streamByOrderIDPage(ctx, filters, timeField, startAt, endAt, consumer)
	}
	return r.streamByComplexOrderPage(ctx, filters, timeField, startAt, endAt, consumer)
}

func (r *MySQLMemberOrderRepository) streamByOrderIDPage(ctx context.Context, filters MemberOrderFilters, timeField string, startAt, endAt time.Time, consumer func(MemberOrder) error) (int64, error) {
	var total int64
	var lastID int64

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}

		page, err := r.queryOrderPage(ctx, filters, timeField, startAt, endAt, lastID)
		if err != nil {
			return total, err
		}
		if len(page) == 0 {
			break
		}

		orders, err := r.queryOrdersByPage(ctx, page)
		if err != nil {
			return total, err
		}
		for _, order := range orders {
			select {
			case <-ctx.Done():
				return total, ctx.Err()
			default:
			}
			if err := consumer(order); err != nil {
				return total, err
			}
			total++
		}

		lastID = page[len(page)-1].ID
		if len(page) < r.batchSize {
			break
		}
	}
	return total, nil
}

func (r *MySQLMemberOrderRepository) streamByComplexOrderPage(ctx context.Context, filters MemberOrderFilters, timeField string, startAt, endAt time.Time, consumer func(MemberOrder) error) (int64, error) {
	var total int64
	var lastID int64

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}

		page, err := r.queryComplexOrderPage(ctx, filters, timeField, startAt, endAt, lastID)
		if err != nil {
			return total, err
		}
		if len(page) == 0 {
			break
		}

		orders, err := r.queryOrdersByPage(ctx, page)
		if err != nil {
			return total, err
		}
		for _, order := range orders {
			select {
			case <-ctx.Done():
				return total, ctx.Err()
			default:
			}
			if err := consumer(order); err != nil {
				return total, err
			}
			total++
		}

		lastID = page[len(page)-1].ID
		if len(page) < r.batchSize {
			break
		}
	}
	return total, nil
}

func (r *MySQLMemberOrderRepository) queryOrderPage(ctx context.Context, filters MemberOrderFilters, timeField string, startAt, endAt time.Time, lastID int64) ([]orderCursor, error) {
	where, args := r.buildOrderPageWhereSQL(filters, timeField, startAt, endAt)
	if lastID > 0 {
		where += "\n  AND o.id > ?"
		args = append(args, lastID)
	}
	query := fmt.Sprintf(`
SELECT o.id, o.uuid
FROM %s o
LEFT JOIN %s u ON u.uuid = o.user_uuid
`, r.tables.Orders, r.tables.ViewUsers) + where + `
ORDER BY o.id ASC
LIMIT ?`
	args = append(args, r.batchSize)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	page := make([]orderCursor, 0, r.batchSize)
	for rows.Next() {
		var item orderCursor
		if err := rows.Scan(&item.ID, &item.UUID); err != nil {
			return nil, err
		}
		page = append(page, item)
	}
	return page, rows.Err()
}

func (r *MySQLMemberOrderRepository) queryComplexOrderPage(ctx context.Context, filters MemberOrderFilters, timeField string, startAt, endAt time.Time, lastID int64) ([]orderCursor, error) {
	where, args := r.buildComplexOrderPageWhereSQL(filters, timeField, startAt, endAt)
	if lastID > 0 {
		where += "\n  AND o.id > ?"
		args = append(args, lastID)
	}
	query := fmt.Sprintf(`
SELECT o.id, o.uuid
FROM %s o
LEFT JOIN %s u ON u.uuid = o.user_uuid
`, r.tables.Orders, r.tables.ViewUsers) + where + `
ORDER BY o.id ASC
LIMIT ?`
	args = append(args, r.batchSize)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	page := make([]orderCursor, 0, r.batchSize)
	for rows.Next() {
		var item orderCursor
		if err := rows.Scan(&item.ID, &item.UUID); err != nil {
			return nil, err
		}
		page = append(page, item)
	}
	return page, rows.Err()
}

func (r *MySQLMemberOrderRepository) queryOrdersByPage(ctx context.Context, page []orderCursor) ([]MemberOrder, error) {
	if len(page) == 0 {
		return nil, nil
	}

	idPlaceholders := placeholders(len(page))
	uuidPlaceholders := placeholders(len(page))
	ids := make([]any, 0, len(page))
	uuids := make([]any, 0, len(page))
	for _, item := range page {
		ids = append(ids, item.ID)
		uuids = append(uuids, item.UUID)
	}

	query := fmt.Sprintf(`
SELECT
    o.id,
    COALESCE(u.number, '') AS user_number,
    COALESCE(u.nickname, '') AS user_nickname,
    COALESCE(u.mobile, '') AS mobile,
    COALESCE(o.no, '') AS order_no,
    COALESCE(o.amount, 0) AS amount,
    COALESCE(o.discount_amount, 0) AS discount_amount,
    COALESCE(o.payment_contribute_amount, 0) AS payment_contribute_amount,
    COALESCE(o.coupon_amount, 0) AS coupon_amount,
    COALESCE(o.receipt_amount, 0) AS receipt_amount,
    COALESCE(o.status, '') AS status,
    COALESCE(o.payment_status, '') AS payment_status,
    COALESCE(oi.payment_method, '') AS payment_method,
    COALESCE(oi.payment_transaction_no, '') AS payment_transaction_no,
    COALESCE(oi.refund_status, '') AS refund_status,
    COALESCE(o.refund_amount, 0) AS refund_amount,
    COALESCE(o.source, '') AS source,
    COALESCE(o.company_uuid, '') AS company_uuid,
    COALESCE(o.goods_type, '') AS goods_type,
    COALESCE(member_info.member_uuid, '') AS member_uuid,
    COALESCE(member_info.member_title, '') AS member_title,
    TRIM(BOTH ' -' FROM CONCAT(COALESCE(uc.name, ''), ' - ', COALESCE(ul.name, ''))) AS register_channel_link,
    COALESCE(ml.name, '') AS source_owner,
    TRIM(BOTH ' -' FROM CONCAT(
        COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(o.extra, '$.attribution_channel_name')), 'null'), mc.name, ''),
        ' - ',
        COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(o.extra, '$.attribution_link_name')), 'null'), '')
    )) AS attribution,
    o.payment_at,
    o.created_at
FROM %s o
LEFT JOIN %s u ON u.uuid = o.user_uuid
LEFT JOIN %s uc ON uc.uuid = u.channel_uuid
LEFT JOIN %s ul ON ul.uuid = u.link_uuid
LEFT JOIN %s mc ON mc.uuid = o.channel_uuid
LEFT JOIN %s ml ON ml.uuid = o.link_uuid
LEFT JOIN %s oi ON oi.order_uuid = o.uuid
LEFT JOIN %s member_info ON member_info.order_uuid = o.uuid
WHERE o.id IN (%s)
ORDER BY o.id ASC
`, r.tables.Orders, r.tables.ViewUsers, r.tables.MarketChannels, r.tables.MarketLinks, r.tables.MarketChannels, r.tables.MarketLinks, r.installmentAggSQLForUUIDs(uuidPlaceholders), r.memberAggSQLForUUIDs(uuidPlaceholders), idPlaceholders)

	args := make([]any, 0, len(uuids)*2+len(ids))
	args = append(args, uuids...)
	args = append(args, uuids...)
	args = append(args, ids...)
	return r.queryOrders(ctx, query, args, len(page))
}

func (r *MySQLMemberOrderRepository) queryOrders(ctx context.Context, query string, args []any, capHint int) ([]MemberOrder, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orders := make([]MemberOrder, 0, capHint)
	for rows.Next() {
		order, err := scanMemberOrder(rows)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	return orders, rows.Err()
}

func scanMemberOrder(rows *sql.Rows) (MemberOrder, error) {
	var order MemberOrder
	var paymentAt sql.NullTime
	var createdAt sql.NullTime
	err := rows.Scan(
		&order.ID,
		&order.UserNumber,
		&order.UserNickname,
		&order.Mobile,
		&order.OrderNo,
		&order.Amount,
		&order.DiscountAmount,
		&order.PaymentContributeAmount,
		&order.CouponAmount,
		&order.ReceiptAmount,
		&order.Status,
		&order.PaymentStatus,
		&order.PaymentMethod,
		&order.PaymentTransactionNo,
		&order.RefundStatus,
		&order.RefundAmount,
		&order.Source,
		&order.CompanyUUID,
		&order.GoodsType,
		&order.MemberUUID,
		&order.MemberTitle,
		&order.RegisterChannelLink,
		&order.SourceOwner,
		&order.Attribution,
		&paymentAt,
		&createdAt,
	)
	if paymentAt.Valid {
		order.PaymentAt = paymentAt.Time
	}
	if createdAt.Valid {
		order.CreatedAt = createdAt.Time
	}
	return order, err
}

func (r *MySQLMemberOrderRepository) buildCountSQL(filters MemberOrderFilters, timeField string, startAt, endAt time.Time) (string, []any) {
	if canUseOrderIDPage(filters) {
		where, args := r.buildOrderPageWhereSQL(filters, timeField, startAt, endAt)
		query := fmt.Sprintf(`
SELECT COUNT(DISTINCT o.id)
FROM %s o
LEFT JOIN %s u ON u.uuid = o.user_uuid
`, r.tables.Orders, r.tables.ViewUsers) + where
		return query, args
	}

	where, args := r.buildWhereSQL(filters, timeField, startAt, endAt)
	query := fmt.Sprintf(`
SELECT COUNT(DISTINCT o.id)
FROM %s o
LEFT JOIN %s u ON u.uuid = o.user_uuid
LEFT JOIN %s uc ON uc.uuid = u.channel_uuid
LEFT JOIN %s ul ON ul.uuid = u.link_uuid
LEFT JOIN %s mc ON mc.uuid = o.channel_uuid
LEFT JOIN %s ml ON ml.uuid = o.link_uuid
LEFT JOIN %s oi ON oi.order_uuid = o.uuid
LEFT JOIN %s member_info ON member_info.order_uuid = o.uuid
`, r.tables.Orders, r.tables.ViewUsers, r.tables.MarketChannels, r.tables.MarketLinks, r.tables.MarketChannels, r.tables.MarketLinks, r.installmentAggSQL(), r.memberAggSQL()) + where
	return query, args
}

func (r *MySQLMemberOrderRepository) buildBaseSQL(filters MemberOrderFilters, timeField string, startAt, endAt time.Time, forStream bool) (string, []any) {
	where, args := r.buildWhereSQL(filters, timeField, startAt, endAt)

	if forStream {
		query := fmt.Sprintf(`
SELECT o.id, o.uuid
FROM %s o
LEFT JOIN %s u ON u.uuid = o.user_uuid
LEFT JOIN %s uc ON uc.uuid = u.channel_uuid
LEFT JOIN %s ul ON ul.uuid = u.link_uuid
LEFT JOIN %s mc ON mc.uuid = o.channel_uuid
LEFT JOIN %s ml ON ml.uuid = o.link_uuid
LEFT JOIN %s oi ON oi.order_uuid = o.uuid
LEFT JOIN %s member_info ON member_info.order_uuid = o.uuid
`, r.tables.Orders, r.tables.ViewUsers, r.tables.MarketChannels, r.tables.MarketLinks, r.tables.MarketChannels, r.tables.MarketLinks, r.installmentAggSQL(), r.memberAggSQL()) + where
		return query, args
	}

	query := fmt.Sprintf(`
SELECT
    o.id,
    COALESCE(u.number, '') AS user_number,
    COALESCE(u.nickname, '') AS user_nickname,
    COALESCE(u.mobile, '') AS mobile,
    COALESCE(o.no, '') AS order_no,
    COALESCE(o.amount, 0) AS amount,
    COALESCE(o.discount_amount, 0) AS discount_amount,
    COALESCE(o.payment_contribute_amount, 0) AS payment_contribute_amount,
    COALESCE(o.coupon_amount, 0) AS coupon_amount,
    COALESCE(o.receipt_amount, 0) AS receipt_amount,
    COALESCE(o.status, '') AS status,
    COALESCE(o.payment_status, '') AS payment_status,
    COALESCE(oi.payment_method, '') AS payment_method,
    COALESCE(oi.payment_transaction_no, '') AS payment_transaction_no,
    COALESCE(oi.refund_status, '') AS refund_status,
    COALESCE(o.refund_amount, 0) AS refund_amount,
    COALESCE(o.source, '') AS source,
    COALESCE(o.company_uuid, '') AS company_uuid,
    COALESCE(o.goods_type, '') AS goods_type,
    COALESCE(member_info.member_uuid, '') AS member_uuid,
    COALESCE(member_info.member_title, '') AS member_title,
    TRIM(BOTH ' -' FROM CONCAT(COALESCE(uc.name, ''), ' - ', COALESCE(ul.name, ''))) AS register_channel_link,
    COALESCE(ml.name, '') AS source_owner,
    TRIM(BOTH ' -' FROM CONCAT(
        COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(o.extra, '$.attribution_channel_name')), 'null'), mc.name, ''),
        ' - ',
        COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(o.extra, '$.attribution_link_name')), 'null'), '')
    )) AS attribution,
    o.payment_at,
    o.created_at
FROM %s o
LEFT JOIN %s u ON u.uuid = o.user_uuid
LEFT JOIN %s uc ON uc.uuid = u.channel_uuid
LEFT JOIN %s ul ON ul.uuid = u.link_uuid
LEFT JOIN %s mc ON mc.uuid = o.channel_uuid
LEFT JOIN %s ml ON ml.uuid = o.link_uuid
LEFT JOIN %s oi ON oi.order_uuid = o.uuid
LEFT JOIN %s member_info ON member_info.order_uuid = o.uuid
`, r.tables.Orders, r.tables.ViewUsers, r.tables.MarketChannels, r.tables.MarketLinks, r.tables.MarketChannels, r.tables.MarketLinks, r.installmentAggSQL(), r.memberAggSQL()) + where + "\nORDER BY o.id DESC"
	return query, args
}

func (r *MySQLMemberOrderRepository) installmentAggSQL() string {
	return fmt.Sprintf(`(
    SELECT
        order_uuid,
        SUBSTRING_INDEX(GROUP_CONCAT(payment_method ORDER BY id ASC), ',', 1) AS payment_method,
        GROUP_CONCAT(DISTINCT payment_transaction_no ORDER BY id ASC SEPARATOR ',') AS payment_transaction_no,
        SUBSTRING_INDEX(GROUP_CONCAT(COALESCE(refund_status, '') ORDER BY id DESC), ',', 1) AS refund_status,
        SUBSTRING_INDEX(GROUP_CONCAT(COALESCE(type, '') ORDER BY id ASC), ',', 1) AS installment_type
    FROM %s
    WHERE deleted_at IS NULL
    GROUP BY order_uuid
)`, r.tables.OrderInstallments)
}

func (r *MySQLMemberOrderRepository) memberAggSQL() string {
	return fmt.Sprintf(`(
    SELECT
        item.order_uuid,
        SUBSTRING_INDEX(GROUP_CONCAT(v.uuid ORDER BY item.id ASC), ',', 1) AS member_uuid,
        GROUP_CONCAT(DISTINCT v.title ORDER BY item.id ASC SEPARATOR ';') AS member_title
    FROM %s item
    INNER JOIN %s v
        ON v.uuid = item.model_uuid
       AND v.deleted_at IS NULL
    WHERE item.deleted_at IS NULL
      AND item.model_type = 'App\\Models\\ServiceVip'
    GROUP BY item.order_uuid
)`, r.tables.OrderItems, r.tables.ViewServiceVIPs)
}

func (r *MySQLMemberOrderRepository) installmentAggSQLForUUIDs(uuidPlaceholders string) string {
	return fmt.Sprintf(`(
    SELECT
        order_uuid,
        SUBSTRING_INDEX(GROUP_CONCAT(payment_method ORDER BY id ASC), ',', 1) AS payment_method,
        GROUP_CONCAT(DISTINCT payment_transaction_no ORDER BY id ASC SEPARATOR ',') AS payment_transaction_no,
        SUBSTRING_INDEX(GROUP_CONCAT(COALESCE(refund_status, '') ORDER BY id DESC), ',', 1) AS refund_status,
        SUBSTRING_INDEX(GROUP_CONCAT(COALESCE(type, '') ORDER BY id ASC), ',', 1) AS installment_type
    FROM %s
    WHERE deleted_at IS NULL
      AND order_uuid IN (%s)
    GROUP BY order_uuid
)`, r.tables.OrderInstallments, uuidPlaceholders)
}

func (r *MySQLMemberOrderRepository) memberAggSQLForUUIDs(uuidPlaceholders string) string {
	return fmt.Sprintf(`(
    SELECT
        item.order_uuid,
        SUBSTRING_INDEX(GROUP_CONCAT(v.uuid ORDER BY item.id ASC), ',', 1) AS member_uuid,
        GROUP_CONCAT(DISTINCT v.title ORDER BY item.id ASC SEPARATOR ';') AS member_title
    FROM %s item
    INNER JOIN %s v
        ON v.uuid = item.model_uuid
       AND v.deleted_at IS NULL
    WHERE item.deleted_at IS NULL
      AND item.model_type = 'App\\Models\\ServiceVip'
      AND item.order_uuid IN (%s)
    GROUP BY item.order_uuid
)`, r.tables.OrderItems, r.tables.ViewServiceVIPs, uuidPlaceholders)
}

func (r *MySQLMemberOrderRepository) buildOrderPageWhereSQL(filters MemberOrderFilters, timeField string, startAt, endAt time.Time) (string, []any) {
	query := "\nWHERE o.deleted_at IS NULL\n  AND o.type = 'member'"
	args := make([]any, 0, 12)

	if !startAt.IsZero() {
		query += fmt.Sprintf("\n  AND o.%s >= ?", timeField)
		args = append(args, startAt)
	}
	if !endAt.IsZero() {
		query += fmt.Sprintf("\n  AND o.%s <= ?", timeField)
		args = append(args, endAt)
	}
	if filters.Status != "" {
		query += "\n  AND o.status = ?"
		args = append(args, filters.Status)
	}
	if filters.PaymentStatus != "" {
		query += "\n  AND o.payment_status = ?"
		args = append(args, filters.PaymentStatus)
	}
	if filters.CompanyUUID != "" {
		query += "\n  AND o.company_uuid = ?"
		args = append(args, filters.CompanyUUID)
	}
	if filters.Source != "" {
		query += "\n  AND o.source = ?"
		args = append(args, filters.Source)
	}
	if filters.GoodsType != "" {
		query += "\n  AND COALESCE(o.goods_type, '') = ?"
		args = append(args, filters.GoodsType)
	}
	if filters.Amount != "" {
		query += "\n  AND o.amount = ?"
		args = append(args, filters.Amount)
	}

	return appendOrderPageKeywordSQL(query, args, filters)
}

func (r *MySQLMemberOrderRepository) buildWhereSQL(filters MemberOrderFilters, timeField string, startAt, endAt time.Time) (string, []any) {
	query := "\nWHERE o.deleted_at IS NULL\n  AND o.type = 'member'"
	args := make([]any, 0, 20)

	if !startAt.IsZero() {
		query += fmt.Sprintf("\n  AND o.%s >= ?", timeField)
		args = append(args, startAt)
	}
	if !endAt.IsZero() {
		query += fmt.Sprintf("\n  AND o.%s <= ?", timeField)
		args = append(args, endAt)
	}
	if filters.Status != "" {
		query += "\n  AND o.status = ?"
		args = append(args, filters.Status)
	}
	if filters.PaymentStatus != "" {
		query += "\n  AND o.payment_status = ?"
		args = append(args, filters.PaymentStatus)
	}
	if filters.CompanyUUID != "" {
		query += "\n  AND o.company_uuid = ?"
		args = append(args, filters.CompanyUUID)
	}
	if filters.Source != "" {
		query += "\n  AND o.source = ?"
		args = append(args, filters.Source)
	}
	if filters.RefundStatus != "" {
		query += "\n  AND COALESCE(oi.refund_status, '') = ?"
		args = append(args, filters.RefundStatus)
	}
	if filters.PaymentMethod != "" {
		query += "\n  AND COALESCE(oi.payment_method, '') = ?"
		args = append(args, filters.PaymentMethod)
	}
	if filters.PaymentTransactionNo != "" {
		query += "\n  AND COALESCE(oi.payment_transaction_no, '') LIKE ?"
		args = append(args, "%"+filters.PaymentTransactionNo+"%")
	}
	if filters.InstallmentType != "" {
		query += "\n  AND COALESCE(oi.installment_type, '') = ?"
		args = append(args, filters.InstallmentType)
	}
	if filters.GoodsType != "" {
		query += "\n  AND COALESCE(o.goods_type, '') = ?"
		args = append(args, filters.GoodsType)
	}
	if filters.MemberUUID != "" {
		query += "\n  AND COALESCE(member_info.member_uuid, '') = ?"
		args = append(args, filters.MemberUUID)
	}
	if filters.MemberTitle != "" {
		query += "\n  AND COALESCE(member_info.member_title, '') LIKE ?"
		args = append(args, "%"+filters.MemberTitle+"%")
	}
	if filters.Amount != "" {
		query += "\n  AND o.amount = ?"
		args = append(args, filters.Amount)
	}

	query, args = appendMemberOrderKeywordSQL(query, args, filters)
	return query, args
}

func (r *MySQLMemberOrderRepository) buildComplexOrderPageWhereSQL(filters MemberOrderFilters, timeField string, startAt, endAt time.Time) (string, []any) {
	query := "\nWHERE o.deleted_at IS NULL\n  AND o.type = 'member'"
	args := make([]any, 0, 24)

	if !startAt.IsZero() {
		query += fmt.Sprintf("\n  AND o.%s >= ?", timeField)
		args = append(args, startAt)
	}
	if !endAt.IsZero() {
		query += fmt.Sprintf("\n  AND o.%s <= ?", timeField)
		args = append(args, endAt)
	}
	if filters.Status != "" {
		query += "\n  AND o.status = ?"
		args = append(args, filters.Status)
	}
	if filters.PaymentStatus != "" {
		query += "\n  AND o.payment_status = ?"
		args = append(args, filters.PaymentStatus)
	}
	if filters.CompanyUUID != "" {
		query += "\n  AND o.company_uuid = ?"
		args = append(args, filters.CompanyUUID)
	}
	if filters.Source != "" {
		query += "\n  AND o.source = ?"
		args = append(args, filters.Source)
	}
	if filters.RefundStatus != "" {
		query += fmt.Sprintf(`
  AND COALESCE((
    SELECT COALESCE(oi_filter.refund_status, '')
    FROM %s oi_filter
    WHERE oi_filter.order_uuid = o.uuid
      AND oi_filter.deleted_at IS NULL
    ORDER BY oi_filter.id DESC
    LIMIT 1
  ), '') = ?`, r.tables.OrderInstallments)
		args = append(args, filters.RefundStatus)
	}
	if filters.PaymentMethod != "" {
		query += fmt.Sprintf(`
  AND COALESCE((
    SELECT oi_filter.payment_method
    FROM %s oi_filter
    WHERE oi_filter.order_uuid = o.uuid
      AND oi_filter.deleted_at IS NULL
      AND oi_filter.payment_method IS NOT NULL
    ORDER BY oi_filter.id ASC
    LIMIT 1
  ), '') = ?`, r.tables.OrderInstallments)
		args = append(args, filters.PaymentMethod)
	}
	if filters.PaymentTransactionNo != "" {
		query += fmt.Sprintf(`
  AND EXISTS (
    SELECT 1
    FROM %s oi_filter
    WHERE oi_filter.order_uuid = o.uuid
      AND oi_filter.deleted_at IS NULL
      AND COALESCE(oi_filter.payment_transaction_no, '') LIKE ?
  )`, r.tables.OrderInstallments)
		args = append(args, "%"+filters.PaymentTransactionNo+"%")
	}
	if filters.InstallmentType != "" {
		query += fmt.Sprintf(`
  AND COALESCE((
    SELECT COALESCE(oi_filter.type, '')
    FROM %s oi_filter
    WHERE oi_filter.order_uuid = o.uuid
      AND oi_filter.deleted_at IS NULL
    ORDER BY oi_filter.id ASC
    LIMIT 1
  ), '') = ?`, r.tables.OrderInstallments)
		args = append(args, filters.InstallmentType)
	}
	if filters.GoodsType != "" {
		query += "\n  AND COALESCE(o.goods_type, '') = ?"
		args = append(args, filters.GoodsType)
	}
	if filters.MemberUUID != "" {
		query += fmt.Sprintf(`
  AND COALESCE((
    SELECT v_filter.uuid
    FROM %s item_filter
    INNER JOIN %s v_filter
      ON v_filter.uuid = item_filter.model_uuid
     AND v_filter.deleted_at IS NULL
    WHERE item_filter.order_uuid = o.uuid
      AND item_filter.deleted_at IS NULL
      AND item_filter.model_type = 'App\\Models\\ServiceVip'
    ORDER BY item_filter.id ASC
    LIMIT 1
  ), '') = ?`, r.tables.OrderItems, r.tables.ViewServiceVIPs)
		args = append(args, filters.MemberUUID)
	}
	if filters.MemberTitle != "" {
		query += r.memberTitleExistsSQL()
		args = append(args, "%"+filters.MemberTitle+"%")
	}
	if filters.Amount != "" {
		query += "\n  AND o.amount = ?"
		args = append(args, filters.Amount)
	}

	query, args = r.appendComplexOrderPageKeywordSQL(query, args, filters)
	return query, args
}

func appendOrderPageKeywordSQL(query string, args []any, filters MemberOrderFilters) (string, []any) {
	keyword := strings.TrimSpace(filters.Keyword)
	if keyword == "" {
		return query, args
	}

	like := "%" + keyword + "%"
	switch normalizeSearchOption(filters.Options) {
	case "mobile":
		query += "\n  AND COALESCE(u.mobile, '') = ?"
		args = append(args, keyword)
	case "nickname":
		query += "\n  AND COALESCE(u.nickname, '') LIKE ?"
		args = append(args, like)
	case "user_number", "user_id":
		query += "\n  AND COALESCE(u.number, '') = ?"
		args = append(args, keyword)
	case "no", "order_no":
		query += "\n  AND COALESCE(o.no, '') = ?"
		args = append(args, keyword)
	}
	return query, args
}

func (r *MySQLMemberOrderRepository) appendComplexOrderPageKeywordSQL(query string, args []any, filters MemberOrderFilters) (string, []any) {
	keyword := strings.TrimSpace(filters.Keyword)
	if keyword == "" {
		return query, args
	}

	like := "%" + keyword + "%"
	switch normalizeSearchOption(filters.Options) {
	case "mobile":
		query += "\n  AND COALESCE(u.mobile, '') = ?"
		args = append(args, keyword)
	case "nickname":
		query += "\n  AND COALESCE(u.nickname, '') LIKE ?"
		args = append(args, like)
	case "user_number", "user_id":
		query += "\n  AND COALESCE(u.number, '') = ?"
		args = append(args, keyword)
	case "no", "order_no":
		query += "\n  AND COALESCE(o.no, '') = ?"
		args = append(args, keyword)
	case "title", "member_title":
		query += r.memberTitleExistsSQL()
		args = append(args, like)
	case "payment_transaction_no":
		query += r.paymentTransactionNoExistsSQL()
		args = append(args, like)
	default:
		query += fmt.Sprintf(`
  AND (
    COALESCE(u.mobile, '') LIKE ?
    OR COALESCE(u.nickname, '') LIKE ?
    OR COALESCE(u.number, '') LIKE ?
    OR COALESCE(o.no, '') LIKE ?
    OR EXISTS (
      SELECT 1
      FROM %s member_item_filter
      INNER JOIN %s member_v_filter
        ON member_v_filter.uuid = member_item_filter.model_uuid
       AND member_v_filter.deleted_at IS NULL
      WHERE member_item_filter.order_uuid = o.uuid
        AND member_item_filter.deleted_at IS NULL
        AND member_item_filter.model_type = 'App\\Models\\ServiceVip'
        AND COALESCE(member_v_filter.title, '') LIKE ?
    )
    OR EXISTS (
      SELECT 1
      FROM %s tx_filter
      WHERE tx_filter.order_uuid = o.uuid
        AND tx_filter.deleted_at IS NULL
        AND COALESCE(tx_filter.payment_transaction_no, '') LIKE ?
    )
  )`, r.tables.OrderItems, r.tables.ViewServiceVIPs, r.tables.OrderInstallments)
		args = append(args, like, like, like, like, like, like)
	}
	return query, args
}

func appendMemberOrderKeywordSQL(query string, args []any, filters MemberOrderFilters) (string, []any) {
	keyword := strings.TrimSpace(filters.Keyword)
	if keyword == "" {
		return query, args
	}

	like := "%" + keyword + "%"
	switch normalizeSearchOption(filters.Options) {
	case "mobile":
		query += "\n  AND COALESCE(u.mobile, '') = ?"
		args = append(args, keyword)
	case "nickname":
		query += "\n  AND COALESCE(u.nickname, '') LIKE ?"
		args = append(args, like)
	case "user_number", "user_id":
		query += "\n  AND COALESCE(u.number, '') = ?"
		args = append(args, keyword)
	case "no", "order_no":
		query += "\n  AND COALESCE(o.no, '') = ?"
		args = append(args, keyword)
	case "title", "member_title":
		query += "\n  AND COALESCE(member_info.member_title, '') LIKE ?"
		args = append(args, like)
	case "payment_transaction_no":
		query += "\n  AND COALESCE(oi.payment_transaction_no, '') LIKE ?"
		args = append(args, like)
	default:
		query += "\n  AND (COALESCE(u.mobile, '') LIKE ? OR COALESCE(u.nickname, '') LIKE ? OR COALESCE(u.number, '') LIKE ? OR COALESCE(o.no, '') LIKE ? OR COALESCE(member_info.member_title, '') LIKE ? OR COALESCE(oi.payment_transaction_no, '') LIKE ?)"
		args = append(args, like, like, like, like, like, like)
	}
	return query, args
}

func (r *MySQLMemberOrderRepository) memberTitleExistsSQL() string {
	return fmt.Sprintf(`
  AND EXISTS (
    SELECT 1
    FROM %s item_filter
    INNER JOIN %s v_filter
      ON v_filter.uuid = item_filter.model_uuid
     AND v_filter.deleted_at IS NULL
    WHERE item_filter.order_uuid = o.uuid
      AND item_filter.deleted_at IS NULL
      AND item_filter.model_type = 'App\\Models\\ServiceVip'
      AND COALESCE(v_filter.title, '') LIKE ?
  )`, r.tables.OrderItems, r.tables.ViewServiceVIPs)
}

func (r *MySQLMemberOrderRepository) paymentTransactionNoExistsSQL() string {
	return fmt.Sprintf(`
  AND EXISTS (
    SELECT 1
    FROM %s oi_filter
    WHERE oi_filter.order_uuid = o.uuid
      AND oi_filter.deleted_at IS NULL
      AND COALESCE(oi_filter.payment_transaction_no, '') LIKE ?
  )`, r.tables.OrderInstallments)
}

func canUseOrderIDPage(filters MemberOrderFilters) bool {
	if filters.RefundStatus != "" || filters.PaymentMethod != "" || filters.PaymentTransactionNo != "" || filters.InstallmentType != "" {
		return false
	}
	if filters.MemberUUID != "" || filters.MemberTitle != "" {
		return false
	}
	if strings.TrimSpace(filters.Keyword) == "" {
		return true
	}
	switch normalizeSearchOption(filters.Options) {
	case "mobile", "nickname", "user_number", "user_id", "no", "order_no":
		return true
	default:
		return false
	}
}

func placeholders(size int) string {
	if size <= 0 {
		return ""
	}
	items := make([]string, size)
	for i := range items {
		items[i] = "?"
	}
	return strings.Join(items, ",")
}

func matchTimeRange(order MemberOrder, field string, startAt, endAt time.Time) bool {
	target := order.PaymentAt
	if field == "created_at" {
		target = order.CreatedAt
	}
	if !startAt.IsZero() && target.Before(startAt) {
		return false
	}
	if !endAt.IsZero() && target.After(endAt) {
		return false
	}
	return true
}

func matchMemberOrderFields(order MemberOrder, filters MemberOrderFilters) bool {
	if filters.Status != "" && order.Status != filters.Status {
		return false
	}
	if filters.PaymentStatus != "" && order.PaymentStatus != filters.PaymentStatus {
		return false
	}
	if filters.CompanyUUID != "" && order.CompanyUUID != filters.CompanyUUID {
		return false
	}
	if filters.Source != "" && order.Source != filters.Source {
		return false
	}
	if filters.RefundStatus != "" && order.RefundStatus != filters.RefundStatus {
		return false
	}
	if filters.PaymentMethod != "" && order.PaymentMethod != filters.PaymentMethod {
		return false
	}
	if filters.PaymentTransactionNo != "" && !strings.Contains(order.PaymentTransactionNo, filters.PaymentTransactionNo) {
		return false
	}
	if filters.GoodsType != "" && order.GoodsType != filters.GoodsType {
		return false
	}
	if filters.MemberUUID != "" && order.MemberUUID != filters.MemberUUID {
		return false
	}
	if filters.MemberTitle != "" && !strings.Contains(order.MemberTitle, filters.MemberTitle) {
		return false
	}
	if filters.Amount != "" && fmt.Sprintf("%.2f", order.Amount) != filters.Amount {
		return false
	}
	return matchMemberOrderKeyword(order, filters.Options, filters.Keyword)
}

func matchMemberOrderKeyword(order MemberOrder, options, keyword string) bool {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return true
	}
	like := func(value string) bool {
		return strings.Contains(strings.ToLower(value), strings.ToLower(keyword))
	}
	switch normalizeSearchOption(options) {
	case "mobile":
		return order.Mobile == keyword
	case "nickname":
		return like(order.UserNickname)
	case "user_number":
		return order.UserNumber == keyword
	case "no":
		return order.OrderNo == keyword
	case "title", "member_title":
		return like(order.MemberTitle)
	case "payment_transaction_no":
		return like(order.PaymentTransactionNo)
	default:
		return like(order.Mobile) || like(order.UserNickname) || like(order.UserNumber) || like(order.OrderNo) || like(order.MemberTitle) || like(order.PaymentTransactionNo)
	}
}

func seedMemberOrders(size int) []MemberOrder {
	randSource := rand.New(rand.NewSource(20260511))
	statuses := []string{"created", "waitpay", "processing", "paid", "closed"}
	paymentStatuses := []string{"notpay", "partialpay", "success", "failed"}
	refundStatuses := []string{"", "partial", "success"}
	paymentMethods := []string{"wechat", "alipay", "balance", "claim"}
	goodsTypes := []string{"member", "member_renewal", "member_upgrade"}
	memberTitles := []string{"月度会员", "季度会员", "年度会员", "SVIP会员", "ZVIP会员"}
	startAt := time.Date(2025, 1, 1, 8, 0, 0, 0, time.Local)

	orders := make([]MemberOrder, 0, size)
	for i := 1; i <= size; i++ {
		paymentAt := startAt.Add(time.Duration(randSource.Intn(520*24)) * time.Hour)
		createdAt := paymentAt.Add(-time.Duration(randSource.Intn(96)) * time.Hour)
		amount := 199 + randSource.Float64()*1800
		discount := randSource.Float64() * 80
		coupon := randSource.Float64() * 50
		receipt := amount - discount - coupon
		if receipt < 0 {
			receipt = 0
		}
		orders = append(orders, MemberOrder{
			ID:                      int64(i),
			UserNumber:              fmt.Sprintf("U%08d", 100000+i),
			UserNickname:            fmt.Sprintf("MemberUser%05d", i),
			Mobile:                  fmt.Sprintf("13%09d", 100000000+randSource.Intn(899999999)),
			OrderNo:                 fmt.Sprintf("MO2026%08d", i),
			Amount:                  amount,
			DiscountAmount:          discount,
			PaymentContributeAmount: randSource.Float64() * 20,
			CouponAmount:            coupon,
			ReceiptAmount:           receipt,
			Status:                  statuses[randSource.Intn(len(statuses))],
			PaymentStatus:           paymentStatuses[randSource.Intn(len(paymentStatuses))],
			RefundStatus:            refundStatuses[randSource.Intn(len(refundStatuses))],
			RefundAmount:            randSource.Float64() * 40,
			PaymentMethod:           paymentMethods[randSource.Intn(len(paymentMethods))],
			PaymentTransactionNo:    fmt.Sprintf("TX%d%06d", time.Now().Year(), i),
			GoodsType:               goodsTypes[randSource.Intn(len(goodsTypes))],
			MemberUUID:              fmt.Sprintf("member-%03d", 1+randSource.Intn(80)),
			MemberTitle:             memberTitles[randSource.Intn(len(memberTitles))],
			RegisterChannelLink:     "默认渠道 - 默认链接",
			SourceOwner:             "默认归属",
			Attribution:             "默认渠道 - 默认链接",
			PaymentAt:               paymentAt,
			CreatedAt:               createdAt,
		})
	}
	return orders
}
