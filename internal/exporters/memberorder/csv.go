package memberorder

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"time"
)

var memberOrderHeader = []string{
	"用户编号",
	"订单号",
	"订单金额",
	"优惠金额",
	"支付贡献金额",
	"优惠券金额",
	"实付金额",
	"订单状态",
	"支付状态",
	"支付方式",
	"交易单号",
	"支付时间",
	"创建时间",
	"退款金额",
	"注册渠道-链接",
	"归属成员",
	"归因渠道-链接",
	"会员名称",
	"用户昵称",
}

func memberOrderRow(order MemberOrder) []string {
	return []string{
		order.UserNumber,
		order.OrderNo,
		money(order.Amount),
		money(order.DiscountAmount),
		money(order.PaymentContributeAmount),
		money(order.CouponAmount),
		money(order.ReceiptAmount),
		order.Status,
		order.PaymentStatus,
		order.PaymentMethod,
		order.PaymentTransactionNo,
		formatTime(order.PaymentAt),
		formatTime(order.CreatedAt),
		money(order.RefundAmount),
		order.RegisterChannelLink,
		order.SourceOwner,
		order.Attribution,
		order.MemberTitle,
		order.UserNickname,
	}
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

func money(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format("2006-01-02 15:04:05")
}
