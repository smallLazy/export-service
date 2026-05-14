package main

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
