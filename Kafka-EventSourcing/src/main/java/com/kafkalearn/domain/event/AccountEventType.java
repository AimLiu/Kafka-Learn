package com.kafkalearn.domain.event;

/**
 * 账户领域事件类型常量。
 */
public final class AccountEventType {

    /** 开户 */
    public static final String ACCOUNT_OPENED = "AccountOpened";

    /** 充值 */
    public static final String MONEY_DEPOSITED = "MoneyDeposited";

    /** 扣款 */
    public static final String MONEY_WITHDRAWN = "MoneyWithdrawn";

    /** 关户 */
    public static final String ACCOUNT_CLOSED = "AccountClosed";

    private AccountEventType() {
    }
}
