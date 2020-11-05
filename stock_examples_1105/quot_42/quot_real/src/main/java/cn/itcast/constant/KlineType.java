package cn.itcast.constant;

public enum KlineType {
	DAY_K("1", "trade_date"),
	WEEK_K("2", "week_first_txdate"),
	MONTH_K("3", "month_first_txdate"),
	YEAR_K("4","year_first_txdate");

	private String type; //K线类型
	private String firstTxDateType; //周期首个交易日

	KlineType(String type, String firstTxDateType) {
		this.type = type;
		this.firstTxDateType = firstTxDateType;
	}
	public String getType() {
		return type;
	}
	public String getFirstTxDateType() {
		return firstTxDateType;
	}
}
