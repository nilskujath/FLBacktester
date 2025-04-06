import abc
import dataclasses
import logging
import pandas as pd
import enum
import uuid

pd.set_option("display.width", 1000)
pd.set_option("display.max_columns", None)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class RecordType(enum.Enum):
    OHLCV_1S = 32

    @classmethod
    def to_string(cls, rtype: int) -> str:
        match rtype:
            case cls.OHLCV_1S.value:
                return "1s Bars"
            case _:
                return f"Unknown ({rtype})"


class OrderType(enum.Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"


class Side(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclasses.dataclass
class OrderBase:
    order_id: uuid.UUID
    ts_event: pd.Timestamp
    order_direction: Side
    quantity: float


@dataclasses.dataclass
class MarketOrder(OrderBase):
    order_type: OrderType = OrderType.MARKET


@dataclasses.dataclass
class LimitOrder(OrderBase):
    limit_price: float
    order_type: OrderType = OrderType.LIMIT


@dataclasses.dataclass
class StopOrder(OrderBase):
    stop_price: float
    order_type: OrderType = OrderType.STOP


@dataclasses.dataclass
class Trade:
    trade_id: uuid.UUID
    ts_event: pd.Timestamp
    associated_order_id: uuid.UUID
    trade_direction: Side
    quantity: float
    fill_at: float  # Point value where the futures contract was filled
    commission_and_fees: float
    fill_adjusted: float  # Point value adjusted for commission and fees


@dataclasses.dataclass(frozen=True)
class Contract:
    symbol: str
    point_value: float
    tick_size: float
    intraday_initial_margin: float
    intraday_maintenance_margin: float
    long_overnight_initial_margin: float
    long_overnight_maintenance_margin: float
    short_overnight_initial_margin: float
    short_overnight_maintenance_margin: float
    broker_commission_per_contract: float
    exchange_fees_per_contract: float
    description: str


@dataclasses.dataclass(frozen=True)
class DefaultMNQ(Contract):
    symbol: str = "MNQ"
    point_value: float = 2.0
    tick_size: float = 0.25
    intraday_initial_margin: float = 2580.60
    intraday_maintenance_margin: float = 2580.60
    long_overnight_initial_margin: float = 2580.60
    long_overnight_maintenance_margin: float = 2580.60
    short_overnight_initial_margin: float = 2580.60
    short_overnight_maintenance_margin: float = 2580.60
    broker_commission_per_contract: float = 0.25
    exchange_fees_per_contract: float = 0.37
    description: str = "Micro E-mini NASDAQ-100"


class SingleContractBacktesterBase(abc.ABC):
    def __init__(self):
        self._df = pd.DataFrame()
        self._trades = pd.DataFrame(
            columns=[
                "trade_id",
                "ts_event",
                "associated_order_id",
                "trade_direction",
                "quantity",
                "fill_at",
                "commission_and_fees",
                "fill_adjusted",
            ]
        )

        self._cash = 100_000.0
        self._current_contract: Contract | None = None
        self._total_fees_per_contract: float = 0.0

        self.pending_market_orders: dict[uuid.UUID, MarketOrder] = {}
        self.pending_limit_orders: dict[uuid.UUID, LimitOrder] = {}
        self.pending_stop_orders: dict[uuid.UUID, StopOrder] = {}

    def set_contract(self, contract: Contract) -> None:
        self._current_contract = contract
        self._total_fees_per_contract = (
            contract.broker_commission_per_contract
            + contract.exchange_fees_per_contract
        )

    def set_initial_cash(self, initial_cash: float) -> None:
        self._cash = initial_cash

    def load_historical_data(
        self, path_to_dbcsv: str, symbol: str | None = None
    ) -> None:
        try:
            logger.info(f"Reading CSV file: {path_to_dbcsv}")
            self._df = pd.read_csv(
                path_to_dbcsv,
                usecols=[
                    "ts_event",
                    "rtype",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "symbol",
                ],
                dtype={
                    "ts_event": int,
                    "rtype": int,
                    "open": int,
                    "high": int,
                    "low": int,
                    "close": int,
                    "volume": int,
                    "symbol": str,
                },
            )

            if symbol is not None:
                logger.info(f"Filtering data for symbol {symbol}")
                self._df = self._df[self._df["symbol"] == symbol]

            logger.info(
                "Converting timestamps and price values to human-readable formats"
            )
            self._df["ts_event"] = pd.to_datetime(self._df["ts_event"], unit="ns")
            self._df["open"] = self._df["open"] / 1e9
            self._df["high"] = self._df["high"] / 1e9
            self._df["low"] = self._df["low"] / 1e9
            self._df["close"] = self._df["close"] / 1e9

            _rtypes = self._df["rtype"].unique().tolist()
            if len(_rtypes) != 1:
                raise ValueError(f"Expected single rtype but found multiple: {_rtypes}")

            logger.info(
                f"Data loaded: {self._df['ts_event'].min()} to {self._df['ts_event'].max()}, "
                f"Record type: {RecordType.to_string(_rtypes[0])}, "
                f"Number of bars: {len(self._df)}"
            )
            logger.info(
                f"Loaded DataFrame with historical OHLCV data:"
                f"\n{self._df.head().to_string(index=False)}\n"
                f"...\n{self._df.tail().to_string(index=False)}"
            )

        except Exception as e:
            logger.error(f"Error loading CSV file: {str(e)}")
            self._df = pd.DataFrame()
            raise

    @abc.abstractmethod
    def add_indicators(self) -> None:
        pass

    @abc.abstractmethod
    def strategy(self, row: pd.Series) -> None:
        pass

    def submit_order(self, order: OrderBase) -> None:
        if isinstance(order, MarketOrder):
            self.pending_market_orders[order.order_id] = order
            logger.info(f"Submitted market order {order.order_id}")
        elif isinstance(order, LimitOrder):
            self.pending_limit_orders[order.order_id] = order
            logger.info(f"Submitted limit order {order.order_id}")
        elif isinstance(order, StopOrder):
            self.pending_stop_orders[order.order_id] = order
            logger.info(f"Submitted stop order {order.order_id}")

    def run_backtest(self) -> None:
        self.add_indicators()
        logger.info(
            f"Backtester DataFrame with added Indicators:\n{self._df.head(100).to_string(index=False)}\n"
            f"...\n{self._df.tail().to_string(index=False)}"
        )
        for _, row in self._df.iterrows():
            self._process_pending_orders(row)
            self.strategy(row)
            logger.debug(f"Processed Bar: {row['ts_event']}")

    def show_performance_metrics(self) -> None:
        pass

    def _process_pending_orders(self, row: pd.Series) -> None:
        if self._current_contract is None:
            raise ValueError(
                "Contract specifications not set. Call set_contract() first."
            )

        for order_id, order in list(self.pending_market_orders.items()):
            trade = Trade(
                trade_id=uuid.uuid4(),
                ts_event=row["ts_event"],
                associated_order_id=order.order_id,
                trade_direction=order.order_direction,
                quantity=order.quantity,
                fill_at=row["open"],  # Raw fill price in points
                commission_and_fees=self._total_fees_per_contract
                * order.quantity,  # Total fees in dollars
                fill_adjusted=(
                    row["open"]
                    + (
                        self._total_fees_per_contract
                        / self._current_contract.point_value
                    )
                    if order.order_direction == Side.BUY
                    else row["open"]
                    - (
                        self._total_fees_per_contract
                        / self._current_contract.point_value
                    )
                ),  # Fill price adjusted for fees, expressed in points
            )
            self._register_trade_execution(trade)
            del self.pending_market_orders[order_id]

        # Process pending limit orders
        for order_id, order in list(self.pending_limit_orders.items()):
            pass

        # Process pending stop orders
        for order_id, order in list(self.pending_stop_orders.items()):
            pass

    def _register_trade_execution(self, trade: Trade) -> None:
        pass


def csv_to_databento_format(path_to_csv: str) -> None:
    pass


if __name__ == "__main__":

    class Backtester(SingleContractBacktesterBase):
        def add_indicators(self) -> None:
            close = self._df["close"]
            detrend = close - close.rolling(window=7).mean()
            self._df["cmb_detrend"] = detrend
            self._df["cmb_detrend_sma20"] = detrend.rolling(window=20).mean()
            self._df["cmb_detrend_bb_upper"] = (
                self._df["cmb_detrend_sma20"] + 2 * detrend.rolling(window=20).std()
            )
            self._df["cmb_detrend_bb_lower"] = (
                self._df["cmb_detrend_sma20"] - 2 * detrend.rolling(window=20).std()
            )

        def strategy(self, row: pd.Series) -> None:
            pass

    backtester = Backtester()
    backtester.set_contract(DefaultMNQ())
    backtester.set_initial_cash(100_000.0)
    backtester.load_historical_data(
        "../../csv_port/glbx-mdp3-20241202-20241205.ohlcv-1s.csv", "MNQZ4"
    )
    backtester.run_backtest()
