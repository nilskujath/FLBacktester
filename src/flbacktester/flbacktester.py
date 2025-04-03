import abc
import pandas as pd
import logging
from dataclasses import dataclass
from enum import Enum
import uuid
from datetime import datetime
from collections import deque

pd.set_option("display.width", 1000)
pd.set_option("display.max_columns", None)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"


class TradeDirection(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class OrderBase:
    order_id: uuid.UUID
    ts_event: datetime
    trade_direction: TradeDirection
    quantity: float


@dataclass
class MarketOrder(OrderBase):
    order_type: OrderType = OrderType.MARKET


@dataclass
class LimitOrder(OrderBase):
    limit_price: float
    order_type: OrderType = OrderType.LIMIT


@dataclass
class StopOrder(OrderBase):
    stop_price: float
    order_type: OrderType = OrderType.STOP


@dataclass
class Trade:
    trade_id: uuid.UUID
    ts_event: datetime
    assoc_order_id: uuid.UUID
    trade_direction: TradeDirection
    quantity: float
    fill_price: float


class FLBacktesterBase(abc.ABC):
    def __init__(self):
        self.EQUITY = 100000.0
        self.df = pd.DataFrame()
        self.pending_market_orders: dict[uuid.UUID, MarketOrder] = {}
        self.pending_limit_orders: dict[uuid.UUID, LimitOrder] = {}
        self.pending_stop_orders: dict[uuid.UUID, StopOrder] = {}
        self._trade_buffer: deque[dict] = deque(maxlen=1000)
        self.executed_trades = pd.DataFrame(
            columns=[
                "trade_id",
                "ts_event",
                "assoc_order_id",
                "trade_direction",
                "quantity",
                "fill_price",
            ]
        )

    def set_initial_equity(self, cash: float):
        self.EQUITY = cash
        logger.info(f"Initial equity set to {cash}")

    def load_data(self, path_to_csv: str, symbol: str) -> None:
        try:
            logger.info(f"Reading CSV file: {path_to_csv}")
            self.df = pd.read_csv(
                path_to_csv,
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

            logger.info(f"Filtering data for symbol {symbol}")
            self.df = self.df[self.df["symbol"] == symbol]

            logger.info("Converting timestamps and price values")
            self.df["ts_event"] = pd.to_datetime(self.df["ts_event"], unit="ns")
            self.df["open"] = self.df["open"] / 1e9
            self.df["high"] = self.df["high"] / 1e9
            self.df["low"] = self.df["low"] / 1e9
            self.df["close"] = self.df["close"] / 1e9

            rtypes = self.df["rtype"].unique().tolist()
            if len(rtypes) != 1:
                raise ValueError(f"Expected single rtype but found multiple: {rtypes}")

            logger.info(
                f"Data loaded: {self.df['ts_event'].min()} to {self.df['ts_event'].max()}, "
                f"rtype: {rtypes[0]}, "
                f"bars: {len(self.df)}"
            )
            logger.info(
                f"Loaded DataFrame:\n{self.df.head().to_string(index=False)}\n"
                f"...\n{self.df.tail().to_string(index=False)}"
            )

            self.df['position'] = 0

        except Exception as e:
            logger.error(f"Error loading CSV file: {str(e)}")
            self.df = pd.DataFrame()
            raise

    @abc.abstractmethod
    def indicators(self):
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

    def _process_orders(self, row):
        for order_id, order in list(self.pending_market_orders.items()):
            trade = Trade(
                trade_id=uuid.uuid4(),
                ts_event=row["ts_event"],
                assoc_order_id=order.order_id,
                trade_direction=order.trade_direction,
                quantity=order.quantity,
                fill_price=row["open"],
            )
            self._add_trade(trade)
            logger.info(f"Filled market order: {trade}")
            del self.pending_market_orders[order_id]

        for order_id, order in list(self.pending_limit_orders.items()):
            if (
                order.trade_direction == TradeDirection.BUY
                and order.limit_price >= row["low"]
            ) or (
                order.trade_direction == TradeDirection.SELL
                and order.limit_price <= row["high"]
            ):
                trade = Trade(
                    trade_id=uuid.uuid4(),
                    ts_event=row["ts_event"],
                    assoc_order_id=order.order_id,
                    trade_direction=order.trade_direction,
                    quantity=order.quantity,
                    fill_price=order.limit_price,
                )
                self._add_trade(trade)
                logger.info(f"Filled limit order: {trade}")
                del self.pending_limit_orders[order_id]

        for order_id, order in list(self.pending_stop_orders.items()):
            if (
                order.trade_direction == TradeDirection.BUY
                and row["high"] >= order.stop_price
            ) or (
                order.trade_direction == TradeDirection.SELL
                and row["low"] <= order.stop_price
            ):
                fill_price = (
                    max(order.stop_price, row["open"])
                    if order.trade_direction == TradeDirection.BUY
                    else min(order.stop_price, row["open"])
                )

                trade = Trade(
                    trade_id=uuid.uuid4(),
                    ts_event=row["ts_event"],
                    assoc_order_id=order.order_id,
                    trade_direction=order.trade_direction,
                    quantity=order.quantity,
                    fill_price=fill_price,
                )
                self._add_trade(trade)
                logger.info(f"Filled stop order: {trade}")
                del self.pending_stop_orders[order_id]

    @abc.abstractmethod
    def strategy(self, row):
        pass

    def backtest(self):
        self.indicators()
        logger.info(
            f"Backtester DataFrame with added Indicators:\n{self.df.head().to_string(index=False)}\n"
            f"...\n{self.df.tail().to_string(index=False)}"
        )
        for _, row in self.df.iterrows():
            self._process_orders(row)
            self.strategy(row)
            logger.debug(f"Processed Bar Event: {row['ts_event']}")

    def _add_trade(self, trade: Trade) -> None:
        trade_data = {
            "trade_id": trade.trade_id,
            "ts_event": trade.ts_event,
            "assoc_order_id": trade.assoc_order_id,
            "trade_direction": trade.trade_direction.value,
            "quantity": trade.quantity,
            "fill_price": trade.fill_price,
        }
        self._trade_buffer.append(trade_data)
        
        position_change = (
            trade.quantity if trade.trade_direction == TradeDirection.BUY 
            else -trade.quantity
        )
        self.df.loc[self.df['ts_event'] >= trade.ts_event, 'position'] += position_change
        
        if len(self._trade_buffer) >= self._trade_buffer.maxlen:
            self._flush_trade_buffer()

    def _flush_trade_buffer(self) -> None:
        if not self._trade_buffer:
            return

        new_trades = pd.DataFrame(list(self._trade_buffer))
        self.executed_trades = pd.concat(
            [self.executed_trades, new_trades], ignore_index=True
        )
        self._trade_buffer.clear()


if __name__ == "__main__":
    class ExampleFLBacktester(FLBacktesterBase):
        def indicators(self):
            self.df["SMA20_close"] = self.df["close"].rolling(window=20).mean()
            self.df["SMA100_close"] = self.df["close"].rolling(window=100).mean()

        def strategy(self, row):
            if row['position'] == 0:
                if row["SMA20_close"] > row["SMA100_close"]:
                    self.submit_order(
                        MarketOrder(
                            order_id=uuid.uuid4(),
                            ts_event=row["ts_event"],
                            trade_direction=TradeDirection.BUY,
                            quantity=100,
                        )
                    )
            elif row['position'] > 0:
                if row["SMA20_close"] < row["SMA100_close"]:
                    self.submit_order(
                        MarketOrder(
                            order_id=uuid.uuid4(),
                            ts_event=row["ts_event"],
                            trade_direction=TradeDirection.SELL,
                            quantity=abs(row['position']),
                        )
                    )

    backtester = ExampleFLBacktester()
    backtester.set_initial_equity(100000.0)
    backtester.load_data(
        "../../csv_port/glbx-mdp3-20241202-20241205.ohlcv-1s.csv", "MNQZ4"
    )
    backtester.backtest()

    logger.info(f"Executed Trades:\n{backtester.executed_trades.tail()}")

    plot_info = {"SMA20_close": 0, "SMA100_close": 0}