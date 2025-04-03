import abc
import pandas as pd
import logging

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


class FLBacktesterBase(abc.ABC):
    
    def __init__(self):
        self.EQUITY = 100000.0
        self.df = pd.DataFrame()
    
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

            rtypes = self.df['rtype'].unique().tolist()
            if len(rtypes) != 1:
                raise ValueError(f"Expected single rtype but found multiple: {rtypes}")
            
            logger.info(
                f"Data loaded: {self.df['ts_event'].min()} to {self.df['ts_event'].max()}, "
                f"rtype: {rtypes[0]}, "
                f"bars: {len(self.df)}"
            )
            logger.info(f"Loaded DataFrame:\n{self.df.head().to_string(index=False)}\n"
                        f"...\n{self.df.tail().to_string(index=False)}")

        except Exception as e:
            logger.error(f"Error loading CSV file: {str(e)}")
            self.df = pd.DataFrame()
            raise


if __name__ == "__main__":
    
    class ExampleFLBacktester(FLBacktesterBase):
        pass
    
    backtester = ExampleFLBacktester()
    backtester.set_initial_equity(100000.0)
    backtester.load_data("../../csv_port/glbx-mdp3-20241202-20241205.ohlcv-1s.csv", "MNQZ4")
    