# A Pandas-based Framework for Rapid Prototyping of TA-Based Futures Trading Strategies

## Introduction

## Architecture

### Abstract Base Class

* We use an abstract base class to define the interface for the backtester.
* This allows us to define all the functionality that is required from the backtester while only exposing those things that should be altered to the user (specific methods)

```python
import abc

class BacktesterABC(abc.ABC):
    pass
```

### Stages of Backtesting

* first we need to load the historical data, this method does not change and is thus not an abstract method
* we presuppose a continous futures contract


* flexible visual backtesting (as Market Technician Title)
* A Hitchhikers' guide to Backtesting