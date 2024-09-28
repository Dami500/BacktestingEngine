import datetime
from datetime import datetime
import numpy as np
import pandas as pd
import statsmodels.api as sm
from dataeventhandler import strategy
from dataeventhandler import signal_event
from backtest import Backtest
from dataeventhandler import securities_master_handler
from execution import SimulatedExecutionHandler
from Portfolio import portfolio
import mysql.connector as msc
import queue


class MovingAverageCrossStrategy(strategy):
    """
    Carries out a basic Moving Average Crossover strategy with a
    short/long simple weighted moving average. Default short/long
    windows are 100/400 periods respectively.
    """
    def __init__(self, bars, events, short_window=100, long_window=400):
        """
        Initialises the Moving Average Cross Strategy.
        Parameters:
        bars - The DataHandler object that provides bar information
        events - The Event Queue object.
        short_window - The short moving average lookback.
        long_window - The long moving average lookback.
        """
        self.bars = bars
        self.symbol = self.bars.symbol
        self.events = events
        self.short_window = short_window
        self.long_window = long_window
        # Set to True if a symbol is in the market
        self.bought = self.calculate_initial_bought()
        self.price = 'close_price'

    def calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for symbol
        and sets it to 'OUT'.
        """
        bought = {self.symbol: 'OUT'}
        return bought

    def calculate_signals(self, events):
        """
        Generates a new set of signals based on the MAC
        SMA with the short window crossing the long window
        meaning a long entry and vice versa for a short entry.
        Parameters
        event - A MarketEvent object.
        """
        if events.type == 'MARKET':
            bars = self.bars.get_latest_bars(self.long_window, self.symbol)
            print(bars)
            bar_date = self.bars.get_latest_bars_datetime()
            if bars is not None and bars != []:
                short_ma = np.mean(bars[0:self.short_window])
                long_ma = np.mean(bars[0:self.long_window])
                dt = datetime.now()
                sig_dir = ""
                if short_ma > long_ma and self.bought[self.symbol] == "OUT":
                    print("LONG: %s" % bar_date)
                    sig_dir = 'LONG'
                    signal = signal_event(1, self.symbol, dt, sig_dir, 1.0)
                    self.events.put(signal)
                    self.bought[self.symbol] = 'LONG'
                elif short_ma < long_ma and self.bought[self.symbol] == "LONG":
                    print("SHORT: %s" % bar_date)
                    sig_dir = 'EXIT'
                    signal = signal_event(1, self.symbol, dt, sig_dir, 1.0)

                    self.events.put(signal)
                    self.bought[self.symbol] = 'OUT'


if __name__ == "__main__":
    symbol = 'AAPL'
    db_host = 'localhost'
    db_user = 'sec_user'
    db_pass = 'Damilare20$'
    db_name = 'securities_master'
    initial_capital = 100000.0
    heartbeat = 0
    # events = queue.Queue()
    start_date = datetime(2001, 1, 1, 0, 0, 0)
    # SMH = securities_master_handler(symbol, db_host, db_user, db_pass, db_name)
    # MAC = MovingAverageCrossStrategy(SMH, events)
    backtest = Backtest(symbol, db_host, db_user, db_pass, db_name, initial_capital,
                        heartbeat, start_date, securities_master_handler, SimulatedExecutionHandler,
                        portfolio, MovingAverageCrossStrategy)
    backtest.simulate_trading('close_price')