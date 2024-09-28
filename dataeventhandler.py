from abc import ABCMeta, abstractmethod
import mysql.connector as msc
import pandas as pd
import numpy as np
import queue
import warnings
warnings.filterwarnings("ignore")
db_host = 'localhost'
db_user = 'user'
db_pass = 'password'
db_name = 'securities_master'
con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name)


class event(object):
    """
    Event is base class providing an interface for all subsequent
    (inherited) events, that will trigger further events in the
    trading infrastructure.
    """
    pass


class market_event(event):
    """
    Handles the event of receiving a new market update with
    corresponding bars.
    """
    def __init__(self):
        self.type = 'MARKET'


class signal_event(event):
    """
    Sends a signal from strategy object to portfolio object
    """

    def __init__(self, strategy_id, symbol, datetime, signal_type, strength):
        """
        Initialises the SignalEvent
        strategy_id : Unique identifier for strategy
        symbol: Ticker symbol
        datetime: timestamp when the signal was generated
        signal_type: 'LONG' or 'SHORT portfolio = portfolio(queue.Queue, datetime.datetime(2024,5,12), 'AAPL')
        print(portfolio.construct_positions())
        print(portfolio.construct_holdings())ORT'
        strength: measures the strength of the signal (useful for pairs trading)
        """
        self.type = 'SIGNAL'
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type
        self.strength = strength


class order_event(event):
    """
    Handles the event of sending an Order to an execution system.
    The order contains a symbol (e.g. AAPL), a type (market or limit)
    , quantity and a direction
    """
    def __init__(self, symbol, order_type, quantity, direction):
        """
        symbol - The instrument to trade.
        order_type - 'MKT' or 'LMT' for Market or Limit.
        quantity - Non-negative integer for quantity.
        direction - 'BUY' or 'SELL' for long or short.
        """
        self.type = 'ORDER'
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.direction = direction

    def print_order(self):
        """
        Outputs the values within the Order.
        """
        print(
            "Order: Symbol = %s, Type = %s, Quantity = %s, Direction = %s" %
            (self.symbol, self.order_type, self.quantity, self.direction))


class fill_event(event):
    """
    Encapsulates the notion of a Filled Order, as returned
    from a brokerage. Stores the quantity of an instrument
    actually filled and at what price. In addition, stores
    the commission of the trade from the brokerage.
    """

    def __init__(self, time_index, symbol, exchange, quantity, direction, fill_cost, commission = None):
        """Parameters:
        time_index - The bar-resolution when the order was filled.
        symbol - The instrument which was filled.
        exchange - The exchange where the order was filled.
        quantity - The filled quantity.
        direction - The direction of fill ('BUY' or 'SELL')
        fill_cost - The holdings value in dollars.
        commission - An optional commission sent from Interactive Brokers
        """
        self.type = 'FILL'
        self.time_index = time_index
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost
        # Calculate commission
        if commission is None:
            self.commission = self.calculate_ib_commission()
        else:
            self.commission = commission

    def calculate_ib_commission(self):
        """
        Calculates the fees of trading based on an Interactive
        Brokers fee structure for API, in CAD
        based on : https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2
        """
        cost = 0
        if self.quantity <= 300000:
            cost = 0.01 * self.quantity
        return cost


class data_handler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).
    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested.
    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, n, type):
        """
        Returns the last N bars updated.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def get_latest_bars_datetime(self, n):
        """
        Returns a Python datetime object for the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def update_bars(self, price_type, bar, day):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low,
        close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")


class securities_master_handler(data_handler):
    """
    securities_master_handler is designed to obtain data form securities database for
    each requested symbol and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """
    def __init__(self, events, symbol, host, user, password, name):
        """
        initialises the securities_master_handler by connecting to the database and
        pulling data concerning the symbols in the symbol list
        Parameters:
            events - The event queue
            symbol_list - The list of ticker symbols
            host - The database host
            user - The database user
            password - The database password
            name - The database name
        """

        self.symbol = symbol
        self.host = host
        self.user = user
        self.password = password
        self.db_name = name
        self.events = events
        self.symbol_data = {}
        self.latest_symbol_data = {self.symbol: [], 'Date': [], 'returns': []}
        self.continue_backtest = True

    def pull_data(self, price_type):
        """
        pulls data from the database based on the symbol
        """
        con = msc.connect(host= self.host, user=self.user, password=self.password, db=self.db_name)
        symbol_select_str = """SELECT securities_master.‘symbol‘.‘id‘
                               FROM securities_master.‘symbol‘
                               where securities_master.‘symbol‘.‘ticker‘ = '%s'""" % self.symbol
        df = pd.read_sql_query(symbol_select_str, con)
        symbol_id = df.iloc[0, 0]
        select_str = """select distinct *
                        from securities_master.‘daily_price‘
                        where securities_master.‘daily_price‘.symbol_id = '%d'
                        order by securities_master.‘daily_price‘.price_date 
                            """ % symbol_id
        final_df = pd.read_sql_query(select_str, con)
        final_df['returns'] = final_df[price_type].pct_change().fillna(0)
        self.symbol_data[self.symbol] = []
        for price in final_df['%s' % price_type]:
            self.symbol_data[self.symbol].append(price)
        self.symbol_data['Date'] = []
        for date in final_df['price_date']:
            self.symbol_data['Date'].append(date)
        self.symbol_data['returns'] = []
        for returns in final_df['returns']:
            self.symbol_data['returns'].append(returns)
        return self.symbol_data

    def get_new_bar(self):
        """
        obtain the price one at a time to simulate a live trading experience
        """
        for b in self.pull_data('close_price')[self.symbol]:
            yield b

    def get_latest_bar(self):
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[self.symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, N, data_type):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[data_type]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bars_datetime(self):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data['Date']
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def update_bars(self, price_type, gen, day):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for symbol
        """
        try:
            bar = next(gen)
            bar_date = self.symbol_data['Date'][day]
            bar_returns = self.symbol_data['returns'][day]
        except StopIteration:
            self.continue_backtest = False
        else:
            if bar is not None:
                self.latest_symbol_data[self.symbol].append(bar)
                self.latest_symbol_data['Date'].append(bar_date)
                self.latest_symbol_data['returns'].append(bar_returns)
        self.events.put(market_event())
        return self.latest_symbol_data


class strategy(object):
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy handling objects.
    The goal of a (derived) Strategy object is to generate Signal
    objects for particular symbols based on the inputs of Bars
    (OHLCV) generated by a DataHandler object.
    This is designed to work both with historic and live data as
    the Strategy object is agnostic to where the data came from,
    since it obtains the bar tuples from a queue object.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def calculate_signals(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_signals()")
