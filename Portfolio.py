import datetime
from math import floor
import queue
import numpy as np
import pandas as pd
from dataeventhandler import event
from dataeventhandler import order_event
from sharpe import calculate_sharpe, calculate_drawdowns


class portfolio(object):
    """
    The Portfolio class handles the positions and market
    value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
    The positions DataFrame stores a time-index of the
    quantity of positions held.
    The holdings DataFrame stores the cash and total market
    holdings value of each symbol for a particular
    time-index, as well as the percentage change in
    portfolio total across bars.
    """

    def __init__(self, bars, event, start_date, symbols, initial_capital=100000):
        """
        Initialises the portfolio with bars and an event queue.
        Also includes a starting datetime index and initial capital
        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - any date you pick
        initial_capital - The starting capital in USD.
        """
        self.bars = bars
        self.start_date = start_date
        self.initial_capital = initial_capital
        self.symbols = symbols
        self.event = event
        self.positions = self.construct_all_positions()
        self.current_positions = {symbol: 0 for symbol in self.symbols}
        self.current_holdings = self.construct_current_holdings()
        self.holdings = self.construct_all_holdings()
        self.equity_curve = self.create_equity_curve_dataframe()

    def construct_all_positions(self):
        """
        returns a dictionary with the date and position
        """
        position = [{symbol: 0, 'datetime': self.start_date} for symbol in self.symbols]
        return position

    def construct_current_holdings(self):
        """"
        returns a dictionary containing symbol, date, commission, capital and Total
        """
         # holding = {symbol: 0, 'datetime': self.start_date, 'commission': 0, 'cash': self.initial_capital,
        #            'total': self.initial_capital }
        holding = {}
        for symbol in self.symbols:
            holding[symbol] = 0
            holding['datetime'] = self.start_date
            holding['commission'] = 0
            holding['cash'] = self.initial_capital
            holding['total'] = self.initial_capital
        return holding

    def construct_all_holdings(self):
        """"
        returns a dictionary containing symbol, date, commission, capital and Total
        """
        holding = [{symbol: 0, 'datetime': self.start_date, 'commission': 0, 'cash': self.initial_capital,
                   'total': self.initial_capital} for symbol in self.symbols]
        return [holding]

    def update_time(self, event):
        """
        Adds a new record to the positions matrix for the current
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OHLCV).
        Makes use of a MarketEvent from the events queue. Updates positions and holdings
        """
        latest_datetime = self.bars.get_latest_bars_datetime(1)
        # Update positions
        dp = {}
        for symbol in self.symbols:
            dp[symbol] = self.current_positions[symbol]
            dp['datetime'] = latest_datetime
        self.positions.append(dp)
        # Update holdings
        dh = {}
        for symbol in self.symbols:
            dh[symbol] = self.holdings[symbol]

        # dh = {self.symbol: 0, 'datetime': latest_datetime, 'cash': self.current_holdings['cash'],
        #       'commission': self.current_holdings['commission'], 'total': self.current_holdings['cash']}
        market_value = self.current_positions[self.symbol]*self.bars.get_latest_bar()
        dh['market_value'] = market_value
        self.holdings.append(dh)

    def update_positions_from_fill(self, fill):
        """
        Takes a Fill object and updates the position matrix to
        reflect the new position.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1
        # Update positions list with new quantities
        self.current_positions[self.symbol] += fill_dir * fill.quantity

    def update_holdings_from_fill(self, fill):
        """
        Takes a Fill object and updates the holdings matrix to
        reflect the holdings value.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1
    # Update holdings dict with new quantities
        fill_cost = self.bars.get_latest_bar()
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)

    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_market_order(self, signal):
        """
        Generates an order for a specific symbol
        """
        symbol = signal.symbol
        direction = signal.signal_type
        strength = signal.strength
        mkt_quantity = 100
        cur_quantity = self.current_positions[symbol]
        order = None
        order_type = 'MKT'
        if direction == 'LONG' and cur_quantity == 0:
            order = order_event(symbol, order_type, mkt_quantity, 'BUY')
        if direction == 'SHORT' and cur_quantity == 0:
            order = order_event(symbol, order_type, mkt_quantity, 'SELL')
        if direction == 'EXIT' and cur_quantity > 0:
            order = order_event(symbol, order_type, abs(cur_quantity), 'SELL')
        if direction == 'EXIT' and cur_quantity < 0:
            order = order_event(symbol, order_type, abs(cur_quantity), 'BUY')
        return order

    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.
        """
        if event.type == 'SIGNAL':
            order_event = self.generate_market_order(event)
            self.event.put(order_event)

    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.holdings)
        curve.set_index('datetime', inplace = True)
        curve['returns'] = curve['cash'].pct_change()
        curve['equity_curve'] = (1.0 + curve['returns']).cumprod()
        return curve

    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio.
        """
        total_return = self.create_equity_curve_dataframe()['equity_curve'][-1]
        returns = self.create_equity_curve_dataframe()['returns']
        pnl = self.create_equity_curve_dataframe()['equity_curve']
        sharpe_ratio = calculate_sharpe(returns, 252 * 60 * 6.5)
        drawdown, max_dd, dd_duration = calculate_drawdowns(pnl)
        self.create_equity_curve_dataframe()['drawdown'] = drawdown
        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]
        return stats

