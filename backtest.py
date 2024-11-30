import datetime
import pprint
import time
import queue
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector as msc

# db_host = 'localhost'
# db_user = 'sec_user'
# db_pass = 'Damilare20$'
# db_name = 'securities_master'
# plug ='caching_sha2_password'
# con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name, auth_plugin= plug)



class Backtest(object):
    """
    Encapsulates the settings and components for carrying out
    an event-driven backtest.
    """
    def __init__(self, symbol, host, user, password, name, plugin, initial_capital, heartbeat, start_date, data_handler
                 , execution_handler, portfolio, strategy):
        """
        Initialize the backtest.
        """
        self.symbols = symbol
        self.host = host
        self.user = user
        self.password = password
        self.plugin = plugin
        self.db_name = name
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.events = queue.Queue()
        self.start_date = start_date
        self.data_handler = data_handler(self.events, self.symbols, self.host, self.user, self.password, self.db_name, self.plugin)
        self.execution_handler = execution_handler(self.events)
        self.portfolio = portfolio(self.data_handler, self.events, self.start_date, self.symbols, self.initial_capital)
        self.strategy = strategy(self.data_handler, self.events)
        self.signals = 0
        self.orders = 0
        self.fills = 0
        self.num_strats = 1

    def run_backtest(self, price_type):
        """
        executes the backtest
        """
        i = -1
        gen = self.data_handler.get_new_bar(price_type)
        while True:
            i += 1
        # Update the market bars
            if self.data_handler.continue_backtest:
                self.data_handler.update_bars(price_type, gen, i)
            else:
                break
        # Handle the events
            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)
                            self.portfolio.update_time(event)
                        elif event.type == 'SIGNAL':
                            self.signals += 1
                            self.portfolio.update_signal(event)
                        elif event.type == 'ORDER':
                            self.orders += 1
                            self.execution_handler.execute_order(event)
                        elif event.type == 'FILL':
                            self.fills += 1
                            self.portfolio.update_fill(event)
            time.sleep(self.heartbeat)

    def output_performance(self):
        """
        Outputs the strategy performance from the backtest.
        """
        self.portfolio.create_equity_curve_dataframe()
        print("Creating summary stats...")
        stats = self.portfolio.output_summary_stats()
        print("Creating equity curve...")
        print(self.portfolio.create_equity_curve_dataframe())
        pprint.pprint(stats)
        print("Signals: %s" % self.signals)
        print("Orders: %s" % self.orders)
        print("Fills: %s" % self.fills)

    def plot_values(self):
        data = self.portfolio.create_equity_curve_dataframe()
        # Plot three charts: Equity curve,
        # period returns, drawdowns
        fig = plt.figure()
        # Set the outer colour to white
        fig.patch.set_facecolor('white')
        # Plot the equity curve
        ax1 = fig.add_subplot(311, ylabel='Portfolio value, % ')
        data['equity_curve'].plot(ax=ax1, color="blue", lw=2.)
        plt.grid(True)
        # Plot the returns
        ax2 = fig.add_subplot(312, ylabel='Period returns, % ')
        data['returns'].plot(ax=ax2, color="black", lw=2.)
        plt.grid(True)
        # Plot the figure
        plt.tight_layout()
        plt.show()

    def simulate_trading(self, price_type):
        """
        Simulates the backtest and outputs portfolio performance.
        """
        self.run_backtest(price_type)
        self.output_performance()
        self.plot_values()


















