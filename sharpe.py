import pandas as pd
import numpy as np
import mysql.connector as msc
import warnings
import datetime
from datetime import datetime
from scipy.stats import norm

db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'Damilare20%'
db_name = 'securities_master'
con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name)

warnings.filterwarnings('ignore')


def calculate_sharpe(returns_df, n = 252):
    """
    Calculate the periodic Sharpe ratio of a returns stream
    based on a number of trading periods, n. n defaults to 252,
    which then assumes a stream of daily returns for a year.
    The function assumes that the returns are the excess of
    those compared to a benchmark.
    """
    return np.sqrt(n)*(returns_df.mean()/returns_df.std())


def single_equity_sharpe(ticker, start_date, end_date, n):
    """
    Calculates the annualised Sharpe ratio based on the daily
    returns of an equity ticker symbol
    """
    # Obtain the equities daily historic data for the desired time period
    # and add to a pandas DataFrame
    symbol_id = """select securities_master.‘symbol‘.‘id‘
                   from securities_master.‘symbol‘
                   where securities_master.‘symbol‘.‘ticker‘ = '%s'
                   """ % ticker
    symbol = pd.read_sql_query(symbol_id, con)
    f_start_date = start_date.strftime('%Y-%m-%d')
    f_end_date = end_date.strftime('%Y-%m-%d')
    select_str = """select distinct securities_master.‘daily_price‘.close_price
                    from securities_master.‘daily_price‘
                    where securities_master.‘daily_price‘.symbol_id = '%d' 
                    and securities_master.‘daily_price‘.price_date >= '%s' and 
                    securities_master.‘daily_price‘.price_date <= '%s'
                """ % (symbol.iloc[0, 0], f_start_date, f_end_date)
    symbol_price = pd.read_sql_query(select_str, con)
    # Use the percentage change method to easily calculate daily returns
    symbol_price['returns'] = symbol_price['close_price'].pct_change()
    # Assume an average annual risk-free rate over the period of 5%
    symbol_price['excess_daily_ret'] = symbol_price['returns'] - 0.05 / n
    # Return the annualised Sharpe ratio based on the excess daily returns
    return calculate_sharpe(symbol_price['excess_daily_ret'])


def market_neutral_sharpe_ratio(ticker, index_ticker, start_date, end_date, n):
    """
    Calculate the sharpe ratio for taking both long and short positions of equal amounts of capital
    """
    symbol_id = """select securities_master.‘symbol‘.‘id‘
                   from securities_master.‘symbol‘
                   where securities_master.‘symbol‘.‘ticker‘ = '%s'
                   """ % ticker
    index_id = """select securities_master.‘symbol‘.‘id‘
                  from securities_master.‘symbol‘
                  where securities_master.‘symbol‘.‘ticker‘ = '%s'
    """ % index_ticker
    symbol = pd.read_sql_query(symbol_id, con)
    index = pd.read_sql_query(index_id, con)
    f_start_date = start_date.strftime('%Y-%m-%d')
    f_end_date = end_date.strftime('%Y-%m-%d')
    select_str = """select distinct securities_master.‘daily_price‘.close_price as asset_cp,  securities_master.‘daily_price‘.price_date
                    from securities_master.‘daily_price‘
                    where securities_master.‘daily_price‘.symbol_id = '%d' 
                    and securities_master.‘daily_price‘.price_date >= '%s' and 
                    securities_master.‘daily_price‘.price_date <= '%s'
                """ % (symbol.iloc[0, 0], f_start_date, f_end_date)
    symbol_price = pd.read_sql_query(select_str, con)
    select_str = """select distinct securities_master.‘daily_price‘.close_price as index_cp, securities_master.‘daily_price‘.price_date
                    from securities_master.‘daily_price‘
                    where securities_master.‘daily_price‘.symbol_id = '%d' 
                    and securities_master.‘daily_price‘.price_date >= '%s' and 
                    securities_master.‘daily_price‘.price_date <= '%s'
                """ % (index.iloc[0, 0], f_start_date, f_end_date)
    index_price = pd.read_sql_query(select_str, con)
    df = pd.merge(symbol_price, index_price, how='inner', on = 'price_date')
    # Calculate the percentage returns on each of the time series
    df['asset_returns'] = df['asset_cp'].pct_change()
    df['index_returns'] = df['index_cp'].pct_change()
    # Create a new DataFrame to store the strategy information
    # The net returns are (long - short)/2, since there is twice
    # the trading capital for this strategy
    df['net_ret'] = (df['asset_returns'] - df['index_returns']) / 2.0
    return calculate_sharpe(df['net_ret'], 252)


def variance_covariance(p, c, mu, sigma):
    """
    Variance-Covariance calculation of daily Value-at-Risk
    using confidence level c, with mean of returns mu
    and standard deviation of returns sigma, on a portfolio
    of value P.
    """
    alpha = norm.ppf(1-c, mu, sigma)
    return P - P * (alpha + 1)


def calculate_drawdowns(pnl):
    """
    Calculate the largest peak-to-trough drawdown of the PnL curve
    as well as the duration of the drawdown. Requires that the
    pnl_returns is a pandas Series.

    Parameters:
    pnl - A pandas Series representing period percentage returns.

    Returns:
    drawdown, max_drawdown, max_duration - The drawdown series, maximum drawdown,
    and the duration of the maximum drawdown.
    """
    # Calculate the cumulative returns curve
    cumulative_returns = (1 + pnl).cumprod() - 1

    # High Watermark list to track the maximum cumulative returns up to that point
    hwm = [0]  # Initial High Watermark (before any returns)
    # Create the drawdown and duration series
    idx = pnl.index
    drawdown = pd.Series(index=idx)
    duration = pd.Series(index=idx)

    # Loop over the index range to calculate drawdown and duration
    for t in range(1, len(idx)):
        hwm.append(max(hwm[t - 1], cumulative_returns[t]))
        drawdown[t] = hwm[t] - cumulative_returns[t]
        duration[t] = 0 if drawdown[t] == 0 else duration[t - 1] + 1

    # Get maximum drawdown and its duration
    max_drawdown = drawdown.max()
    max_duration = duration.max()

    return drawdown, max_drawdown, max_duration


