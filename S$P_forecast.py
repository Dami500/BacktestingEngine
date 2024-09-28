import datetime
from datetime import datetime
import pandas as pd
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis as QDA
from dataeventhandler import strategy
from dataeventhandler import signal_event
from backtest import Backtest
from Portfolio import portfolio
from execution import SimulatedExecutionHandler
from dataeventhandler import securities_master_handler
from forcasting import obtain_lagged_series
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.ensemble import RandomForestRegressor


class SPYdailyforecastrategy(strategy):
    """
    S&P500 forecast strategy. It uses a Quadratic Discriminant
    Analyser to predict the returns for a subsequent time
    period and then generated long/exit signals based on the
    prediction.
    """

    def __init__(self, bars, events):
        self.bars = bars
        self.symbol = self.bars.symbol
        self.events = events
        self.datetime_now = datetime.now()
        self.model_start_date = datetime(2001, 1, 10)
        self.model_end_date = datetime(2005, 12, 31)
        self.model_start_test_date = datetime(2005, 1, 1)
        self.long_market = False
        self.short_market = False
        self.bar_index = 0
        self.model = self.create_symbol_forecast_model()

    def create_symbol_forecast_model(self):
        """
        # Create a lagged series of the S&P500 US stock market index
        """
        lagged_series = obtain_lagged_series(self.symbol, self.model_start_date, self.model_end_date, 'close_price', 5)
        # Use the prior two days of returns as predictor
        # values, with direction as the response
        X = lagged_series[["lag1", "lag2"]]
        y = lagged_series["direction"]
        # Create training and test sets
        start_test = self.model_start_test_date
        X_train = X[X.index < start_test]
        print(X_train)
        X_test = X[X.index >= start_test]
        y_train = y[y.index < start_test]
        print(y_train)
        y_test = y[y.index >= start_test]
        model = QDA()
        model.fit(X_train, y_train)
        return model

    def calculate_signals(self, event):
        """
        Calculate the SignalEvents based on market data.
        """
        sym = self.symbol
        dt = self.datetime_now
        if event.type == 'MARKET':
            self.bar_index += 1
            if self.bar_index > 5:
                lags = self.bars.get_latest_bars(self.bar_index, 'returns')
                pred_df = pd.DataFrame({'lag1': [lags[-1] * 100.0], 'lag2': [lags[-2] * 100.0]})
                pred = self.model.predict(pred_df)
                if pred > 0 and not self.long_market:
                    print('LONG')
                    self.long_market = True
                    signal = signal_event(1, sym, dt, 'LONG', 1.0)
                    self.events.put(signal)
                if pred < 0 and self.long_market:
                    print('EXIT')
                    self.long_market = False
                    signal = signal_event(1, sym, dt, 'EXIT', 1.0)
                    self.events.put(signal)


if __name__ == "__main__":
    symbol = '^SPX'
    db_host = 'localhost'
    db_user = 'user'
    db_pass = 'password'
    db_name = 'securities_master'
    initial_capital = 100000.0
    heartbeat = 0
    start_date = datetime(2021, 1, 1, 0, 0, 0)
    backtest = Backtest(symbol, db_host, db_user, db_pass, db_name, initial_capital,
                        heartbeat, start_date, securities_master_handler, SimulatedExecutionHandler,
                        portfolio, SPYdailyforecastrategy)
    backtest.simulate_trading('returns')
























