import ibapi
from ibapi.common import BarData
from ibapi.contract import Contract
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
import time
import pandas as pd

class IBdatafeed(EClient, EWrapper):
    """This class streams data as well as gathers historical data into a
    pandas dataframe from interactive brokers"""

    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.data = {'reqId': None, 'Date': [], 'Open': [], 'High': [], 'Low': [], 'Close': [], 'Volume': []}

    def error(self, reqId, errorCode, errorString):
        print(f"Error {reqId}: {errorCode} - {errorString}")

    @staticmethod
    def create_contract(symbol, sec_type):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = 'SMART'
        contract.currency = 'USD'
        return contract

    def get_historical_dataframe(self, reqId, contract, duration, intervals):
        self.data['reqId'] = reqId
        print("Requesting historical data...")
        self.reqHistoricalData(reqId=reqId, contract=contract, endDateTime="",
                               durationStr=duration, barSizeSetting=intervals, whatToShow='MIDPOINT',
                               useRTH=1, formatDate=1, keepUpToDate=False, chartOptions=[])
        time.sleep(3)  # Adjust this as necessary
        print("Data request sent. Waiting for response...")
        return pd.DataFrame(self.data)

    def historicalData(self, reqId: int, bar: BarData):
        print(
            f"Received data for ReqId {reqId}: Date: {bar.date}, Open: {bar.open}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}")
        self.data['Date'].append(bar.date)
        self.data['Open'].append(bar.open)
        self.data['High'].append(bar.high)
        self.data['Low'].append(bar.low)
        self.data['Close'].append(bar.close)
        self.data['Volume'].append(bar.volume)


    def background_connection_thread(self):
        self.connect("127.0.0.1", 7497, clientId=10)
        api_thread = threading.Thread(target=self.run, daemon=True)
        api_thread.start()

app = IBdatafeed()
app.background_connection_thread()
contract = app.create_contract('AAPL', 'STK')
df = app.get_historical_dataframe(1, contract, '1 D', '1 hour')
app.historicalData(1, BarData())
print(df)

