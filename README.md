The following files are used to develop an event driven backtesting system. This system treats each new bar, fill, order and signal as an event. As a result, the user can backtest their trading 
strategies on daily EOD data as far back as 2001. Trading statistics such as the sharpe ratio, maximum_drawdown as well as total returns are placed into account.
Examples of a simple moving average strategy is included as well as an S&P forecasting strategy that makes use of quadratic driscriminant analysis. 

IMPORTANT NOTES:
The porfolio order generates a naive order of a 100 units at specific prices deteremined by the  various strategies. This can be edited by the user depending on their needs. 
All transaction costs are calculated based on Interactive broker fees : https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2
