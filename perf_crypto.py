import pandas as pd
from binance.client import Client
from concurrent.futures import ThreadPoolExecutor

client = Client()

info = client.get_exchange_info()

symbols = [x['symbol'] for x in info['symbols']]
exclure = ['UP', 'DOWN', 'BEAR', 'BULL']
sans_levier = [symbol for symbol in symbols if all(exclures not in symbol for exclures in exclure)]
interessant = [symbol for symbol in sans_levier if symbol.endswith('USDT')]


def get_klines(symbol: str):
    return symbol, client.get_historical_klines(symbol, '1m', '1 hour ago UTC')

callbacks = [lambda sym=symbol: get_klines(sym) for symbol in interessant]
klines = {}

with ThreadPoolExecutor() as executor:
    tasks = [executor.submit(callback) for callback in callbacks]
    for task in tasks:
        symbol, lines = task.result()
        klines[symbol] = lines


returns, symbols = [], []

for symbol in interessant:
    if len(klines[symbol]) > 0:
        cumret = (pd.DataFrame(klines[symbol])[4].astype(float).pct_change() +1).prod() - 1
        returns.append(cumret)
        symbols.append(symbol)

retdf = pd.DataFrame(returns, index=symbols, columns=['Retour'])

print(retdf.Retour.nlargest(5))