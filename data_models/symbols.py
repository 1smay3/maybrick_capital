import requests
from constants import FMP_API_KEY

URL = f'https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={FMP_API_KEY}'


def get_sp500_symbols():
    response = requests.get(URL)
    if response.status_code == 200:
        data = response.json()
        return [item['symbol'] for item in data]
    else:
        response.raise_for_status()

