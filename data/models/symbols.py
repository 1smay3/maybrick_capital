import requests
from _secrets import FMP_API_KEY
import time

URL = f'https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={FMP_API_KEY}'


def get_sp500_symbols():
    while True:
        try:
            response = requests.get(URL)
            if response.status_code == 200:
                data = response.json()
                return [item['symbol'] for item in data]
            else:
                response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Too Many Requests
                print("Rate limit hit, retrying in 10 seconds...")
                time.sleep(10)
            else:
                raise e  # For other HTTP errors, re-raise the exception
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            time.sleep(10)  # Retry after 10 seconds for other errors

