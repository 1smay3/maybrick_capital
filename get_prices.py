from constants import FMP_API_KEY
from database import DataHandler
from data_models.prices import PricesDataHandler
from data_models.symbols import get_sp500_symbols


# Initialize handlers
symbols=get_sp500_symbols()
interval = "historical-price-full"
data_handler = DataHandler()
stock_data_handler = PricesDataHandler(api_key=FMP_API_KEY, symbols=symbols, interval=interval, rate_limit=275, data_handler=data_handler)

# Update data
stock_data_handler.update_data()

# Read specific symbol data
aapl_data = data_handler.read('AAPL')
aapl_data_pandas = data_handler.read('AAPL', engine="pandas")

print(aapl_data)
