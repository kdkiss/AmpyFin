from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass

trading_client = TradingClient('PK3SJ8HXF9YRBYPILBN5', 'crxtSGZKR1CF3tlaWrugqaHcP2I4Thvk0vfLsgsU')

# search for US equities
search_params = GetAssetsRequest(asset_class=AssetClass.CRYPTO)

assets = trading_client.get_all_assets(search_params)
print(assets)