# Breaking changes
## 0.3.0  
- Package naming changed to `t_tech` with all corresponding subpackages
## 0.2.0-beta60
- `MarketDataCache` was moved to [t_tech/invest/caching/market_data_cache/cache.py](t_tech/invest/caching/market_data_cache/cache.py).
- The correct import is now `from t_tech.invest.caching.market_data_cache.cache import MarketDataCache` instead of `from t_tech.invest.services import MarketDataCache`.
- Import in [download_all_candles.py](examples/download_all_candles.py) was also corrected.