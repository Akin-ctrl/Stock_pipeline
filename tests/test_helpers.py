"""Helper module for fixing test method calls."""

# This is a helper to understand the signatures:

# StockRepository.create_stock() signature:
# create_stock(stock_code: str, company_name: str, sector_id: int, exchange: str, ...)
# Returns: DimStock object with stock_id

# AlertRepository.create_rule() signature (now added):
# create_rule(rule: AlertRule) -> int
# Returns: rule_id

# To create stock from sample_stocks fixture:
# stock = sample_stocks[0]  # This is a DimStock object
# stock_id = stock_repo.create_stock(
#     stock_code=stock.stock_code,
#     company_name=stock.company_name,
#     sector_id=stock.sector_id,
#     exchange=stock.exchange
# )
