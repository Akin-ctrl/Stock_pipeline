from datetime import date

from app.services.data_sources.afrimarket_source import AfrimarketDataSource


class FakeResponse:
    def __init__(self, text: str, url: str, status_code: int = 200):
        self.text = text
        self.url = url
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


PAGE_1_HTML = """
<html>
  <head>
    <link rel="next" href="https://afx.kwayisi.org/ngx/?page=2" />
  </head>
  <body>
    <table>
      <thead>
        <tr>
          <th>Ticker</th>
          <th>Name</th>
          <th>Volume</th>
          <th>Price</th>
          <th>Change</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>ABBEYBDS</td>
          <td>Abbey Mortgage Bank Plc</td>
          <td>105118</td>
          <td>8.95</td>
          <td>0.0</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


PAGE_2_HTML = """
<html>
  <body>
    <table>
      <thead>
        <tr>
          <th>Ticker</th>
          <th>Name</th>
          <th>Volume</th>
          <th>Price</th>
          <th>Change</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>ZENITHBANK</td>
          <td>Zenith Bank Plc</td>
          <td>500000</td>
          <td>52.10</td>
          <td>1.5</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


def test_fetch_parses_live_table_shape_without_afrimarket_package_parser(monkeypatch):
    source = AfrimarketDataSource()

    responses = {
        source.market_url: FakeResponse(PAGE_1_HTML, source.market_url),
        "https://afx.kwayisi.org/ngx/?page=2": FakeResponse(
            PAGE_2_HTML, "https://afx.kwayisi.org/ngx/?page=2"
        ),
    }

    monkeypatch.setattr(source.session, "get", lambda url, timeout: responses[url])

    df = source.fetch()

    assert len(df) == 2
    assert set(df["stock_code"]) == {"ABBEYBDS", "ZENITHBANK"}
    assert (df["source"] == "afrimarket").all()
    assert (df["exchange"] == "NGX").all()
    assert df["price_date"].nunique() == 1
    assert df["price_date"].iloc[0] == date.today()
    assert df["close_price"].tolist() == [8.95, 52.10]
    assert df["volume"].tolist() == [105118, 500000]
    assert df["price_change_amount"].tolist() == [0.0, 1.5]


def test_fetch_all_stocks_returns_sorted_unique_codes(monkeypatch):
    source = AfrimarketDataSource()

    page_2_with_duplicate = PAGE_2_HTML.replace(
        "<tbody>",
        "<tbody><tr><td>ABBEYBDS</td><td>Abbey Mortgage Bank Plc</td><td>1</td><td>8.95</td><td>0.0</td></tr>",
    )

    responses = {
        source.market_url: FakeResponse(PAGE_1_HTML, source.market_url),
        "https://afx.kwayisi.org/ngx/?page=2": FakeResponse(
            page_2_with_duplicate, "https://afx.kwayisi.org/ngx/?page=2"
        ),
    }

    monkeypatch.setattr(source.session, "get", lambda url, timeout: responses[url])

    stocks = source.fetch_all_stocks()

    assert stocks == ["ABBEYBDS", "ZENITHBANK"]
