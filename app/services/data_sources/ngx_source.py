"""
Data source for Nigerian Stock Exchange (NGX).

Scrapes stock data from african-markets.com for NGX listed companies.
"""

import time
from typing import Optional
from datetime import date, datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

from app.services.data_sources.base import DataSource
from app.config import get_settings
from app.utils import retry
from app.utils.exceptions import DataFetchError, DataParseError


class NGXDataSource(DataSource):
    """
    Data source for NGX stocks from african-markets.com.
    
    Scrapes the listed companies page to get current prices and company info
    for all ~156 stocks on the Nigerian Stock Exchange.
    """
    
    def __init__(self):
        """Initialize NGX data source."""
        super().__init__("ngx")
        self.settings = get_settings()
        self.url = self.settings.data_sources.ngx_url
        self.timeout = self.settings.data_sources.request_timeout
        
    @retry(max_attempts=3, delay=2.0)
    def fetch(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch current NGX stock data by scraping african-markets.com.
        
        Note: NGX source only provides current/latest prices, not historical data.
        The start_date and end_date parameters are ignored.
        
        Returns:
            DataFrame with columns:
                - stock_code
                - company_name
                - sector
                - exchange (always 'NGX')
                - price_date (today's date)
                - close_price (current price)
                - change_1d_pct
                - change_ytd_pct
                - market_cap
                - volume (always None, not available)
        """
        start_time = time.time()
        
        try:
            self.logger.info(f"Fetching NGX data from {self.url}")
            
            # Fetch the webpage
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Parse the table (structure may vary, needs verification)
            df = self._parse_table(soup)
            
            # Add metadata
            df['exchange'] = 'NGX'
            df['price_date'] = date.today()
            
            # Validate
            self.validate_dataframe(df)
            
            # Log summary
            duration = time.time() - start_time
            self.log_fetch_summary(df, duration)
            
            return df
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch NGX data: {str(e)}")
            raise DataFetchError(f"NGX fetch failed: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Failed to parse NGX data: {str(e)}")
            raise DataParseError(f"NGX parse failed: {str(e)}") from e
    
    def _parse_table(self, soup: BeautifulSoup) -> pd.DataFrame:
        """
        Parse the stock table from HTML.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            DataFrame with parsed stock data
            
        Structure found (2025-12-07):
        - First table has 156 rows
        - Columns: [Company Name, Sector, Price, Change%, YTD%, Market Cap, Date]
        - Stock codes are in href links: /listed-companies/company?code=STOCKCODE
        """
        # Find all tables (first one is the main stocks table)
        tables = soup.find_all('table')
        
        if not tables:
            raise DataParseError("Could not find any table in HTML")
        
        # Use first table (main stocks listing)
        main_table = tables[0]
        
        # Extract data rows
        tbody = main_table.find('tbody')
        if not tbody:
            raise DataParseError("Table has no tbody")
        
        rows_data = []
        for tr in tbody.find_all('tr'):
            cells = tr.find_all('td')
            if len(cells) < 6:  # Expect at least 6 columns
                continue
            
            # Extract stock code from link
            link = cells[0].find('a')
            if link and 'href' in link.attrs:
                href = link['href']
                # Extract code from: /listed-companies/company?code=STOCKCODE
                if '?code=' in href:
                    stock_code = href.split('?code=')[1].strip().upper()
                else:
                    stock_code = None
            else:
                stock_code = None
            
            # Extract text from cells
            # [Company Name, Sector, Price, Change%, YTD%, Market Cap, Date]
            company_name = cells[0].get_text(strip=True)
            sector = cells[1].get_text(strip=True)
            price = cells[2].get_text(strip=True)
            change_1d = cells[3].get_text(strip=True)
            change_ytd = cells[4].get_text(strip=True)
            market_cap = cells[5].get_text(strip=True)
            
            rows_data.append({
                'stock_code': stock_code,
                'company_name': company_name,
                'sector': sector,
                'close_price': price,
                'change_1d_pct': change_1d,
                'change_ytd_pct': change_ytd,
                'market_cap': market_cap
            })
        
        if not rows_data:
            self.logger.warning("No data rows found in table")
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(rows_data)
        
        # Clean and convert data types
        df['stock_code'] = df['stock_code'].str.strip().str.upper()
        df['close_price'] = self._clean_price(df['close_price'])
        df['change_1d_pct'] = self._clean_percentage(df['change_1d_pct'])
        df['change_ytd_pct'] = self._clean_percentage(df['change_ytd_pct'])
        
        # Filter out rows without stock code or invalid price
        df = df[
            df['stock_code'].notna() & 
            (df['stock_code'] != '') &
            df['close_price'].notna() & 
            (df['close_price'] > 0)
        ]
        
        self.logger.info(
            f"Parsed {len(df)} valid stocks from HTML",
            extra={"stocks": len(df), "sectors": df['sector'].nunique()}
        )
        
        return df
    
    def _clean_price(self, series: pd.Series) -> pd.Series:
        """
        Clean price strings and convert to float.
        
        Args:
            series: Series with price strings (e.g., '₦123.45', '123.45')
            
        Returns:
            Series with float values
        """
        return (
            series
            .astype(str)
            .str.replace('₦', '', regex=False)
            .str.replace('NGN', '', regex=False)
            .str.replace(',', '', regex=False)
            .str.strip()
            .replace('', None)
            .astype(float)
        )
    
    def _clean_percentage(self, series: pd.Series) -> pd.Series:
        """
        Clean percentage strings and convert to float.
        
        Args:
            series: Series with percentage strings (e.g., '+5.2%', '-3.1%', '-')
            
        Returns:
            Series with float values (5.2, -3.1, NaN)
        """
        return (
            series
            .astype(str)
            .str.replace('%', '', regex=False)
            .str.replace('+', '', regex=False)
            .str.strip()
            .replace(['-', '', 'None', 'nan'], None)  # Handle missing values
            .astype(float, errors='ignore')
        )
