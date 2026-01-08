#!/usr/bin/env python3
"""
Stock Pipeline CLI

Command-line interface for Nigerian Stock Exchange data pipeline.
Provides commands for running pipeline, querying data, and generating reports.
"""

import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Optional, List

import click
from tabulate import tabulate

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.config.settings import Settings
from app.config.database import get_session
from app.pipelines.orchestrator import PipelineOrchestrator, PipelineConfig
from app.repositories import (
    StockRepository, 
    PriceRepository, 
    IndicatorRepository,
    AlertRepository,
    RecommendationRepository
)
from app.services.advisory import StockScreener
from app.utils import get_logger

logger = get_logger("cli")


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """
    Nigerian Stock Exchange Data Pipeline CLI
    
    Manage stock data ingestion, analysis, alerts, and recommendations.
    """
    pass


# ============================================================================
# PIPELINE COMMANDS
# ============================================================================

@cli.group()
def pipeline():
    """Run and manage data pipeline."""
    pass


@pipeline.command()
@click.option('--fetch-ngx/--no-fetch-ngx', default=True, help='Fetch NGX data')
@click.option('--validate/--no-validate', default=True, help='Validate data quality')
@click.option('--load-stocks/--no-load-stocks', default=True, help='Load stock master data')
@click.option('--load-prices/--no-load-prices', default=True, help='Load price data')
@click.option('--calculate-indicators/--no-calculate-indicators', default=True, help='Calculate indicators')
@click.option('--evaluate-alerts/--no-evaluate-alerts', default=True, help='Evaluate alerts')
@click.option('--generate-recommendations/--no-generate-recommendations', default=True, help='Generate recommendations')
@click.option('--batch-size', default=50, type=int, help='Batch processing size')
@click.option('--lookback-days', default=30, type=int, help='Days of historical data')
def run(
    fetch_ngx: bool,
    validate: bool,
    load_stocks: bool,
    load_prices: bool,
    calculate_indicators: bool,
    evaluate_alerts: bool,
    generate_recommendations: bool,
    batch_size: int,
    lookback_days: int
):
    """Run the complete data pipeline."""
    click.echo("🚀 Starting Nigerian Stock Pipeline...")
    click.echo("=" * 80)
    
    config = PipelineConfig(
        fetch_ngx=fetch_ngx,
        validate_data=validate,
        load_stocks=load_stocks,
        load_prices=load_prices,
        calculate_indicators=calculate_indicators,
        evaluate_alerts=evaluate_alerts,
        generate_recommendations=generate_recommendations,
        batch_size=batch_size,
        max_errors=10,
        lookback_days=lookback_days
    )
    
    orchestrator = PipelineOrchestrator(config)
    result = orchestrator.run()
    
    click.echo("\n" + "=" * 80)
    click.echo("📊 PIPELINE RESULTS")
    click.echo("=" * 80)
    
    status_icon = "✅" if result.success else "❌"
    click.echo(f"\n{status_icon} Status: {'SUCCESS' if result.success else 'FAILED'}")
    click.echo(f"⏱️  Duration: {result.execution_time:.2f}s")
    click.echo(f"\n📈 Processing Metrics:")
    click.echo(f"  • Stocks Processed: {result.stocks_processed}")
    click.echo(f"  • Prices Loaded: {result.prices_loaded}")
    click.echo(f"  • Indicators Calculated: {result.indicators_calculated}")
    click.echo(f"  • Alerts Generated: {result.alerts_generated}")
    click.echo(f"  • Recommendations Generated: {result.recommendations_generated}")
    
    if result.errors:
        click.echo(f"\n⚠️  Errors: {len(result.errors)}")
        for error in result.errors[:5]:
            click.echo(f"  - {error}")
    
    if result.warnings:
        click.echo(f"\n⚠️  Warnings: {len(result.warnings)}")
    
    sys.exit(0 if result.success else 1)


@pipeline.command()
def status():
    """Check pipeline and database status."""
    click.echo("🔍 Checking system status...\n")
    
    db = get_session()
    stock_repo = StockRepository(db)
    price_repo = PriceRepository(db)
    indicator_repo = IndicatorRepository(db)
    alert_repo = AlertRepository(db)
    rec_repo = RecommendationRepository(db)
    
    # Database stats
    active_stocks = stock_repo.get_all_active()
    total_stocks = len(active_stocks)
    
    # Get latest date with data
    latest_prices = price_repo.get_latest_prices(limit=1)
    latest_date = latest_prices[0].price_date if latest_prices else None
    
    click.echo("📊 DATABASE STATUS")
    click.echo("=" * 80)
    click.echo(f"  Active Stocks: {total_stocks}")
    click.echo(f"  Latest Data: {latest_date or 'No data'}")
    
    if latest_date:
        # Stats for latest date
        prices_count = len(price_repo.get_prices_by_date(latest_date))
        indicators_count = indicator_repo.count_indicators_by_date(latest_date)
        
        click.echo(f"  Prices (latest): {prices_count}")
        click.echo(f"  Indicators (latest): {indicators_count}")
        
        # Recent alerts
        week_ago = latest_date - timedelta(days=7)
        recent_alerts = alert_repo.get_alerts_by_date_range(week_ago, latest_date)
        click.echo(f"  Alerts (7 days): {len(recent_alerts)}")
        
        # Recent recommendations
        recent_recs = rec_repo.get_recent_recommendations(days=7)
        click.echo(f"  Recommendations (7 days): {len(recent_recs)}")
    
    click.echo("\n✅ System operational")


# ============================================================================
# STOCK COMMANDS
# ============================================================================

@cli.group()
def stocks():
    """Query and manage stock data."""
    pass


@stocks.command()
@click.option('--sector', help='Filter by sector name')
@click.option('--exchange', default='NGX', help='Exchange (default: NGX)')
@click.option('--active-only/--all', default=True, help='Show only active stocks')
@click.option('--limit', default=50, type=int, help='Max results')
def list(sector: Optional[str], exchange: str, active_only: bool, limit: int):
    """List stocks in the database."""
    db = get_session()
    stock_repo = StockRepository(db)
    
    if sector:
        stocks_list = stock_repo.get_stocks_by_sector(sector)
    else:
        stocks_list = stock_repo.get_all_stocks()
    
    if active_only:
        stocks_list = [s for s in stocks_list if s.is_active]
    
    if exchange:
        stocks_list = [s for s in stocks_list if s.exchange == exchange]
    
    stocks_list = stocks_list[:limit]
    
    if not stocks_list:
        click.echo("No stocks found matching criteria.")
        return
    
    table_data = [
        [
            s.stock_code,
            s.company_name[:40],
            s.sector.sector_name if s.sector else 'N/A',
            s.exchange,
            '✓' if s.is_active else '✗'
        ]
        for s in stocks_list
    ]
    
    click.echo(f"\n📈 STOCKS ({len(stocks_list)} results)\n")
    click.echo(tabulate(
        table_data,
        headers=['Code', 'Company', 'Sector', 'Exchange', 'Active'],
        tablefmt='grid'
    ))


@stocks.command()
@click.argument('stock_code')
def info(stock_code: str):
    """Get detailed information about a stock."""
    db = get_session()
    stock_repo = StockRepository(db)
    price_repo = PriceRepository(db)
    
    stock = stock_repo.get_stock_by_code(stock_code.upper())
    
    if not stock:
        click.echo(f"❌ Stock '{stock_code}' not found")
        sys.exit(1)
    
    click.echo(f"\n📊 {stock.company_name} ({stock.stock_code})")
    click.echo("=" * 80)
    click.echo(f"Sector: {stock.sector.sector_name if stock.sector else 'N/A'}")
    click.echo(f"Exchange: {stock.exchange}")
    click.echo(f"Status: {'Active' if stock.is_active else 'Inactive'}")
    
    if stock.listing_date:
        click.echo(f"Listed: {stock.listing_date}")
    
    # Latest price
    latest_prices = price_repo.get_latest_prices_for_stock(stock.stock_id, limit=1)
    if latest_prices:
        latest = latest_prices[0]
        click.echo(f"\n💰 Latest Price ({latest.price_date}):")
        click.echo(f"  Close: ₦{latest.close_price:,.2f}")
        click.echo(f"  Change: {latest.price_change:+.2f} ({latest.price_change_percent:+.2f}%)")
        click.echo(f"  Volume: {latest.volume:,}")


# ============================================================================
# PRICE COMMANDS
# ============================================================================

@cli.group()
def prices():
    """Query price data."""
    pass


@prices.command()
@click.argument('stock_code')
@click.option('--days', default=30, type=int, help='Number of days')
def history(stock_code: str, days: int):
    """Show price history for a stock."""
    db = get_session()
    stock_repo = StockRepository(db)
    price_repo = PriceRepository(db)
    
    stock = stock_repo.get_stock_by_code(stock_code.upper())
    if not stock:
        click.echo(f"❌ Stock '{stock_code}' not found")
        sys.exit(1)
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    prices_list = price_repo.get_prices_for_stock_date_range(
        stock.stock_id,
        start_date,
        end_date
    )
    
    if not prices_list:
        click.echo(f"No price data found for {stock_code}")
        return
    
    table_data = [
        [
            p.price_date.strftime('%Y-%m-%d'),
            f"₦{p.close_price:,.2f}",
            f"{p.price_change_percent:+.2f}%",
            f"{p.volume:,}",
            f"₦{p.high_price:,.2f}",
            f"₦{p.low_price:,.2f}"
        ]
        for p in reversed(prices_list[-20:])  # Last 20 days
    ]
    
    click.echo(f"\n💹 {stock.company_name} ({stock_code}) - Last {min(len(prices_list), 20)} days\n")
    click.echo(tabulate(
        table_data,
        headers=['Date', 'Close', 'Change %', 'Volume', 'High', 'Low'],
        tablefmt='grid'
    ))


# ============================================================================
# RECOMMENDATION COMMANDS
# ============================================================================

@cli.group()
def recommendations():
    """View investment recommendations."""
    pass


@recommendations.command()
@click.option('--signal', type=click.Choice(['STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL']), help='Filter by signal')
@click.option('--min-score', type=float, help='Minimum score (0-100)')
@click.option('--days', default=7, type=int, help='Days lookback')
@click.option('--limit', default=20, type=int, help='Max results')
def list(signal: Optional[str], min_score: Optional[float], days: int, limit: int):
    """List recent investment recommendations."""
    db = get_session()
    rec_repo = RecommendationRepository(db)
    
    recs = rec_repo.get_recent_recommendations(days=days, limit=limit * 2)
    
    # Filter
    if signal:
        recs = [r for r in recs if r.signal_type == signal]
    if min_score:
        recs = [r for r in recs if float(r.overall_score) >= min_score]
    
    recs = recs[:limit]
    
    if not recs:
        click.echo("No recommendations found matching criteria.")
        return
    
    table_data = [
        [
            r.stock.stock_code,
            r.recommendation_date.strftime('%Y-%m-%d'),
            r.signal_type,
            f"{float(r.overall_score):.1f}",
            r.score_category,
            f"₦{float(r.current_price):,.2f}",
            f"₦{float(r.target_price):,.2f}" if r.target_price else 'N/A',
            f"{float(r.potential_return_pct):+.1f}%" if r.potential_return_pct else 'N/A',
            r.risk_level
        ]
        for r in recs
    ]
    
    click.echo(f"\n🎯 INVESTMENT RECOMMENDATIONS ({len(recs)} results)\n")
    click.echo(tabulate(
        table_data,
        headers=['Stock', 'Date', 'Signal', 'Score', 'Category', 'Price', 'Target', 'Return', 'Risk'],
        tablefmt='grid'
    ))


@recommendations.command()
@click.option('--signal', type=click.Choice(['STRONG_BUY', 'BUY']), default='BUY', help='Signal type')
@click.option('--limit', default=10, type=int, help='Number of picks')
def top(signal: str, limit: int):
    """Show top investment picks."""
    db = get_session()
    rec_repo = RecommendationRepository(db)
    
    top_picks = rec_repo.get_top_picks(signal_type=signal, limit=limit)
    
    if not top_picks:
        click.echo(f"No {signal} recommendations found.")
        return
    
    click.echo(f"\n🏆 TOP {signal} RECOMMENDATIONS\n")
    
    for i, rec in enumerate(top_picks, 1):
        click.echo(f"{i}. {rec.stock.company_name} ({rec.stock.stock_code})")
        click.echo(f"   Signal: {rec.signal_type} | Score: {float(rec.overall_score):.1f}/100 ({rec.score_category})")
        click.echo(f"   Current: ₦{float(rec.current_price):,.2f} → Target: ₦{float(rec.target_price):,.2f}" if rec.target_price else f"   Current: ₦{float(rec.current_price):,.2f}")
        if rec.potential_return_pct:
            click.echo(f"   Potential Return: {float(rec.potential_return_pct):+.1f}% | Risk: {rec.risk_level}")
        click.echo(f"   Reason: {rec.recommendation_reason[:100]}...")
        click.echo()


@recommendations.command()
@click.option('--days', default=30, type=int, help='Analysis period')
def performance():
    """Show recommendation performance metrics."""
    db = get_session()
    rec_repo = RecommendationRepository(db)
    
    stats = rec_repo.get_performance_stats(days=days)
    
    if not stats:
        click.echo("No performance data available.")
        return
    
    click.echo(f"\n📊 RECOMMENDATION PERFORMANCE (Last {days} days)\n")
    click.echo("=" * 80)
    
    for stat in stats:
        click.echo(f"\n{stat['signal_type']}:")
        click.echo(f"  Total: {stat['total']}")
        click.echo(f"  Avg Score: {stat['avg_score']:.1f}")
        if stat['avg_return'] is not None:
            click.echo(f"  Avg Return: {stat['avg_return']:+.2f}%")
        click.echo(f"  Hit Rate: {stat['success_rate']:.1f}%")


# ============================================================================
# ALERT COMMANDS
# ============================================================================

@cli.group()
def alerts():
    """View and manage alerts."""
    pass


@alerts.command()
@click.option('--days', default=7, type=int, help='Days lookback')
@click.option('--severity', type=click.Choice(['INFO', 'WARNING', 'CRITICAL']), help='Filter by severity')
@click.option('--limit', default=50, type=int, help='Max results')
def list(days: int, severity: Optional[str], limit: int):
    """List recent alerts."""
    db = get_session()
    alert_repo = AlertRepository(db)
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    alerts_list = alert_repo.get_alerts_by_date_range(start_date, end_date)
    
    if severity:
        alerts_list = [a for a in alerts_list if a.severity == severity]
    
    alerts_list = alerts_list[:limit]
    
    if not alerts_list:
        click.echo("No alerts found.")
        return
    
    table_data = [
        [
            a.alert_date.strftime('%Y-%m-%d'),
            a.stock.stock_code if a.stock else 'N/A',
            a.alert_type,
            a.severity,
            a.message[:60] + '...' if len(a.message) > 60 else a.message,
            '✓' if a.notification_sent else '✗'
        ]
        for a in alerts_list
    ]
    
    click.echo(f"\n🔔 ALERTS ({len(alerts_list)} results)\n")
    click.echo(tabulate(
        table_data,
        headers=['Date', 'Stock', 'Type', 'Severity', 'Message', 'Sent'],
        tablefmt='grid'
    ))


# ============================================================================
# REPORT COMMANDS
# ============================================================================

@cli.group()
def reports():
    """Generate reports."""
    pass


@reports.command()
@click.option('--output', default='reports/daily_summary.txt', help='Output file')
def daily():
    """Generate daily summary report."""
    db = get_session()
    stock_repo = StockRepository(db)
    price_repo = PriceRepository(db)
    rec_repo = RecommendationRepository(db)
    alert_repo = AlertRepository(db)
    
    today = date.today()
    
    # Get data
    latest_prices = price_repo.get_prices_by_date(today)
    today_alerts = alert_repo.get_alerts_by_date_range(today, today)
    today_recs = rec_repo.get_recommendations_by_date(today)
    
    # Generate report
    report_lines = [
        f"NIGERIAN STOCK EXCHANGE - DAILY SUMMARY",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        "",
        f"📊 MARKET OVERVIEW - {today}",
        "-" * 80,
        f"Stocks with Price Updates: {len(latest_prices)}",
        f"Active Alerts: {len(today_alerts)}",
        f"New Recommendations: {len(today_recs)}",
        "",
    ]
    
    # Top movers
    if latest_prices:
        gainers = sorted(latest_prices, key=lambda p: p.price_change_percent, reverse=True)[:5]
        losers = sorted(latest_prices, key=lambda p: p.price_change_percent)[:5]
        
        report_lines.extend([
            "📈 TOP GAINERS",
            "-" * 80,
        ])
        for p in gainers:
            report_lines.append(f"{p.stock.stock_code:10} ₦{float(p.close_price):>10,.2f}  {float(p.price_change_percent):+6.2f}%")
        
        report_lines.extend([
            "",
            "📉 TOP LOSERS",
            "-" * 80,
        ])
        for p in losers:
            report_lines.append(f"{p.stock.stock_code:10} ₦{float(p.close_price):>10,.2f}  {float(p.price_change_percent):+6.2f}%")
    
    # Recommendations
    if today_recs:
        buy_recs = [r for r in today_recs if r.signal_type in ['STRONG_BUY', 'BUY']]
        if buy_recs:
            report_lines.extend([
                "",
                "🎯 BUY RECOMMENDATIONS",
                "-" * 80,
            ])
            for r in buy_recs[:10]:
                report_lines.append(f"{r.stock.stock_code:10} {r.signal_type:12} Score: {float(r.overall_score):5.1f}/100")
    
    # Write report
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('\n'.join(report_lines))
    
    click.echo(f"✅ Report generated: {output}")
    click.echo('\n'.join(report_lines))


if __name__ == '__main__':
    cli()
