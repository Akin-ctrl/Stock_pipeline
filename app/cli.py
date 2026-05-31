#!/usr/bin/env python3
"""
Stock Pipeline CLI

Trimmed command-line interface for the Nigerian stock pipeline.

This CLI is intentionally limited to commands that align with the current
Airflow-first architecture and the close-price-centric schema.
"""

import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Optional, List

import click
from tabulate import tabulate
from sqlalchemy import func

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.config.database import get_session
from app.models import FactDailyPrice, FactTechnicalIndicator
from app.pipelines.orchestrator import PipelineOrchestrator, PipelineConfig
from app.repositories import (
    StockRepository, 
    PriceRepository, 
    AlertRepository,
    RecommendationRepository
)
from app.utils import get_logger

logger = get_logger("cli")


def _get_latest_market_date(db) -> Optional[date]:
    """Return the latest stored daily price date across all stocks."""
    return db.query(func.max(FactDailyPrice.price_date)).scalar()


def _get_prices_for_date(db, price_date: date) -> List[FactDailyPrice]:
    """Return all price rows for a specific trading date."""
    return (
        db.query(FactDailyPrice)
        .filter(FactDailyPrice.price_date == price_date)
        .all()
    )


def _get_indicator_count_for_date(db, calculation_date: date) -> int:
    """Return the count of indicator rows stored for a calculation date."""
    return (
        db.query(func.count(FactTechnicalIndicator.indicator_id))
        .filter(FactTechnicalIndicator.calculation_date == calculation_date)
        .scalar()
        or 0
    )


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """
    Nigerian stock pipeline CLI.

    Use this for pipeline runs and a small set of aligned inspection commands.
    Airflow remains the primary operational surface.
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
@click.option('--validate/--no-validate', default=True, help='Validate data quality')
@click.option('--load-stocks/--no-load-stocks', default=True, help='Load stock master data')
@click.option('--load-prices/--no-load-prices', default=True, help='Load price data')
@click.option('--calculate-indicators/--no-calculate-indicators', default=True, help='Calculate indicators')
@click.option('--evaluate-alerts/--no-evaluate-alerts', default=True, help='Evaluate alerts')
@click.option('--generate-recommendations/--no-generate-recommendations', default=True, help='Generate recommendations')
@click.option('--recommendation-profile', type=click.Choice(['steady_20p_10d']), default='steady_20p_10d', show_default=True, help='Recommendation profile for screening horizon')
@click.option('--batch-size', default=50, type=int, help='Batch processing size')
@click.option('--lookback-days', default=30, type=int, help='Days of historical data')
def run(
    validate: bool,
    load_stocks: bool,
    load_prices: bool,
    calculate_indicators: bool,
    evaluate_alerts: bool,
    generate_recommendations: bool,
    recommendation_profile: str,
    batch_size: int,
    lookback_days: int
):
    """Run the complete data pipeline."""
    click.echo("🚀 Starting Nigerian Stock Pipeline...")
    click.echo("=" * 80)
    
    config = PipelineConfig(
        validate_data=validate,
        load_stocks=load_stocks,
        load_prices=load_prices,
        calculate_indicators=calculate_indicators,
        evaluate_alerts=evaluate_alerts,
        generate_recommendations=generate_recommendations,
        recommendation_profile=recommendation_profile,
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
    alert_repo = AlertRepository(db)
    rec_repo = RecommendationRepository(db)
    
    # Database stats
    active_stocks = stock_repo.get_all_active()
    total_stocks = len(active_stocks)
    
    # Get latest date with data
    latest_date = _get_latest_market_date(db)
    
    click.echo("📊 DATABASE STATUS")
    click.echo("=" * 80)
    click.echo(f"  Active Stocks: {total_stocks}")
    click.echo(f"  Latest Data: {latest_date or 'No data'}")
    
    if latest_date:
        # Stats for latest date
        prices_count = len(_get_prices_for_date(db, latest_date))
        indicators_count = _get_indicator_count_for_date(db, latest_date)
        
        click.echo(f"  Prices (latest): {prices_count}")
        click.echo(f"  Indicators (latest): {indicators_count}")
        
        # Recent alerts
        week_ago = latest_date - timedelta(days=7)
        recent_alerts = alert_repo.get_alerts_by_date_range(week_ago, latest_date)
        click.echo(f"  Alerts (7 days): {len(recent_alerts)}")

        # Recent recommendations
        recent_recs = rec_repo.get_recommendations_by_date_range(week_ago, latest_date)
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
        stocks_list = stock_repo.get_by_sector(sector)
        if exchange:
            stocks_list = [s for s in stocks_list if s.exchange == exchange.upper()]
    else:
        if active_only:
            stocks_list = stock_repo.get_all_active(exchange=exchange)
        elif exchange:
            stocks_list = stock_repo.get_by_exchange(exchange)
        else:
            stocks_list = stock_repo.get_all()
    
    if active_only:
        stocks_list = [s for s in stocks_list if s.is_active]
    
    if exchange:
        stocks_list = [s for s in stocks_list if s.exchange == exchange.upper()]
    
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
    
    stock = stock_repo.get_by_code(stock_code.upper())
    
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
    latest = price_repo.get_latest_trusted_price(stock.stock_id)
    if latest:
        click.echo(f"\n💰 Latest Price ({latest.price_date}):")
        click.echo(f"  Close: ₦{latest.close_price:,.2f}")
        change_pct = float(latest.change_1d_pct) if latest.change_1d_pct is not None else None
        if change_pct is not None:
            click.echo(f"  Change (1d): {change_pct:+.2f}%")
        click.echo(f"  Volume: {latest.volume:,}" if latest.volume is not None else "  Volume: N/A")
        click.echo(f"  Status: {latest.bar_status} | Quality: {latest.data_quality_flag}")


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
    
    stock = stock_repo.get_by_code(stock_code.upper())
    if not stock:
        click.echo(f"❌ Stock '{stock_code}' not found")
        sys.exit(1)
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    prices_list = price_repo.get_price_history(
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
            f"{float(p.change_1d_pct):+.2f}%" if p.change_1d_pct is not None else "N/A",
            f"{p.volume:,}" if p.volume is not None else "N/A",
            p.bar_status,
            p.data_quality_flag,
        ]
        for p in reversed(prices_list[-20:])  # Last 20 days
    ]
    
    click.echo(f"\n💹 {stock.company_name} ({stock_code}) - Last {min(len(prices_list), 20)} days\n")
    click.echo(tabulate(
        table_data,
        headers=['Date', 'Close', 'Change %', 'Volume', 'Status', 'Quality'],
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
@click.option('--signal', type=click.Choice(['STRONG_BUY', 'BUY', 'HOLD', 'AVOID', 'STRONGLY_AVOID']), help='Filter by action')
@click.option('--min-score', type=float, help='Minimum score (0-100)')
@click.option('--days', default=7, type=int, help='Days lookback')
@click.option('--limit', default=20, type=int, help='Max results')
def list(signal: Optional[str], min_score: Optional[float], days: int, limit: int):
    """List recent investment recommendations."""
    db = get_session()
    rec_repo = RecommendationRepository(db)
    
    end_date = _get_latest_market_date(db) or date.today()
    start_date = end_date - timedelta(days=days)
    recs = rec_repo.get_recommendations_by_date_range(start_date, end_date)
    
    # Filter
    if signal:
        recs = [r for r in recs if r.action_type == signal]
    if min_score:
        recs = [r for r in recs if float(r.heuristic_score) >= min_score]
    
    recs = recs[:limit]
    
    if not recs:
        click.echo("No recommendations found matching criteria.")
        return
    
    table_data = [
        [
            r.stock.stock_code,
            r.recommendation_date.strftime('%Y-%m-%d'),
            r.action_type,
            r.technical_signal_type,
            f"{float(r.heuristic_score):.1f}",
            r.heuristic_score_category,
            f"₦{float(r.current_price):,.2f}",
            f"₦{float(r.policy_target_price):,.2f}" if r.policy_target_price else 'N/A',
            f"{float(r.policy_upside_pct):+.1f}%" if r.policy_upside_pct else 'N/A',
            r.heuristic_risk_level
        ]
        for r in recs
    ]
    
    click.echo(f"\n🎯 INVESTMENT RECOMMENDATIONS ({len(recs)} results)\n")
    click.echo(tabulate(
        table_data,
        headers=['Stock', 'Date', 'Action', 'Tech Signal', 'Score', 'Category', 'Price', 'Target', 'Upside', 'Risk'],
        tablefmt='grid'
    ))


@recommendations.command()
@click.option('--signal', type=click.Choice(['STRONG_BUY', 'BUY']), default='BUY', help='Signal type')
@click.option('--limit', default=10, type=int, help='Number of picks')
def top(signal: str, limit: int):
    """Show top investment picks."""
    db = get_session()
    rec_repo = RecommendationRepository(db)
    recommendation_date = _get_latest_market_date(db)

    if recommendation_date is None:
        click.echo("No recommendation data available.")
        return
    
    top_picks = rec_repo.get_top_picks(
        recommendation_date=recommendation_date,
        signal_type=signal,
        top_n=limit,
    )
    
    if not top_picks:
        click.echo(f"No {signal} recommendations found.")
        return
    
    click.echo(f"\n🏆 TOP {signal} RECOMMENDATIONS\n")
    
    for i, rec in enumerate(top_picks, 1):
        click.echo(f"{i}. {rec.stock.company_name} ({rec.stock.stock_code})")
        click.echo(
            f"   Action: {rec.action_type} | Technical: {rec.technical_signal_type} | "
            f"Score: {float(rec.heuristic_score):.1f}/100 ({rec.heuristic_score_category})"
        )
        click.echo(
            f"   Current: ₦{float(rec.current_price):,.2f} → Target: ₦{float(rec.policy_target_price):,.2f}"
            if rec.policy_target_price
            else f"   Current: ₦{float(rec.current_price):,.2f}"
        )
        if rec.policy_upside_pct:
            click.echo(
                f"   Policy Upside: {float(rec.policy_upside_pct):+.1f}% | "
                f"Risk: {rec.heuristic_risk_level}"
            )
        reason = (rec.reasons or [""])[0]
        click.echo(f"   Reason: {reason[:100]}...")
        click.echo()


@recommendations.command()
@click.option('--days', default=30, type=int, help='Analysis period')
def performance(days: int):
    """Show recommendation performance metrics."""
    db = get_session()
    rec_repo = RecommendationRepository(db)
    
    end_date = _get_latest_market_date(db) or date.today()
    start_date = end_date - timedelta(days=days)
    stats = rec_repo.get_performance_stats(start_date=start_date, end_date=end_date)
    
    if not stats:
        click.echo("No performance data available.")
        return
    
    click.echo(f"\n📊 RECOMMENDATION PERFORMANCE (Last {days} days)\n")
    click.echo("=" * 80)
    
    click.echo(f"  Total Recommendations: {stats['total_recommendations']}")
    click.echo(f"  Average Return: {stats['average_return_pct']:+.2f}%")
    click.echo(f"  Win Rate: {stats['win_rate_pct']:.1f}%")
    click.echo(f"  Wins: {stats['wins']}")
    click.echo(f"  Losses: {stats['losses']}")

    if stats['outcomes']:
        click.echo("\n  Outcomes:")
        for outcome, count in sorted(stats['outcomes'].items()):
            click.echo(f"    {outcome}: {count}")


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
def daily(output: str):
    """Generate daily summary report."""
    db = get_session()
    rec_repo = RecommendationRepository(db)
    alert_repo = AlertRepository(db)
    
    report_date = _get_latest_market_date(db)
    if report_date is None:
        click.echo("No price data available for report generation.")
        return
    
    # Get data
    latest_prices = _get_prices_for_date(db, report_date)
    today_alerts = alert_repo.get_alerts_by_date_range(report_date, report_date)
    today_recs = rec_repo.get_recommendations_by_date(report_date)
    
    # Generate report
    report_lines = [
        f"NIGERIAN STOCK EXCHANGE - DAILY SUMMARY",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        "",
        f"📊 MARKET OVERVIEW - {report_date}",
        "-" * 80,
        f"Stocks with Price Updates: {len(latest_prices)}",
        f"Active Alerts: {len(today_alerts)}",
        f"New Recommendations: {len(today_recs)}",
        "",
    ]
    
    # Top movers
    if latest_prices:
        priced_with_change = [p for p in latest_prices if p.change_1d_pct is not None]
        gainers = sorted(priced_with_change, key=lambda p: p.change_1d_pct, reverse=True)[:5]
        losers = sorted(priced_with_change, key=lambda p: p.change_1d_pct)[:5]
        
        report_lines.extend([
            "📈 TOP GAINERS",
            "-" * 80,
        ])
        for p in gainers:
            stock_code = p.stock.stock_code if p.stock else "N/A"
            report_lines.append(
                f"{stock_code:10} ₦{float(p.close_price):>10,.2f}  {float(p.change_1d_pct):+6.2f}%"
            )
        
        report_lines.extend([
            "",
            "📉 TOP LOSERS",
            "-" * 80,
        ])
        for p in losers:
            stock_code = p.stock.stock_code if p.stock else "N/A"
            report_lines.append(
                f"{stock_code:10} ₦{float(p.close_price):>10,.2f}  {float(p.change_1d_pct):+6.2f}%"
            )
    
    # Recommendations
    if today_recs:
        buy_recs = [r for r in today_recs if r.action_type in ['STRONG_BUY', 'BUY']]
        if buy_recs:
            report_lines.extend([
                "",
                "🎯 BUY RECOMMENDATIONS",
                "-" * 80,
            ])
            for r in buy_recs[:10]:
                report_lines.append(f"{r.stock.stock_code:10} {r.action_type:12} Score: {float(r.heuristic_score):5.1f}/100")
    
    # Write report
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('\n'.join(report_lines))
    
    click.echo(f"✅ Report generated: {output}")
    click.echo('\n'.join(report_lines))


if __name__ == '__main__':
    cli()
