"""
"""Test script for stock screening system.

**DISCLAIMER**: This generates technical analysis signals for educational purposes.
Not financial advice.

Tests signal generation and displays top stock picks.
"""

import sys
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent))

from app.config.database import get_db
from app.services.advisory import StockScreener, SignalType
from app.repositories import RecommendationRepository
from app.utils import get_logger


def test_advisory_system():
    """Test stock screening system."""
    logger = get_logger("test_advisory")
    
    logger.info("=" * 80)
    logger.info("STOCK SCREENING SYSTEM TEST")
    logger.info("=" * 80)
    
    # Initialize database
    db = get_db()
    
    with db.get_session() as session:
        screener = StockScreener(session)
        rec_repo = RecommendationRepository(session)
        
        try:
            logger.info("\n" + "=" * 80)
            logger.info("GENERATING RECOMMENDATIONS")
            logger.info("=" * 80)
            
            # Generate recommendations
            today = date.today()
            recommendations = screener.generate_recommendations(
                recommendation_date=today,
                min_score=40.0,
                min_confidence=0.5
            )
            
            if not recommendations:
                logger.warning(f"\n⚠️  No recommendations generated for {today}")
                logger.info("\nThis could mean:")
                logger.info("  • No stocks meet the minimum score/confidence thresholds")
                logger.info("  • No technical indicator data available")
                logger.info("  • No price data for today")
                logger.info("\nRun the pipeline first: python -m app.pipelines.orchestrator")
                return False
            
            logger.info(f"\n✅ Generated {len(recommendations)} recommendations")
            
            # Display statistics
            buy_signals = [r for r in recommendations if r.signal_type in (SignalType.BUY, SignalType.STRONG_BUY)]
            sell_signals = [r for r in recommendations if r.signal_type in (SignalType.SELL, SignalType.STRONG_SELL)]
            hold_signals = [r for r in recommendations if r.signal_type == SignalType.HOLD]
            
            logger.info(f"\nSignal Distribution:")
            logger.info(f"  🚀 BUY/STRONG_BUY: {len(buy_signals)}")
            logger.info(f"  ⏸️  HOLD: {len(hold_signals)}")
            logger.info(f"  📉 SELL/STRONG_SELL: {len(sell_signals)}")
            
            # Get top buy picks
            logger.info("\n" + "=" * 80)
            logger.info("TOP BUY SIGNALS")
            logger.info("=" * 80)
            
            top_buys = screener.get_top_picks(
                recommendations,
                signal_filter=SignalType.BUY,
                top_n=5
            )
            
            if top_buys:
                for i, rec in enumerate(top_buys, 1):
                    logger.info(f"\n{'─' * 80}")
                    logger.info(f"#{i} - {rec.stock_code} ({rec.stock_name})")
                    logger.info(f"{'─' * 80}")
                    
                    # Signal and confidence
                    emoji_map = {
                        SignalType.STRONG_BUY: "🚀",
                        SignalType.BUY: "📈",
                    }
                    emoji = emoji_map.get(rec.signal_type, "")
                    
                    logger.info(f"\nSignal: {emoji} {rec.signal_type.value}")
                    logger.info(f"Confidence: {rec.confidence*100:.0f}%")
                    logger.info(f"Overall Score: {rec.score:.1f}/100 ({rec.score_category.value})")
                    logger.info(f"Risk Level: {rec.risk_level}")
                    
                    # Prices
                    logger.info(f"\nPrices:")
                    logger.info(f"  Current:     ₦{rec.current_price:,.2f}")
                    if rec.target_price:
                        logger.info(f"  Target:      ₦{rec.target_price:,.2f}")
                        potential = ((rec.target_price - rec.current_price) / rec.current_price) * 100
                        logger.info(f"  Potential:   {potential:+.1f}%")
                    if rec.stop_loss:
                        logger.info(f"  Stop Loss:   ₦{rec.stop_loss:,.2f}")
                    
                    # Score breakdown
                    logger.info(f"\nScore Breakdown:")
                    logger.info(f"  Technical:   {rec.stock_score.technical_score:.0f}/100")
                    logger.info(f"  Momentum:    {rec.stock_score.momentum_score:.0f}/100")
                    logger.info(f"  Volatility:  {rec.stock_score.volatility_score:.0f}/100")
                    logger.info(f"  Trend:       {rec.stock_score.trend_score:.0f}/100")
                    logger.info(f"  Volume:      {rec.stock_score.volume_score:.0f}/100")
                    
                    # Top reasons
                    logger.info(f"\nKey Reasons:")
                    for j, reason in enumerate(rec.reasons[:3], 1):
                        logger.info(f"  {j}. {reason}")
            
            # Save to database
            logger.info("\n" + "=" * 80)
            logger.info("SAVING TO DATABASE")
            logger.info("=" * 80)
            
            saved = rec_repo.create_recommendations_bulk(recommendations)
            logger.info(f"\n✅ Saved {saved} recommendations to database")
            
            session.commit()
            
            # Display sample detailed recommendation
            if top_buys:
                logger.info("\n" + "=" * 80)
                logger.info("DETAILED RECOMMENDATION EXAMPLE")
                logger.info("=" * 80)
                
                detailed = advisor.format_recommendation(top_buys[0])
                logger.info(detailed)
            
            # Show how to retrieve from database
            logger.info("\n" + "=" * 80)
            logger.info("DATABASE RETRIEVAL EXAMPLE")
            logger.info("=" * 80)
            
            saved_recs = rec_repo.get_recommendations_by_date(today)
            logger.info(f"\nRetrieved {len(saved_recs)} recommendations from database")
            
            if saved_recs:
                logger.info("\nTop 3 by score:")
                for i, rec in enumerate(saved_recs[:3], 1):
                    logger.info(
                        f"  {i}. {rec.stock.symbol} - "
                        f"Score: {rec.overall_score}, "
                        f"Signal: {rec.signal_type}"
                    )
            
            # Performance stats (if historical data exists)
            logger.info("\n" + "=" * 80)
            logger.info("RECOMMENDATION PERFORMANCE")
            logger.info("=" * 80)
            
            stats = rec_repo.get_performance_stats()
            logger.info(f"\nHistorical Statistics:")
            logger.info(f"  Total Recommendations: {stats['total_recommendations']}")
            logger.info(f"  Win Rate: {stats['win_rate_pct']:.1f}%")
            logger.info(f"  Average Return: {stats['average_return_pct']:.2f}%")
            logger.info(f"  Wins: {stats['wins']}")
            logger.info(f"  Losses: {stats['losses']}")
            
            if stats['outcomes']:
                logger.info(f"\n  Outcome Distribution:")
                for outcome, count in stats['outcomes'].items():
                    logger.info(f"    {outcome}: {count}")
            
            screener.close()
            
        except Exception as e:
            logger.error(f"Test failed: {str(e)}", exc_info=True)
            return False
    
    logger.info("\n" + "=" * 80)
    logger.info("TEST COMPLETE")
    logger.info("=" * 80)
    
    return True


def display_usage_examples():
    """Display usage examples."""
    logger = get_logger("test_advisory")
    
    logger.info("\n" + "=" * 80)
    logger.info("USAGE EXAMPLES")
    logger.info("=" * 80)
    
    # Print usage examples as separate lines to avoid f-string parsing issues
    logger.info("\n# Generate screening signals manually")
    logger.info("from app.config.database import get_db")
    logger.info("from app.services.advisory import StockScreener")
    logger.info("from datetime import date\n")
    logger.info("db = get_db()")
    logger.info("with db.get_session() as session:")
    logger.info("    screener = StockScreener(session)\n")
    logger.info("    recommendations = screener.generate_recommendations(")
    logger.info("        recommendation_date=date.today(),")
    logger.info("        min_score=60.0,  # Only high-scoring stocks")
    logger.info("        min_confidence=0.7  # High confidence only")
    logger.info("    )\n")
    logger.info("    # Get top buy picks")
    logger.info("    top_buys = screener.get_top_picks(")
    logger.info("        recommendations,")
    logger.info("        signal_filter=SignalType.BUY,")
    logger.info("        top_n=10")
    logger.info("    )\n")
    logger.info("    for rec in top_buys:")
    logger.info("        print(screener.format_recommendation(rec))\n")
    logger.info("# Query database for recommendations")
    logger.info("from app.repositories import RecommendationRepository\n")
    logger.info("with db.get_session() as session:")
    logger.info("    repo = RecommendationRepository(session)\n")
    logger.info("    # Get today's recommendations")
    logger.info("    recs = repo.get_recommendations_by_date(date.today())\n")
    logger.info("    # Get active buy recommendations")
    logger.info("    active_buys = repo.get_active_recommendations(signal_type='BUY')\n")
    logger.info("    # Get top picks")
    logger.info("    top_picks = repo.get_top_picks(")
    logger.info("        recommendation_date=date.today(),")
    logger.info("        signal_type='BUY',")
    logger.info("        top_n=5")
    logger.info("    )\n")
    logger.info("    # Get performance stats")
    logger.info("    stats = repo.get_performance_stats()")
    logger.info("    print(f\"Win rate: {stats['win_rate_pct']:.1f}%\")")


if __name__ == "__main__":
    try:
        success = test_advisory_system()
        
        if success:
            display_usage_examples()
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger = get_logger("test_advisory")
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        sys.exit(1)
