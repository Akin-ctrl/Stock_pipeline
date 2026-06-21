#!/usr/bin/env python3
"""One-off repair utility for stalled promotions, duplicate writes, and missing indexes."""

import logging

from sqlalchemy import text

from app.config.database import get_db
from app.pipelines.orchestrator import PipelineConfig, PipelineOrchestrator
from app.repositories.alert_repository import AlertRepository
from app.repositories.price_repository import PriceRepository
from app.repositories.recommendation_repository import RecommendationRepository


def main() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("database").setLevel(logging.WARNING)

    config = PipelineConfig(
        fetch_afrimarket=False,
        use_staging=True,
        validate_data=True,
        load_stocks=False,
        load_prices=True,
        calculate_indicators=True,
        evaluate_alerts=True,
        generate_recommendations=True,
        batch_size=1000,
    )
    orchestrator = PipelineOrchestrator(config=config)

    pending = orchestrator._get_fact_sync_data()
    print("fact_sync_rows", len(pending))

    if not pending.empty:
        transformed = orchestrator._transform_data(pending)
        print("transformed_rows", len(transformed))
        loaded = orchestrator._load_prices(transformed)
    else:
        loaded = 0
    print("prices_loaded", loaded)

    db = get_db()
    with db.get_session() as session:
        latest_date = session.execute(
            text("SELECT MAX(price_date) FROM fact_daily_prices")
        ).scalar()
        price_repo = PriceRepository(session)
        alert_repo = AlertRepository(session)
        rec_repo = RecommendationRepository(session)
        repaired = price_repo.repair_quality_metadata()
        deleted_alerts = alert_repo.deduplicate_existing_rows()
        deleted = rec_repo.deduplicate_existing_rows()
        session.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_history_stock_rule_date
                ON alert_history (stock_id, rule_id, alert_date)
                """
            )
        )
        session.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_recommendation_stock_date
                ON fact_recommendations (stock_id, recommendation_date)
                """
            )
        )
        session.commit()
        print("price_quality_rows_repaired", repaired)
        print("alert_dupes_deleted", deleted_alerts)
        print("recommendation_dupes_deleted", deleted)
        print("latest_price_date", latest_date)

    if latest_date is not None:
        indicators = orchestrator._calculate_indicators(latest_date, None)
        alerts = orchestrator._evaluate_alerts(latest_date)
        recs = orchestrator._generate_recommendations(latest_date, None)
        print("indicators_calculated", indicators)
        print("alerts_generated", alerts)
        print("recommendations_generated", recs)


if __name__ == "__main__":
    main()
