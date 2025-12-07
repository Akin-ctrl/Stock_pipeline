"""
Pipeline orchestration.

Coordinates ETL workflow: fetch → validate → transform → load → calculate → alert.
"""

from app.pipelines.orchestrator import PipelineOrchestrator

__all__ = ['PipelineOrchestrator']
