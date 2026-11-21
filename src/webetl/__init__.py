"""webetl: config-driven web ETL (extract -> transform -> load)."""

from webetl.pipeline import Pipeline, RunResult, run_pipeline

__all__ = ["Pipeline", "RunResult", "run_pipeline"]
__version__ = "0.1.0"
