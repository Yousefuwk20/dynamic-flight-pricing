"""
Dagster Pipeline for Flight Pricing ML System

This pipeline orchestrates:
1. Data generation and ingestion to Snowflake
2. dbt-style transformations (staging → analytics → ML)
3. ML model training with MLflow tracking
4. Batch predictions and scoring
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from definitions import defs

__all__ = ["defs"]
