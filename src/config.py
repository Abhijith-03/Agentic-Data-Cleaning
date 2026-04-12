from __future__ import annotations

from typing import Optional

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # --- LLM ---
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    primary_model: str = Field(default="gpt-4o", alias="PRIMARY_MODEL")
    fast_model: str = Field(default="gpt-4o-mini", alias="FAST_MODEL")
    llm_temperature: float = 0.0
    llm_max_retries: int = 2

    # --- LangSmith ---
    langchain_tracing_v2: bool = Field(default=True, alias="LANGCHAIN_TRACING_V2")
    langchain_api_key: str = Field(default="", alias="LANGCHAIN_API_KEY")
    langchain_project: str = Field(
        default="agentic-data-cleaning", alias="LANGCHAIN_PROJECT"
    )

    # --- Cleaning thresholds ---
    confidence_threshold: float = Field(default=0.7, alias="CONFIDENCE_THRESHOLD")
    rule_confidence: float = 0.95
    pattern_confidence: float = 0.85
    max_cleaning_iterations: int = Field(default=3, alias="MAX_CLEANING_ITERATIONS")

    # --- Data processing ---
    chunk_size: int = Field(default=50_000, alias="CHUNK_SIZE")
    polars_threshold: int = Field(
        default=500_000,
        alias="POLARS_THRESHOLD",
        description="Row count above which Polars is used instead of Pandas",
    )
    schema_sample_size: int = 1000
    zscore_threshold: float = 3.0
    iqr_multiplier: float = 1.5
    fuzzy_dedup_threshold: float = 0.9

    # --- Database ---
    database_url: str = Field(default="sqlite:///./data/agentic_clean.db", alias="DATABASE_URL")
    redis_url: Optional[str] = Field(default=None, alias="REDIS_URL")

    # --- Human-in-the-loop ---
    human_in_loop_enabled: bool = False

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
