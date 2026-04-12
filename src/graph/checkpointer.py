"""State persistence backends for LangGraph checkpointing."""

from __future__ import annotations

import logging
from typing import Any

from src.config import settings

logger = logging.getLogger(__name__)


def get_checkpointer() -> Any:
    """Return the appropriate LangGraph checkpointer based on config.

    - Dev: MemorySaver (in-process)
    - Production with Redis: (placeholder for RedisSaver)
    """
    if settings.redis_url:
        logger.info("Using Redis checkpointer at %s", settings.redis_url)
        try:
            from langgraph.checkpoint.redis import RedisSaver
            return RedisSaver(settings.redis_url)
        except ImportError:
            logger.warning("langgraph redis checkpoint not available, falling back to MemorySaver")

    from langgraph.checkpoint.memory import MemorySaver
    logger.info("Using in-memory checkpointer (MemorySaver)")
    return MemorySaver()
