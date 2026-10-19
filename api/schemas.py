"""Modèles de réponse de l'API v1."""

from datetime import datetime
from typing import Any, Dict, Generic, List, Literal, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class Price(BaseModel):
    price: float
    quantity: Optional[float] = None
    side: Optional[str] = None
    trade_time_iso: Optional[str] = None
    ingested_at: Optional[str] = None
    source: Optional[str] = None
    cache: Literal["redis", "timescale"]


class Candle(BaseModel):
    bucket: datetime
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    trade_count: Optional[int] = None


class Indicator(BaseModel):
    window_start: datetime
    computed_at: datetime
    window_label: str
    vwap: Optional[float] = None
    rsi: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_bandwidth: Optional[float] = None
    buy_ratio: Optional[float] = None
    trade_count: Optional[int] = None
    volume: Optional[float] = None


class WhaleTx(BaseModel):
    tx_hash: str
    event_type: str
    symbol: str
    timestamp_ts: datetime
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    value_eth: Optional[float] = None
    value_token: Optional[float] = None
    block_number: Optional[int] = None


class Page(BaseModel, Generic[T]):
    items: List[T]
    limit: int
    offset: int
    next_offset: Optional[int] = None      # None = dernière page


class SourceFreshness(BaseModel):
    last_event: Optional[datetime] = None
    age_seconds: Optional[float] = None
    status: Literal["ok", "stale", "empty"]


class Status(BaseModel):
    status: Literal["ok", "degraded", "down"]
    generated_at: datetime
    services: Dict[str, str]
    freshness: Dict[str, SourceFreshness]
    trades_last_minute: Optional[int] = None
    kafka: Dict[str, Any] = {}
