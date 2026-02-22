from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Any, Literal

LeadLabel = Literal["HOT", "WARM", "COLD"]
BizType = Literal["B2B", "B2C", "UNKNOWN"]
BizSize = Literal["SMALL", "MEDIUM", "UNKNOWN"]

class Lead(BaseModel):
    # core
    name: str
    category: str | None = None
    rating: float | None = None
    review_count: int | None = None
    phone: str | None = None
    website: str | None = None
    email: str | None = None
    city: str | None = None
    state: str | None = None

    # provider/meta (SaaS-ready)
    source: str | None = None                # mock | places_api | ...
    place_id: str | None = None              # if available
    raw: dict[str, Any] | None = None        # optional raw payload (careful with size)

    # enriched/meta
    phone_e164: str | None = None
    website_valid: bool | None = None
    category_norm: str | None = None
    dedupe_key: str | None = None

    # scoring outputs
    score: int = 0
    label: LeadLabel = "COLD"
    score_breakdown: dict[str, int] = Field(default_factory=dict)  # rule_name -> points
    reasons: list[str] = Field(default_factory=list)               # human-friendly reasons

    # classification outputs
    biz_type: BizType = "UNKNOWN"
    biz_size: BizSize = "UNKNOWN"


class Summary(BaseModel):
    total_leads: int
    hot_leads: int
    warm_leads: int
    cold_leads: int
    average_rating: float | None = None