from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from leadforge.models import Lead
from leadforge.config import settings


@dataclass(frozen=True)
class RuleResult:
    points: int
    reason: str | None = None


class ScoringRule(Protocol):
    name: str
    def apply(self, lead: Lead) -> RuleResult: ...


class HasWebsiteRule:
    name = "has_website"
    def apply(self, lead: Lead) -> RuleResult:
        if lead.website:
            return RuleResult(settings.SCORE_WEBSITE, "Has website")
        return RuleResult(0, None)


class HasEmailRule:
    name = "has_email"
    def apply(self, lead: Lead) -> RuleResult:
        if lead.email:
            return RuleResult(settings.SCORE_EMAIL, "Has email")
        return RuleResult(0, None)


class RatingAbove4Rule:
    name = "rating_gt_4"
    def apply(self, lead: Lead) -> RuleResult:
        if lead.rating is not None and lead.rating > 4:
            return RuleResult(settings.SCORE_RATING_GT_4, "Rating > 4")
        return RuleResult(0, None)


class ReviewsAbove50Rule:
    name = "reviews_gt_50"
    def apply(self, lead: Lead) -> RuleResult:
        if lead.review_count is not None and lead.review_count > 50:
            return RuleResult(settings.SCORE_REVIEWS_GT_50, "Reviews > 50")
        return RuleResult(0, None)


class HasPhoneRule:
    name = "has_phone"
    def apply(self, lead: Lead) -> RuleResult:
        if lead.phone or lead.phone_e164:
            return RuleResult(settings.SCORE_PHONE, "Has phone")
        return RuleResult(0, None)


def default_rules() -> list[ScoringRule]:
    return [
        HasWebsiteRule(),
        HasEmailRule(),
        RatingAbove4Rule(),
        ReviewsAbove50Rule(),
        HasPhoneRule(),
    ]


def label_from_score(score: int) -> str:
    if score >= settings.HOT_THRESHOLD:
        return "HOT"
    if score >= settings.WARM_THRESHOLD:
        return "WARM"
    return "COLD"

# -------------------------------------------
# Backward compatibility (for old tests)
# -------------------------------------------

from leadforge.models import Lead

def score_lead(lead: Lead) -> Lead:
    """
    Backward-compatible single-lead scoring
    (used by older tests).
    """
    from leadforge.scoring.engine import apply_scoring
    return apply_scoring([lead])[0]