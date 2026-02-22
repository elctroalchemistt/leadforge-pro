from __future__ import annotations

from leadforge.models import Lead, Summary


def summarize(leads: list[Lead]) -> Summary:
    total = len(leads)
    hot = sum(1 for l in leads if l.label == "HOT")
    warm = sum(1 for l in leads if l.label == "WARM")
    cold = sum(1 for l in leads if l.label == "COLD")
    ratings = [l.rating for l in leads if l.rating is not None]
    avg = (sum(ratings) / len(ratings)) if ratings else None
    return Summary(
        total_leads=total,
        hot_leads=hot,
        warm_leads=warm,
        cold_leads=cold,
        average_rating=avg,
    )


def top_hot(leads: list[Lead], n: int = 10) -> list[Lead]:
    hot = [l for l in leads if l.label == "HOT"]
    hot.sort(key=lambda x: (x.score, x.rating or 0, x.review_count or 0), reverse=True)
    return hot[:n]