from __future__ import annotations

from leadforge.models import Lead
from leadforge.scoring.rules import default_rules, label_from_score


def apply_scoring(leads: list[Lead]) -> list[Lead]:
    rules = default_rules()

    for l in leads:
        l.score = 0
        l.score_breakdown = {}
        l.reasons = []

        for r in rules:
            rr = r.apply(l)
            if rr.points:
                l.score += rr.points
                l.score_breakdown[r.name] = rr.points
                if rr.reason:
                    l.reasons.append(rr.reason)

        l.label = label_from_score(l.score)  # type: ignore

    return leads