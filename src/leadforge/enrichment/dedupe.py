import hashlib
from leadforge.models import Lead

def make_dedupe_key(lead: Lead) -> str:
    base = "|".join([
        (lead.name or "").strip().lower(),
        (lead.city or "").strip().lower(),
        (lead.state or "").strip().lower(),
        (lead.phone_e164 or lead.phone or "").strip().lower(),
        (lead.website or "").strip().lower(),
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

def dedupe(leads: list[Lead]) -> list[Lead]:
    seen: set[str] = set()
    out: list[Lead] = []
    for l in leads:
        l.dedupe_key = make_dedupe_key(l)
        if l.dedupe_key in seen:
            continue
        seen.add(l.dedupe_key)
        out.append(l)
    return out