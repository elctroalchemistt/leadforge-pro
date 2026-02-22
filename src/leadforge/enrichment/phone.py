import phonenumbers
from leadforge.models import Lead

def normalize_phone_e164(raw: str, default_region: str = "US") -> str | None:
    try:
        p = phonenumbers.parse(raw, default_region)
        if not phonenumbers.is_valid_number(p):
            return None
        return phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None

def enrich_phone(lead: Lead, default_region: str = "US") -> Lead:
    if lead.phone:
        lead.phone_e164 = normalize_phone_e164(lead.phone, default_region)
    return lead