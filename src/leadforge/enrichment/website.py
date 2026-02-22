from urllib.parse import urlparse
from leadforge.models import Lead

def validate_website(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def enrich_website(lead: Lead) -> Lead:
    if lead.website:
        lead.website_valid = validate_website(lead.website)
    else:
        lead.website_valid = False
    return lead