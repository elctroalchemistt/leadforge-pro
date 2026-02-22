from leadforge.enrichment.website import validate_website
from leadforge.enrichment.phone import normalize_phone_e164
from leadforge.models import Lead
from leadforge.enrichment.dedupe import dedupe

def test_validate_website():
    assert validate_website("https://example.com")
    assert not validate_website("example.com")

def test_phone_normalize():
    out = normalize_phone_e164("(312) 555-1212", "US")
    assert out is None or out.startswith("+1")

def test_dedupe():
    leads = [
        Lead(name="X", city="Chicago", state="IL", website="https://a.com"),
        Lead(name="X", city="Chicago", state="IL", website="https://a.com"),
    ]
    out = dedupe(leads)
    assert len(out) == 1