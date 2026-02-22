from leadforge.models import Lead
from leadforge.scoring.rules import score_lead

def test_hot_lead():
    l = Lead(
        name="A",
        website="https://a.com",
        email="x@a.com",
        rating=4.5,
        review_count=120,
        phone="+1 312-000-0000",
    )
    r = score_lead(l)
    assert r.score >= 5
    assert r.label == "HOT"

def test_cold_lead():
    l = Lead(name="B", rating=3.8, review_count=5)
    r = score_lead(l)
    assert r.label == "COLD"