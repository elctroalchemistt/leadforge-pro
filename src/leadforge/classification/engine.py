from leadforge.models import Lead

B2C_HINTS = {"dentist", "hair", "salon", "restaurant", "cafe", "clinic"}
B2B_HINTS = {"agency", "software", "consulting", "logistics", "wholesale", "b2b"}

def classify_biz_type(category: str | None) -> str:
    if not category:
        return "UNKNOWN"
    c = category.lower()
    if any(k in c for k in B2B_HINTS):
        return "B2B"
    if any(k in c for k in B2C_HINTS):
        return "B2C"
    return "UNKNOWN"

def estimate_size(review_count: int | None) -> str:
    if review_count is None:
        return "UNKNOWN"
    if review_count < 200:
        return "SMALL"
    return "MEDIUM"

def apply_classification(leads: list[Lead]) -> list[Lead]:
    for l in leads:
        l.biz_type = classify_biz_type(l.category)  # type: ignore
        l.biz_size = estimate_size(l.review_count)  # type: ignore
    return leads