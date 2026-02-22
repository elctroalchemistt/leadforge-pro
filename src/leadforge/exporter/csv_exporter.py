import csv
from leadforge.models import Lead

def export_csv(leads: list[Lead], path: str) -> None:
    if not leads:
        open(path, "w", encoding="utf-8").close()
        return

    fields = list(leads[0].model_dump().keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for l in leads:
            w.writerow(l.model_dump())