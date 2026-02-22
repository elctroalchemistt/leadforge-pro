import json
from leadforge.models import Lead

def export_json(leads: list[Lead], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([l.model_dump() for l in leads], f, ensure_ascii=False, indent=2)