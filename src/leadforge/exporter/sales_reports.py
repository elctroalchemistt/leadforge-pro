from __future__ import annotations

from pathlib import Path
from leadforge.models import Lead
from leadforge.exporter.csv_exporter import export_csv
from leadforge.exporter.json_exporter import export_json
from leadforge.exporter.report import summarize, top_hot

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def export_hot_csv(leads: list[Lead], out_path: str | Path, min_score: int = 5) -> int:
    hot = [l for l in leads if l.score >= min_score]
    export_csv(hot, str(out_path))
    return len(hot)

def export_summary_json(leads: list[Lead], out_path: str | Path) -> None:
    s = summarize(leads)
    export_json([s], str(out_path))  # store as list for easy json compatibility

def export_hot_markdown(leads: list[Lead], out_path: str | Path, min_score: int = 5, top_n: int = 25) -> None:
    s = summarize(leads)
    hot = [l for l in leads if l.score >= min_score]
    hot_sorted = top_hot(hot, n=top_n)

    lines: list[str] = []
    lines.append("# LeadForge Pro – HOT Leads Report\n")
    lines.append(f"- Total leads: **{s.total_leads}**")
    lines.append(f"- HOT (>= {min_score}): **{len(hot)}**")
    lines.append(f"- Average rating: **{s.average_rating}**\n")

    lines.append("## Top HOT Leads\n")
    lines.append("| # | Name | Score | Rating | Reviews | Email | Phone | Website | City | State |")
    lines.append("|---:|---|---:|---:|---:|---|---|---|---|---|")

    for i, l in enumerate(hot_sorted, start=1):
        lines.append(
            f"| {i} | {l.name} | {l.score} | {l.rating or ''} | {l.review_count or ''} | "
            f"{l.email or ''} | {l.phone_e164 or l.phone or ''} | {l.website or ''} | {l.city or ''} | {l.state or ''} |"
        )

    Path(out_path).write_text("\n".join(lines), encoding="utf-8")