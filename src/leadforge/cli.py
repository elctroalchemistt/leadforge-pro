from __future__ import annotations

import asyncio
import csv
import json
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import typer
from rich import print
from rich.console import Console
from rich.table import Table

from leadforge.logging import setup_logging
from leadforge.models import Lead
from leadforge.scraper import get_provider
from leadforge.enrichment.pipeline import enrich_all
from leadforge.scoring.engine import apply_scoring
from leadforge.classification.engine import apply_classification

# DB module can live in leadforge/storage/db.py (your tree) OR leadforge/db.py (older layouts)
try:
    from leadforge.storage.db import LeadDB  # type: ignore
except Exception:  # pragma: no cover
    from leadforge.db import LeadDB  # type: ignore


log = logging.getLogger("leadforge")
app = typer.Typer(add_completion=False, help="LeadForge Pro – Business Lead Intelligence Engine")
console = Console()


def abspath(p: str | Path) -> str:
    return str(Path(p).expanduser().resolve())


def run_async(coro: Any) -> Any:
    """
    Run coroutine safely from CLI.
    - If no loop is running → asyncio.run()
    - If a loop is already running (rare in normal terminals) → raise a clear error
    """
    try:
        asyncio.get_running_loop()
        raise RuntimeError(
            "An event loop is already running. Run this command from a normal terminal "
            "(not inside another async environment)."
        )
    except RuntimeError as e:
        if "no running event loop" in str(e).lower():
            return asyncio.run(coro)
        raise


def load_leads(path: str) -> list[Lead]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input not found: {abspath(p)}")
    data = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "items" in data:
        data = data["items"]
    return [Lead(**x) for x in data]


def dump_leads(leads: Iterable[Lead]) -> list[dict[str, Any]]:
    return [l.model_dump() for l in leads]


def export_json(leads: Iterable[Lead], path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(dump_leads(leads), ensure_ascii=False, indent=2), encoding="utf-8")


def export_csv(leads: Iterable[Lead], path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = dump_leads(leads)
    if not rows:
        p.write_text("", encoding="utf-8")
        return
    # Stable field order: put the most important columns first, then the rest.
    preferred = [
        "dedupe_key", "name", "category", "category_norm",
        "city", "state", "rating", "review_count",
        "phone", "phone_e164", "website", "website_valid", "email",
        "score", "label", "biz_type", "biz_size",
        "source", "place_id",
    ]
    all_keys: list[str] = []
    seen = set()
    for k in preferred:
        if k in rows[0] and k not in seen:
            all_keys.append(k); seen.add(k)
    for k in sorted(rows[0].keys()):
        if k not in seen:
            all_keys.append(k); seen.add(k)

    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def summarize(leads: list[Lead]) -> dict[str, Any]:
    total = len(leads)
    if total == 0:
        return {"total_leads": 0, "hot_leads": 0, "warm_leads": 0, "cold_leads": 0, "average_rating": None}
    avg_rating = sum((l.rating or 0) for l in leads) / total
    hot = sum(1 for l in leads if (l.label or "") == "HOT")
    warm = sum(1 for l in leads if (l.label or "") == "WARM")
    cold = sum(1 for l in leads if (l.label or "") == "COLD")
    return {
        "total_leads": total,
        "hot_leads": hot,
        "warm_leads": warm,
        "cold_leads": cold,
        "average_rating": round(avg_rating, 2),
    }


def top_hot(leads: list[Lead], n: int = 10, min_score: int = 5) -> list[Lead]:
    hot = [l for l in leads if (l.score or 0) >= min_score]
    hot.sort(key=lambda x: (x.score or 0, x.rating or 0, x.review_count or 0), reverse=True)
    return hot[:n]


def write_reports(leads: list[Lead], reports_dir: str | Path, min_score: int = 5, sender: str = "LeadForge Pro") -> dict[str, str]:
    out_dir = Path(reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = summarize(leads)
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    hot = top_hot(leads, n=50, min_score=min_score)

    hot_md = out_dir / "hot_leads.md"
    md_lines = ["# Top HOT Leads", ""]
    md_lines.append(f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md_lines.append(f"- Min score: {min_score}")
    md_lines.append("")
    if not hot:
        md_lines.append("_No HOT leads for this threshold._")
    else:
        for l in hot:
            md_lines.append(
                f"- **{l.name}** | score={l.score} | rating={l.rating} | reviews={l.review_count} | "
                f"email={l.email or ''} | website={l.website or ''} | key={l.dedupe_key}"
            )
    hot_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    hot_csv = out_dir / "hot_leads.csv"
    export_csv(hot, hot_csv)

    outreach = out_dir / "outreach_messages.md"
    o_lines = ["# Outreach Messages", ""]
    o_lines.append(f"Sender: **{sender}**")
    o_lines.append("")
    if not hot:
        o_lines.append("_No leads to generate outreach._")
    else:
        for l in hot:
            channel = "email" if l.email else ("website" if l.website else "phone")
            o_lines.append(f"## {l.name}")
            o_lines.append(f"- Channel: **{channel}**")
            o_lines.append(f"- Contact: `{l.email or l.website or l.phone or ''}`")
            o_lines.append("")
            o_lines.append(
                f"Hi {l.name},\n\n"
                f"My name is {sender}. I noticed your business in {l.city or ''} {l.state or ''}.\n"
                f"I help local businesses improve lead flow + follow-ups with lightweight automation.\n\n"
                f"If you're open to it, I can share 2–3 quick improvements for your current setup.\n\n"
                f"Thanks,\n{sender}\n"
            )
            o_lines.append("\n---\n")
    outreach.write_text("\n".join(o_lines), encoding="utf-8")

    return {
        "summary": abspath(summary_path),
        "hot_md": abspath(hot_md),
        "hot_csv": abspath(hot_csv),
        "outreach": abspath(outreach),
    }


def ensure_sample_exists() -> None:
    """
    For mock provider to work, ensure examples/sample_leads.json exists.
    If it doesn't exist, create a minimal sample so the CLI never crashes.
    """
    p = Path("examples") / "sample_leads.json"
    if p.exists():
        return
    p.parent.mkdir(parents=True, exist_ok=True)
    sample = [
        {
            "name": "Smile Dental Chicago",
            "category": "Dentist",
            "rating": 4.6,
            "review_count": 132,
            "phone": "(312) 555-1212",
            "website": "https://smiledental.example",
            "email": "hello@smiledental.example",
            "city": "Chicago",
            "state": "IL",
            "source": "mock",
        },
        {
            "name": "Northside Family Clinic",
            "category": "Clinic",
            "rating": 4.2,
            "review_count": 48,
            "phone": "(312) 555-3434",
            "website": None,
            "email": None,
            "city": "Chicago",
            "state": "IL",
            "source": "mock",
        },
    ]
    p.write_text(json.dumps(sample, ensure_ascii=False, indent=2), encoding="utf-8")


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging")) -> None:
    # Backwards-compatible logging setup: older versions of leadforge.logging.setup_logging()
    # might not accept a `verbose=` kwarg.
    try:
        setup_logging(verbose=verbose)  # type: ignore[arg-type]
    except TypeError:
        setup_logging()
        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)

    if verbose:
        log.info("Verbose logging enabled")


@app.command()
def scrape(
    keyword: str = typer.Option(..., "--keyword"),
    location: str = typer.Option(..., "--location"),
    limit: int = typer.Option(50, "--limit"),
    output: str = typer.Option("leads_raw.json", "--output", "-o"),
    provider: str | None = typer.Option(None, "--provider", help="Override provider (mock / places_api) if supported"),
) -> None:
    """Scrape leads using configured provider (mock / places_api)."""
    ensure_sample_exists()

    async def runner() -> None:
        prov = get_provider(provider_name=provider) if provider else get_provider()
        leads = await prov.search(keyword=keyword, location=location, limit=limit)
        export_json(leads, output)
        print(f"[bold green]Scraped[/bold green] {len(leads)} -> {abspath(output)}")

    run_async(runner())


@app.command()
def score(
    input: str = typer.Option(..., "--input", "-i"),
    output: str = typer.Option("scored.json", "--output", "-o"),
    region: str = typer.Option("US", "--region", help="Default region for phone parsing"),
) -> None:
    """Enrich + score + classify."""
    async def runner() -> None:
        leads = load_leads(input)
        leads = await enrich_all(leads, default_region=region)
        leads = apply_scoring(leads)
        leads = apply_classification(leads)
        export_json(leads, output)
        s = summarize(leads)
        print(f"[bold green]Done[/bold green] total={s['total_leads']} hot={s['hot_leads']} avg_rating={s['average_rating']} -> {abspath(output)}")

    run_async(runner())


@app.command()
def export(
    input: str = typer.Option(..., "--input", "-i"),
    format: str = typer.Option("csv", "--format", help="csv|json|sales"),
    output: str = typer.Option("leads.csv", "--output", "-o"),
    reports_dir: str = typer.Option("reports", "--reports-dir", help="Used for format=sales"),
    min_score: int = typer.Option(5, "--min-score", help="Used for format=sales"),
    sender: str = typer.Option("LeadForge Pro", "--sender", help="Used for format=sales (outreach text)"),
) -> None:
    """Export scored leads to CSV/JSON. 'sales' creates a report bundle."""
    leads = load_leads(input)

    fmt = format.lower().strip()
    if fmt == "json":
        export_json(leads, output)
        print(f"[bold green]Exported[/bold green] -> {abspath(output)}")
        return
    if fmt == "csv":
        export_csv(leads, output)
        print(f"[bold green]Exported[/bold green] -> {abspath(output)}")
        return
    if fmt == "sales":
        paths = write_reports(leads, reports_dir=reports_dir, min_score=min_score, sender=sender)
        print(f"[bold green]Sales reports generated[/bold green] -> {abspath(reports_dir)}")
        for k in ["summary", "hot_csv", "hot_md", "outreach"]:
            print(f"- {paths[k]}")
        return

    raise typer.BadParameter("format must be one of: csv, json, sales")


@app.command()
def report(
    input: str = typer.Option(..., "--input", "-i"),
    top: int = typer.Option(10, "--top"),
    min_score: int = typer.Option(5, "--min-score"),
) -> None:
    """Summary + Top HOT leads."""
    leads = load_leads(input)
    s = summarize(leads)
    print("[bold]Summary:[/bold]")
    print(s)
    print("\n[bold]Top HOT leads:[/bold]")
    for l in top_hot(leads, n=top, min_score=min_score):
        print(f"- {l.name} | score={l.score} | rating={l.rating} | reviews={l.review_count} | email={l.email or ''} | website={l.website or ''}")


@app.command()
def pipeline(
    keyword: str = typer.Option(..., "--keyword"),
    location: str = typer.Option(..., "--location"),
    limit: int = typer.Option(50, "--limit"),
    region: str = typer.Option("US", "--region"),
    provider: str | None = typer.Option(None, "--provider", help="Override provider (mock / places_api) if supported"),
    raw_out: str = typer.Option("leads_raw.json", "--raw-out"),
    scored_out: str = typer.Option("scored.json", "--scored-out"),
    reports_dir: str = typer.Option("reports", "--reports-dir"),
    run_id: str = typer.Option("run", "--run-id", help="Subfolder name for reports (e.g. demo-001)"),
    min_score: int = typer.Option(5, "--min-score"),
    sender: str = typer.Option("LeadForge Pro", "--sender"),
    save_db: bool = typer.Option(False, "--save-db", help="Upsert scored leads into SQLite DB"),
    db_path: str | None = typer.Option(None, "--db-path", help="SQLite db path (default: leadforge.db)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Run but do not write outputs"),
) -> None:
    """One-shot: scrape -> score -> reports (-> optional DB upsert)."""
    ensure_sample_exists()

    async def runner() -> None:
        prov = get_provider(provider_name=provider) if provider else get_provider()

        # 1) Scrape
        leads = await prov.search(keyword=keyword, location=location, limit=limit)
        if not dry_run:
            export_json(leads, raw_out)
        print(f"[bold green]Scraped[/bold green] {len(leads)} -> {abspath(raw_out)}")

        # 2) Enrich + Score + Classify
        leads = await enrich_all(leads, default_region=region)
        leads = apply_scoring(leads)
        leads = apply_classification(leads)
        if not dry_run:
            export_json(leads, scored_out)
        print(f"[bold green]Scored[/bold green] {len(leads)} -> {abspath(scored_out)}")

        # 3) Reports
        final_reports_dir = Path(reports_dir) / run_id if run_id else Path(reports_dir)
        if not dry_run:
            paths = write_reports(leads, reports_dir=final_reports_dir, min_score=min_score, sender=sender)
            print(f"[bold green]Reports[/bold green] -> {abspath(final_reports_dir)}")
            print(f"- {paths['hot_md']}")
            print(f"- {paths['outreach']}")
        else:
            print(f"[yellow]Dry-run:[/yellow] skipping report writing")

        # 4) Optional DB upsert
        if save_db and not dry_run:
            db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
            db.init()
            inserted, updated = db.upsert_leads(leads, run_id=run_id)  # type: ignore[attr-defined]
            print(f"[bold green]DB[/bold green] inserted={inserted} updated={updated} -> {abspath(db.path)}")  # type: ignore[attr-defined]

    run_async(runner())


# -------------------------
# DB Commands
# -------------------------

@app.command("db-init")
def db_init(db_path: str | None = typer.Option(None, "--db-path")) -> None:
    """Initialize SQLite DB (creates tables / migrates)."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    db.init()
    print(f"[bold green]DB ready[/bold green] -> {abspath(str(db.db_path))}")# type: ignore[attr-defined]


def _render_table(rows: list[dict[str, Any]], columns: list[str], title: str) -> None:
    t = Table(title=title, show_lines=False)
    for c in columns:
        t.add_column(c)
    for r in rows:
        t.add_row(*[str(r.get(c, "") or "") for c in columns])
    console.print(t)


@app.command("db-list")
def db_list(
    status: str | None = typer.Option(None, "--status"),
    min_score: int = typer.Option(0, "--min-score"),
    limit: int = typer.Option(50, "--limit"),
    tag: str | None = typer.Option(None, "--tag"),
    owner: str | None = typer.Option(None, "--owner"),
    table: bool = typer.Option(False, "--table", help="Render results as a table"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """List leads from DB with filters."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    rows = db.query_leads(status=status, min_score=min_score, limit=limit, tag=tag, owner=owner)  # type: ignore[attr-defined]
    print(f"Rows: {len(rows)}")
    if table and rows:
        _render_table(rows, ["dedupe_key", "name", "score", "label", "status", "owner", "email", "website", "next_followup"], "Leads")
    else:
        for r in rows:
            print(f"- {r.get('name')} | score={r.get('score')} | label={r.get('label')} | status={r.get('status')} | owner={r.get('owner','')} | email={r.get('email','')} | website={r.get('website','')} | key={r.get('dedupe_key')}")


@app.command("db-search")
def db_search(
    q: str = typer.Option(..., "--q"),
    limit: int = typer.Option(20, "--limit"),
    table: bool = typer.Option(False, "--table"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Search leads by name, city, state, email, website."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    rows = db.search(q=q, limit=limit)  # type: ignore[attr-defined]
    print(f"Matches: {len(rows)}")
    if table and rows:
        _render_table(rows, ["dedupe_key", "name", "score", "label", "status", "owner", "email", "website"], "Matches")
    else:
        for r in rows:
            print(f"- {r.get('name')} | score={r.get('score')} | status={r.get('status')} | email={r.get('email','')} | website={r.get('website','')} | key={r.get('dedupe_key')}")


@app.command("db-show")
def db_show(
    key: str = typer.Option(..., "--key"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Show one lead (full JSON)"""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    row = db.get(key)  # type: ignore[attr-defined]
    if not row:
        raise typer.BadParameter(f"Not found: {key}")
    print(json.dumps(row, ensure_ascii=False, indent=2))


@app.command("db-note")
def db_note(
    key: str = typer.Option(..., "--key"),
    note: str = typer.Option(..., "--note"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Append note to a lead."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    db.add_note(key=key, note=note)  # type: ignore[attr-defined]
    print(f"[bold green]Note added[/bold green] -> {key}")


@app.command("db-assign")
def db_assign(
    key: str = typer.Option(..., "--key"),
    owner: str = typer.Option(..., "--owner"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Assign owner."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    db.set_owner(key=key, owner=owner)  # type: ignore[attr-defined]
    print(f"[bold green]Owner set[/bold green] -> {key} owner='{owner}'")


@app.command("db-tag-add")
def db_tag_add(
    key: str = typer.Option(..., "--key"),
    tag: str = typer.Option(..., "--tag"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Add tag."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    tags = db.tag_add(key=key, tag=tag)  # type: ignore[attr-defined]
    print(f"[bold green]Tag added[/bold green] -> {key} tags={tags}")


@app.command("db-tag-remove")
def db_tag_remove(
    key: str = typer.Option(..., "--key"),
    tag: str = typer.Option(..., "--tag"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Remove tag."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    tags = db.tag_remove(key=key, tag=tag)  # type: ignore[attr-defined]
    print(f"[bold green]Tag removed[/bold green] -> {key} tags={tags}")


@app.command("db-mark-contacted")
def db_mark_contacted(
    key: str = typer.Option(..., "--key"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Set status=contacted and last_contacted=now."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    db.mark_contacted(key=key)  # type: ignore[attr-defined]
    print(f"[bold green]Contacted[/bold green] -> {key}")


@app.command("db-set-status")
def db_set_status(
    key: str = typer.Option(..., "--key"),
    status: str = typer.Option(..., "--status"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Set lead status explicitly."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    db.set_status(key=key, status=status)  # type: ignore[attr-defined]
    print(f"[bold green]Status set[/bold green] -> {key} status={status}")


@app.command("db-followup-set")
def db_followup_set(
    key: str = typer.Option(..., "--key"),
    hours: int = typer.Option(48, "--hours"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Schedule next follow-up (+hours)."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    db.set_followup_in(key=key, hours=hours)  # type: ignore[attr-defined]
    print(f"[bold green]Follow-up scheduled[/bold green] -> {key} (+{hours}h)")


@app.command("db-followups")
def db_followups(
    after_hours: int = typer.Option(0, "--after-hours"),
    limit: int = typer.Option(50, "--limit"),
    table: bool = typer.Option(False, "--table"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """List leads due for follow-up (status=contacted and next_followup <= now+after_hours)."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    rows = db.followups(after_hours=after_hours, limit=limit)  # type: ignore[attr-defined]
    print(f"Follow-ups\nRows: {len(rows)} (status=contacted, after_hours={after_hours})")
    if table and rows:
        _render_table(rows, ["dedupe_key", "name", "score", "label", "owner", "next_followup", "email", "website"], "Follow-ups")
    else:
        for r in rows:
            print(f"- {r.get('name')} | next_followup={r.get('next_followup')} | score={r.get('score')} | owner={r.get('owner','')} | key={r.get('dedupe_key')}")


@app.command("db-stats")
def db_stats(
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Show DB stats (counts, averages, top categories/owners/tags)."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    s = db.stats()  # type: ignore[attr-defined]
    print("[bold]DB Stats[/bold]")
    print(s)


@app.command("db-kanban")
def db_kanban(
    limit: int = typer.Option(10, "--limit"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Kanban view by status."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    board = db.kanban(limit=limit)  # type: ignore[attr-defined]
    print("[bold]Kanban[/bold]\n")
    for status, rows in board.items():
        print(f"[bold]{status.upper()} ({len(rows)})[/bold]")
        for r in rows:
            print(f"- {r.get('name')} | score={r.get('score')} | label={r.get('label')} | email={r.get('email','')} | key={r.get('dedupe_key')}")
        print("")


@app.command("db-bulk-status")
def db_bulk_status(
    status: str = typer.Option(..., "--status"),
    min_score: int = typer.Option(0, "--min-score"),
    tag: str | None = typer.Option(None, "--tag"),
    owner: str | None = typer.Option(None, "--owner"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Bulk set status for filtered leads."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    n = db.bulk_set_status(status=status, min_score=min_score, tag=tag, owner=owner)  # type: ignore[attr-defined]
    print(f"[bold green]Bulk status updated[/bold green] rows={n}")


@app.command("db-bulk-tag")
def db_bulk_tag(
    tag: str = typer.Option(..., "--tag"),
    min_score: int = typer.Option(0, "--min-score"),
    status: str | None = typer.Option(None, "--status"),
    owner: str | None = typer.Option(None, "--owner"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Bulk add a tag for filtered leads."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    n = db.bulk_add_tag(tag=tag, min_score=min_score, status=status, owner=owner)  # type: ignore[attr-defined]
    print(f"[bold green]Bulk tag added[/bold green] rows={n}")


@app.command("db-delete")
def db_delete(
    key: str = typer.Option(..., "--key"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Delete a lead."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    db.delete(key=key)  # type: ignore[attr-defined]
    print(f"[bold green]Deleted[/bold green] -> {key}")


@app.command("db-export-ics")
def db_export_ics(
    output: str = typer.Option("followups.ics", "--output", "-o"),
    after_hours: int = typer.Option(168, "--after-hours", help="Export follow-ups due in the next N hours"),
    db_path: str | None = typer.Option(None, "--db-path"),
) -> None:
    """Export follow-ups as an .ics calendar file (import into Google Calendar)."""
    db = LeadDB(db_path=db_path)  # type: ignore[arg-type]
    rows = db.followups(after_hours=after_hours, limit=10000)  # type: ignore[attr-defined]

    def ics_escape(s: str) -> str:
        return s.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")

    # ICS uses UTC times in format YYYYMMDDTHHMMSSZ
    now = datetime.now(timezone.utc)
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//LeadForge Pro//EN"]

    for r in rows:
        key = str(r.get("dedupe_key") or "")
        name = str(r.get("name") or "Lead")
        dt = r.get("next_followup")
        # dt can be stored as ISO string; tolerate both string/datetime
        if isinstance(dt, str) and dt:
            try:
                dt_obj = datetime.fromisoformat(dt)
            except Exception:
                dt_obj = now + timedelta(hours=1)
        elif isinstance(dt, datetime):
            dt_obj = dt
        else:
            dt_obj = now + timedelta(hours=1)

        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        dt_obj = dt_obj.astimezone(timezone.utc)

        uid = f"{key}@leadforge"
        start = dt_obj.strftime("%Y%m%dT%H%M%SZ")
        end = (dt_obj + timedelta(minutes=30)).strftime("%Y%m%dT%H%M%SZ")

        desc = f"Follow up with {name}\\nKey: {key}\\nEmail: {r.get('email','')}\\nWebsite: {r.get('website','')}"
        lines += [
            "BEGIN:VEVENT",
            f"UID:{ics_escape(uid)}",
            f"DTSTAMP:{now.strftime('%Y%m%dT%H%M%SZ')}",
            f"DTSTART:{start}",
            f"DTEND:{end}",
            f"SUMMARY:{ics_escape('Follow-up: ' + name)}",
            f"DESCRIPTION:{ics_escape(desc)}",
            "END:VEVENT",
        ]

    lines.append("END:VCALENDAR")

    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[bold green]ICS exported[/bold green] -> {abspath(out)} (events={len(rows)})")


if __name__ == "__main__":
    app()
