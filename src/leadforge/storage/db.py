from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _pipe_tags(tags: Iterable[str]) -> str:
    cleaned: list[str] = []
    for t in tags:
        t = (t or "").strip()
        if not t:
            continue
        t = t.replace("|", "").lower()
        if t not in cleaned:
            cleaned.append(t)
    if not cleaned:
        return ""
    return "|" + "|".join(cleaned) + "|"


def _tags_list(tags_pipe: str | None) -> list[str]:
    if not tags_pipe:
        return []
    return [p for p in tags_pipe.split("|") if p]


@dataclass(frozen=True)
class DbPaths:
    db_path: Path

    @classmethod
    def default(cls, db_path: str | None = None) -> "DbPaths":
        return cls(db_path=Path(db_path or "leadforge.db"))


class LeadDB:
    """SQLite persistence for local-first lead tracking."""

    def __init__(self, db_path: str | Path | None = "leadforge.db"):
        if db_path is None:
            db_path = "leadforge.db"
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    def init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    created_at TEXT DEFAULT '',
                    keyword TEXT DEFAULT '',
                    location TEXT DEFAULT '',
                    provider TEXT DEFAULT '',
                    total_leads INTEGER DEFAULT 0,
                    hot_leads INTEGER DEFAULT 0,
                    avg_rating REAL DEFAULT 0.0
                );
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    dedupe_key TEXT PRIMARY KEY,
                    last_run_id TEXT DEFAULT '',
                    name TEXT NOT NULL,
                    category TEXT,
                    rating REAL,
                    review_count INTEGER,
                    phone TEXT,
                    phone_e164 TEXT,
                    website TEXT,
                    website_valid INTEGER,
                    email TEXT,
                    city TEXT,
                    state TEXT,
                    source TEXT,
                    place_id TEXT,
                    raw_json TEXT,
                    score INTEGER,
                    label TEXT,
                    biz_type TEXT,
                    biz_size TEXT,
                    status TEXT DEFAULT 'new',
                    owner TEXT DEFAULT '',
                    tags TEXT DEFAULT '',
                    notes TEXT DEFAULT '',
                    created_at TEXT DEFAULT '',
                    updated_at TEXT DEFAULT '',
                    last_contacted_at TEXT DEFAULT '',
                    next_followup_at TEXT DEFAULT '',
                    contact_count INTEGER DEFAULT 0,
                    FOREIGN KEY(last_run_id) REFERENCES runs(run_id) ON DELETE SET NULL
                );
                """
            )

            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(score);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_owner ON leads(owner);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_city_state ON leads(city, state);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_run ON leads(last_run_id);")

            # backward-compatible migrations (idempotent)
            self._ensure_column(conn, "leads", "last_run_id", "TEXT DEFAULT ''")
            self._ensure_column(conn, "leads", "created_at", "TEXT DEFAULT ''")
            self._ensure_column(conn, "leads", "updated_at", "TEXT DEFAULT ''")
            self._ensure_column(conn, "leads", "last_contacted_at", "TEXT DEFAULT ''")
            self._ensure_column(conn, "leads", "next_followup_at", "TEXT DEFAULT ''")
            self._ensure_column(conn, "leads", "owner", "TEXT DEFAULT ''")
            self._ensure_column(conn, "leads", "tags", "TEXT DEFAULT ''")
            self._ensure_column(conn, "leads", "notes", "TEXT DEFAULT ''")
            self._ensure_column(conn, "leads", "contact_count", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "leads", "source", "TEXT")
            self._ensure_column(conn, "leads", "place_id", "TEXT")
            self._ensure_column(conn, "leads", "raw_json", "TEXT")

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()}
        if column not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl};")

    # -------------------------
    # Runs
    # -------------------------
    def upsert_run(self, run_id: str, **meta: Any) -> None:
        now = utc_now_iso()
        meta = meta or {}
        with self._connect() as conn:
            existing = conn.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone()
            row = {
                "run_id": run_id,
                "created_at": meta.get("created_at") or now,
                "keyword": meta.get("keyword") or "",
                "location": meta.get("location") or "",
                "provider": meta.get("provider") or "",
                "total_leads": int(meta.get("total_leads") or 0),
                "hot_leads": int(meta.get("hot_leads") or 0),
                "avg_rating": float(meta.get("avg_rating") or 0.0),
            }
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO runs(run_id, created_at, keyword, location, provider, total_leads, hot_leads, avg_rating)
                    VALUES(:run_id,:created_at,:keyword,:location,:provider,:total_leads,:hot_leads,:avg_rating)
                    """,
                    row,
                )
            else:
                conn.execute(
                    """
                    UPDATE runs SET
                        keyword=:keyword,
                        location=:location,
                        provider=:provider,
                        total_leads=:total_leads,
                        hot_leads=:hot_leads,
                        avg_rating=:avg_rating
                    WHERE run_id=:run_id
                    """,
                    row,
                )

    # -------------------------
    # Leads upsert
    # -------------------------
    def upsert_run_and_leads(self, run_id: str, leads: list[Any]) -> tuple[int, int]:
        """
        Stores run metadata (basic) and upserts all leads with last_run_id=run_id.
        Returns (inserted, updated).
        """
        # basic run summary
        total = len(leads)
        hot = len([l for l in leads if getattr(l, "label", "") == "HOT"])
        avg_rating = 0.0
        try:
            ratings = [float(getattr(l, "rating", 0) or 0) for l in leads]
            avg_rating = sum(ratings) / len(ratings) if ratings else 0.0
        except Exception:
            avg_rating = 0.0

        self.upsert_run(run_id, total_leads=total, hot_leads=hot, avg_rating=avg_rating)

        inserted = 0
        updated = 0
        now = utc_now_iso()

        with self._connect() as conn:
            for lead in leads:
                d = lead.model_dump() if hasattr(lead, "model_dump") else dict(lead)

                key = d.get("dedupe_key") or d.get("key") or d.get("dedupe")
                if not key:
                    continue

                existing = conn.execute("SELECT 1 FROM leads WHERE dedupe_key=?", (key,)).fetchone()
                payload = json.dumps(d.get("raw") or d, ensure_ascii=False)

                row = {
                    "dedupe_key": key,
                    "last_run_id": run_id,
                    "name": d.get("name") or "",
                    "category": d.get("category"),
                    "rating": d.get("rating"),
                    "review_count": d.get("review_count"),
                    "phone": d.get("phone"),
                    "phone_e164": d.get("phone_e164"),
                    "website": d.get("website"),
                    "website_valid": 1 if d.get("website_valid") else 0,
                    "email": d.get("email"),
                    "city": d.get("city"),
                    "state": d.get("state"),
                    "source": d.get("source"),
                    "place_id": d.get("place_id"),
                    "raw_json": payload,
                    "score": d.get("score"),
                    "label": d.get("label"),
                    "biz_type": d.get("biz_type"),
                    "biz_size": d.get("biz_size"),
                    "updated_at": now,
                }

                if existing is None:
                    row["created_at"] = now
                    conn.execute(
                        """
                        INSERT INTO leads (
                            dedupe_key, last_run_id, name, category, rating, review_count,
                            phone, phone_e164, website, website_valid, email,
                            city, state, source, place_id, raw_json,
                            score, label, biz_type, biz_size,
                            created_at, updated_at
                        )
                        VALUES (
                            :dedupe_key, :last_run_id, :name, :category, :rating, :review_count,
                            :phone, :phone_e164, :website, :website_valid, :email,
                            :city, :state, :source, :place_id, :raw_json,
                            :score, :label, :biz_type, :biz_size,
                            :created_at, :updated_at
                        )
                        """,
                        row,
                    )
                    inserted += 1
                else:
                    conn.execute(
                        """
                        UPDATE leads SET
                            last_run_id=:last_run_id,
                            name=:name,
                            category=:category,
                            rating=:rating,
                            review_count=:review_count,
                            phone=:phone,
                            phone_e164=:phone_e164,
                            website=:website,
                            website_valid=:website_valid,
                            email=:email,
                            city=:city,
                            state=:state,
                            source=:source,
                            place_id=:place_id,
                            raw_json=:raw_json,
                            score=:score,
                            label=:label,
                            biz_type=:biz_type,
                            biz_size=:biz_size,
                            updated_at=:updated_at
                        WHERE dedupe_key=:dedupe_key
                        """,
                        row,
                    )
                    updated += 1

        return inserted, updated

    # -------------------------
    # Queries
    # -------------------------
    def get(self, key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM leads WHERE dedupe_key=?", (key,)).fetchone()
            return dict(row) if row else None

    def delete(self, key: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM leads WHERE dedupe_key=?", (key,))
            return cur.rowcount > 0

    def list(
        self,
        status: str | None = None,
        min_score: int | None = None,
        limit: int = 50,
        q: str | None = None,
        owner: str | None = None,
        tag: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM leads WHERE 1=1"
        params: list[Any] = []

        if status:
            sql += " AND status=?"
            params.append(status)

        if min_score is not None:
            sql += " AND COALESCE(score, 0) >= ?"
            params.append(min_score)

        if owner is not None and owner != "":
            sql += " AND owner=?"
            params.append(owner)

        if tag:
            tag = tag.strip().replace("|", "").lower()
            sql += " AND tags LIKE ?"
            params.append(f"%|{tag}|%")

        if q:
            like = f"%{q}%"
            sql += " AND (name LIKE ? OR category LIKE ? OR city LIKE ? OR state LIKE ? OR email LIKE ? OR website LIKE ?)"
            params.extend([like, like, like, like, like, like])

        sql += " ORDER BY COALESCE(score,0) DESC, COALESCE(rating,0) DESC, COALESCE(review_count,0) DESC"
        sql += " LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    def kanban(self, limit: int = 25) -> dict[str, list[dict[str, Any]]]:
        cols = ["new", "contacted", "replied", "closed"]
        return {c: self.list(status=c, limit=limit) for c in cols}

    # -------------------------
    # Mutations
    # -------------------------
    def add_note(self, key: str, note: str) -> bool:
        note = (note or "").strip()
        if not note:
            return False
        with self._connect() as conn:
            row = conn.execute("SELECT notes FROM leads WHERE dedupe_key=?", (key,)).fetchone()
            if not row:
                return False
            existing = row["notes"] or ""
            new_notes = (existing + "\n" if existing else "") + note
            conn.execute("UPDATE leads SET notes=?, updated_at=? WHERE dedupe_key=?", (new_notes, utc_now_iso(), key))
            return True

    def set_status(self, key: str, status: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("UPDATE leads SET status=?, updated_at=? WHERE dedupe_key=?", (status, utc_now_iso(), key))
            return cur.rowcount > 0

    def mark_contacted(self, key: str) -> bool:
        now = utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE leads
                SET status='contacted',
                    last_contacted_at=?,
                    contact_count=COALESCE(contact_count,0)+1,
                    updated_at=?
                WHERE dedupe_key=?
                """,
                (now, now, key),
            )
            return cur.rowcount > 0

    def assign_owner(self, key: str, owner: str) -> bool:
        owner = (owner or "").strip()
        with self._connect() as conn:
            cur = conn.execute("UPDATE leads SET owner=?, updated_at=? WHERE dedupe_key=?", (owner, utc_now_iso(), key))
            return cur.rowcount > 0

    def add_tag(self, key: str, tag: str) -> bool:
        tag = (tag or "").strip().replace("|", "").lower()
        if not tag:
            return False
        with self._connect() as conn:
            row = conn.execute("SELECT tags FROM leads WHERE dedupe_key=?", (key,)).fetchone()
            if not row:
                return False
            tags = set(_tags_list(row["tags"]))
            tags.add(tag)
            conn.execute("UPDATE leads SET tags=?, updated_at=? WHERE dedupe_key=?", (_pipe_tags(tags), utc_now_iso(), key))
            return True

    def remove_tag(self, key: str, tag: str) -> bool:
        tag = (tag or "").strip().replace("|", "").lower()
        with self._connect() as conn:
            row = conn.execute("SELECT tags FROM leads WHERE dedupe_key=?", (key,)).fetchone()
            if not row:
                return False
            tags = set(_tags_list(row["tags"]))
            if tag in tags:
                tags.remove(tag)
            conn.execute("UPDATE leads SET tags=?, updated_at=? WHERE dedupe_key=?", (_pipe_tags(tags), utc_now_iso(), key))
            return True

    def set_followup_in_hours(self, key: str, hours: int) -> bool:
        if hours <= 0:
            return False
        now = datetime.now(timezone.utc)
        follow = (now + timedelta(hours=hours)).replace(microsecond=0).isoformat()
        with self._connect() as conn:
            cur = conn.execute("UPDATE leads SET next_followup_at=?, updated_at=? WHERE dedupe_key=?", (follow, utc_now_iso(), key))
            return cur.rowcount > 0

    def list_followups(self, after_hours: int = 0, limit: int = 50, status: str = "contacted") -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        with self._connect() as conn:
            sql = "SELECT * FROM leads WHERE 1=1"
            params: list[Any] = []
            if status:
                sql += " AND status=?"
                params.append(status)
            sql += " AND (next_followup_at != '' OR last_contacted_at != '')"
            rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

        due: list[dict[str, Any]] = []
        for r in rows:
            nft = (r.get("next_followup_at") or "").strip()
            lct = (r.get("last_contacted_at") or "").strip()

            if nft:
                try:
                    dt = datetime.fromisoformat(nft.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt <= now:
                        due.append(r)
                except Exception:
                    continue
            elif after_hours > 0 and lct:
                try:
                    dt = datetime.fromisoformat(lct.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt <= (now - timedelta(hours=after_hours)):
                        due.append(r)
                except Exception:
                    continue

        due.sort(key=lambda x: (x.get("next_followup_at") or "", -(x.get("score") or 0)))
        return due[:limit]

    # -------------------------
    # Bulk operations
    # -------------------------
    def bulk_set_status(self, keys: list[str], status: str) -> int:
        if not keys:
            return 0
        now = utc_now_iso()
        with self._connect() as conn:
            cur = conn.executemany("UPDATE leads SET status=?, updated_at=? WHERE dedupe_key=?", [(status, now, k) for k in keys])
            return cur.rowcount

    def bulk_add_tag(self, keys: list[str], tag: str) -> int:
        tag = (tag or "").strip().replace("|", "").lower()
        if not keys or not tag:
            return 0
        changed = 0
        with self._connect() as conn:
            for k in keys:
                row = conn.execute("SELECT tags FROM leads WHERE dedupe_key=?", (k,)).fetchone()
                if not row:
                    continue
                tags = set(_tags_list(row["tags"]))
                before = set(tags)
                tags.add(tag)
                if tags != before:
                    conn.execute("UPDATE leads SET tags=?, updated_at=? WHERE dedupe_key=?", (_pipe_tags(tags), utc_now_iso(), k))
                    changed += 1
        return changed

    # -------------------------
    # Analytics
    # -------------------------
    def stats(self, top_n: int = 5) -> dict[str, Any]:
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) AS c FROM leads").fetchone()["c"]

            status_rows = conn.execute("SELECT status, COUNT(*) AS c FROM leads GROUP BY status ORDER BY c DESC").fetchall()
            status_counts = {r["status"]: r["c"] for r in status_rows}

            avg_score = conn.execute("SELECT AVG(COALESCE(score,0)) AS a FROM leads").fetchone()["a"] or 0
            avg_rating = conn.execute("SELECT AVG(COALESCE(rating,0)) AS a FROM leads").fetchone()["a"] or 0

            hot_count = conn.execute("SELECT COUNT(*) AS c FROM leads WHERE label='HOT'").fetchone()["c"] or 0
            hot_rate = (hot_count / total) if total else 0

            top_categories = [
                dict(r)
                for r in conn.execute(
                    "SELECT COALESCE(category,'') AS category, COUNT(*) AS count FROM leads GROUP BY category ORDER BY count DESC LIMIT ?",
                    (top_n,),
                ).fetchall()
            ]
            top_owners = [
                dict(r)
                for r in conn.execute(
                    "SELECT COALESCE(owner,'') AS owner, COUNT(*) AS count FROM leads GROUP BY owner ORDER BY count DESC LIMIT ?",
                    (top_n,),
                ).fetchall()
            ]

            tag_counts: dict[str, int] = {}
            rows = conn.execute("SELECT tags FROM leads WHERE tags != ''").fetchall()
            for r in rows:
                for t in _tags_list(r["tags"]):
                    tag_counts[t] = tag_counts.get(t, 0) + 1
            top_tags = [{"tag": k, "count": v} for k, v in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]]

        return {
            "total": total,
            "status_counts": status_counts,
            "avg_score": round(float(avg_score), 2),
            "avg_rating": round(float(avg_rating), 2),
            "hot_count": hot_count,
            "hot_rate": round(float(hot_rate), 3),
            "top_categories": top_categories,
            "top_owners": top_owners,
            "top_tags": top_tags,
        }

    # -------------------------
    # ICS Export (followups)
    # -------------------------
    def export_followups_ics(
        self,
        output_path: str | Path,
        after_hours: int = 48,
        limit: int = 200,
        status: str = "contacted",
    ) -> Path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        due = self.list_followups(after_hours=after_hours, limit=limit, status=status)

        def fmt_dt(dt: datetime) -> str:
            dt = dt.astimezone(timezone.utc).replace(microsecond=0)
            return dt.strftime("%Y%m%dT%H%M%SZ")

        now = datetime.now(timezone.utc)
        lines: list[str] = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//LeadForge Pro//EN",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
        ]

        for r in due:
            nft = (r.get("next_followup_at") or "").strip()
            lct = (r.get("last_contacted_at") or "").strip()

            when: datetime | None = None
            if nft:
                try:
                    when = datetime.fromisoformat(nft.replace("Z", "+00:00"))
                    if when.tzinfo is None:
                        when = when.replace(tzinfo=timezone.utc)
                except Exception:
                    when = None
            if when is None and lct:
                try:
                    base = datetime.fromisoformat(lct.replace("Z", "+00:00"))
                    if base.tzinfo is None:
                        base = base.replace(tzinfo=timezone.utc)
                    when = base + timedelta(hours=after_hours)
                except Exception:
                    when = None
            if when is None:
                continue

            uid = f"{r.get('dedupe_key')}@leadforge"
            summary = f"Follow up: {r.get('name','')}"
            desc = (
                "LeadForge follow-up\\n"
                f"Score: {r.get('score','')}\\n"
                f"Email: {r.get('email','')}\\n"
                f"Website: {r.get('website','')}\\n"
                f"Key: {r.get('dedupe_key','')}"
            )

            lines.extend(
                [
                    "BEGIN:VEVENT",
                    f"UID:{uid}",
                    f"DTSTAMP:{fmt_dt(now)}",
                    f"DTSTART:{fmt_dt(when)}",
                    f"SUMMARY:{summary}",
                    f"DESCRIPTION:{desc}",
                    "END:VEVENT",
                ]
            )

        lines.append("END:VCALENDAR")
        output.write_text("\\n".join(lines), encoding="utf-8")
        return output
