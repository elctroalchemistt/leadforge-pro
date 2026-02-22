from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from leadforge.models import Lead


Channel = Literal["email", "whatsapp"]


@dataclass(frozen=True)
class OutreachTemplates:
    email_subject: str = "Quick question about {business}"
    email_body: str = (
        "Hi {name_or_team},\n\n"
        "I came across {business} and wanted to reach out.\n"
        "Do you currently handle {hook} in-house, or do you have someone helping?\n\n"
        "If you’re open to it, I can share 2–3 quick ideas to improve results with minimal effort.\n"
        "Either way, love what you’re doing.\n\n"
        "Best,\n"
        "{sender}\n"
    )
    whatsapp_body: str = (
        "Hi {name_or_team} 👋 I found {business} and had a quick question.\n"
        "Are you currently working on {hook}? If you want, I can share a couple of quick wins.\n"
        "- {sender}"
    )


def guess_hook(lead: Lead) -> str:
    # super simple heuristic; improve later per category
    c = (lead.category or "").lower()
    if "dent" in c or "clinic" in c:
        return "getting more local calls / appointment bookings"
    if "restaurant" in c or "cafe" in c:
        return "more reservations from Google"
    if "salon" in c or "hair" in c:
        return "more bookings + better reviews"
    return "more leads from Google"


def render_message(
    lead: Lead,
    sender: str = "LeadForge Pro",
    templates: OutreachTemplates = OutreachTemplates(),
) -> dict[str, str]:
    name_or_team = "there"
    # If we had a contact name we’d use it; for now "team" fallback
    if lead.name:
        name_or_team = "the team"

    hook = guess_hook(lead)
    data = {
        "business": lead.name,
        "name_or_team": name_or_team,
        "hook": hook,
        "sender": sender,
    }

    subject = templates.email_subject.format(**data)
    email = templates.email_body.format(**data)
    wa = templates.whatsapp_body.format(**data)

    return {
        "email_subject": subject,
        "email_body": email,
        "whatsapp_body": wa,
    }


def export_outreach_markdown(
    leads: list[Lead],
    out_path: str,
    min_score: int = 5,
    sender: str = "LeadForge Pro",
) -> None:
    hot = [l for l in leads if l.score >= min_score]
    lines: list[str] = []
    lines.append("# Outreach Messages (HOT Leads)\n")
    lines.append(f"- HOT threshold: **{min_score}**")
    lines.append(f"- HOT leads: **{len(hot)}**\n")

    for i, l in enumerate(hot, start=1):
        msg = render_message(l, sender=sender)
        lines.append(f"## {i}) {l.name} (score={l.score})\n")
        lines.append(f"- Website: {l.website or ''}")
        lines.append(f"- Email: {l.email or ''}")
        lines.append(f"- Phone: {l.phone_e164 or l.phone or ''}\n")

        lines.append("### Email\n")
        lines.append(f"**Subject:** {msg['email_subject']}\n")
        lines.append("```text")
        lines.append(msg["email_body"].rstrip())
        lines.append("```\n")

        lines.append("### WhatsApp\n")
        lines.append("```text")
        lines.append(msg["whatsapp_body"].rstrip())
        lines.append("```\n")

    from pathlib import Path
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text("\n".join(lines), encoding="utf-8")