import re

US_STATE_CODES = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY"
)

# Example: "123 Main St, Chicago IL 60601, USA"
# We enforce word boundaries around the state code so it won't match inside "cHIcago".
PATTERN = re.compile(
    rf",\s*(?P<city>[^,]+?)\s+(?P<state>\b(?:{US_STATE_CODES})\b)\s+(?P<zip>\d{{5}}(?:-\d{{4}})?)",
    re.IGNORECASE,
)

def parse_city_state_from_address(address: str | None) -> tuple[str | None, str | None]:
    if not address:
        return None, None
    m = PATTERN.search(address)
    if not m:
        return None, None
    city = m.group("city").strip()
    state = m.group("state").upper().strip()
    return city, state