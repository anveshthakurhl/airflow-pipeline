from datetime import datetime

def to_iso(date_str):
    if not isinstance(date_str, str):
        raise ValueError(f"Expected string, got {type(date_str)}")

    date_str = date_str.strip()

    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue

    raise ValueError(f"Unrecognized date format: {date_str!r}")
