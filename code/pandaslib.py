import re
from datetime import datetime
from functools import lru_cache
from typing import Any

# — precompile once —
_CURRENCY_RE = re.compile(r"[^\d.\-]+")  
_COUNTRY_EQUIVS = {
    "united states of america",
    "usa",
    "us",
    "united states",
    "u.s.",
}

def clean_currency(item: Any) -> float:
    """
    Strip out anything but digits, dot, or minus, then parse.
    Falls back to NaN on error.
    """
    s = str(item)
    num = _CURRENCY_RE.sub("", s)
    try:
        return float(num)
    except ValueError:
        return float("nan")


def extract_year_mdy(timestamp: str) -> int:
    """
    Fast‐path: if it looks like 'MM/DD/YYYY ...', just slice out chars 6–10.
    Otherwise fallback to strptime.
    """
    ts = str(timestamp)
    if len(ts) >= 10 and ts[2] == "/" and ts[5] == "/":
        try:
            return int(ts[6:10])
        except ValueError:
            pass
    # fallback for unexpected formats
    return datetime.strptime(ts, "%m/%d/%Y %H:%M:%S").year


@lru_cache(maxsize=None)
def clean_country_usa(item: str) -> str:
    """
    Normalize any USA variant to 'United States', else return trimmed original.
    Memoized for speed if you call it on many identical inputs.
    """
    val = item.strip()
    return "United States" if val.lower() in _COUNTRY_EQUIVS else val
