import re
import sys
import math
import json
import time
import shutil
import traceback
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta, time as dtime
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

import pandas as pd
from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Locator,
    Page,
)

# ============================================================
# CONFIG
# ============================================================

@dataclass
class Config:
    origin: str = "BUF"
    destination: str = "BZN"

    start_date: date = date(2026, 7, 1)
    end_date: date = date(2026, 7, 31)

    min_trip_days: int = 6
    max_trip_days: int = 8

    passengers: int = 1
    round_trip: bool = True
    allowed_airlines: List[str] = field(default_factory=list)

    earliest_departure_time: Optional[dtime] = dtime(7, 0)   # None means no restriction
    latest_arrival_time: Optional[dtime] = dtime(22, 0)      # None means no restriction

    # Search behavior
    max_outbound_cards_to_try: int = 8
    max_return_cards_to_try: int = 8
    max_total_attempts_per_combo: int = 2

    headless: bool = False
    slow_mo_ms: int = 80
    page_timeout_ms: int = 25000

    # Persisted Chrome-ish profile for cookies / consent / session
    user_data_dir: str = "playwright_gflights_profile"

    # Outputs
    output_excel: str = "google_flights_summary.xlsx"
    output_debug_dir: str = "gflights_debug"

    # If True, pause on first run so you can handle cookies / consent manually
    pause_after_first_load: bool = True

    # If True, take screenshots on major failures
    save_failure_screenshots: bool = True

    # When set, adds some extra logging
    verbose: bool = True


CONFIG = Config()


# ============================================================
# MODELS
# ============================================================

@dataclass
class FlightCard:
    kind: str  # "outbound" or "return"
    price_text: Optional[str]
    price_value: Optional[int]
    depart_time_text: Optional[str]
    arrive_time_text: Optional[str]
    depart_time: Optional[dtime]
    arrive_time: Optional[dtime]
    stops_text: Optional[str]
    airline_text: Optional[str]
    raw_text: str
    index: int


@dataclass
class RoundTripResult:
    depart_date: str
    return_date: str
    trip_days: int

    total_price: Optional[int]
    total_price_text: Optional[str]

    outbound_airline: Optional[str]
    outbound_depart: Optional[str]
    outbound_arrive: Optional[str]
    outbound_stops: Optional[str]

    return_airline: Optional[str]
    return_depart: Optional[str]
    return_arrive: Optional[str]
    return_stops: Optional[str]

    notes: str
    success: bool


# ============================================================
# GLOBALS / CONSTANTS
# ============================================================

URL = "https://www.google.com/travel/flights"

PRICE_RE = re.compile(r"\$([\d,]+)")
TIME_TOKEN_RE = re.compile(
    r"\b(\d{1,2}:\d{2}\s?(?:AM|PM|am|pm))\b"
)
STOPS_RE = re.compile(
    r"\b(nonstop|1 stop|2 stops|3 stops|4 stops|5 stops)\b",
    re.IGNORECASE
)

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

DEBUG_DIR = Path(CONFIG.output_debug_dir)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# UTILITIES
# ============================================================

def log(*args):
    if CONFIG.verbose:
        print(*args, flush=True)


def slugify(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def save_text(path: Path, content: str):
    path.write_text(content, encoding="utf-8")


def parse_price(text: str) -> Optional[int]:
    if not text:
        return None
    m = PRICE_RE.search(text)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def parse_time_token(token: str) -> Optional[dtime]:
    if not token:
        return None
    token = token.strip().upper().replace("  ", " ")
    for fmt in ("%I:%M %p", "%I:%M%p"):
        try:
            return datetime.strptime(token, fmt).time()
        except ValueError:
            pass
    return None


def time_ok_for_departure(t: Optional[dtime]) -> bool:
    if t is None:
        return False
    if CONFIG.earliest_departure_time is None:
        return True
    return t >= CONFIG.earliest_departure_time


def arrival_is_valid(depart_t: Optional[dtime], arrive_t: Optional[dtime]) -> bool:
    if depart_t is None or arrive_t is None:
        return False
    if arrive_t < depart_t:
        return False
    if CONFIG.latest_arrival_time is None:
        return True
    return arrive_t <= CONFIG.latest_arrival_time


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def fmt_month_day_year(dt: date) -> str:
    return f"{MONTH_NAMES[dt.month]} {dt.day}, {dt.year}"


def fmt_month_short_day(dt: date) -> str:
    return dt.strftime("%b %d")


def safe_inner_text(locator: Locator) -> str:
    try:
        return locator.inner_text(timeout=4000).strip()
    except Exception:
        return ""


def first_visible(locator: Locator, max_count: int = 20) -> Optional[Locator]:
    try:
        count = min(locator.count(), max_count)
    except Exception:
        return None

    for i in range(count):
        try:
            cand = locator.nth(i)
            if cand.is_visible(timeout=1500):
                return cand
        except Exception:
            continue
    return None


def try_click(locator: Locator, timeout_ms: int = 6000) -> bool:
    try:
        locator.click(timeout=timeout_ms)
        return True
    except Exception:
        try:
            locator.first.click(timeout=timeout_ms)
            return True
        except Exception:
            try:
                locator.evaluate("(el) => el.click()")
                return True
            except Exception:
                return False


def wait_for_settle(page: Page, ms: int = 1200):
    page.wait_for_timeout(ms)


def screenshot(page: Page, name: str):
    if not CONFIG.save_failure_screenshots:
        return
    path = DEBUG_DIR / f"{now_stamp()}_{slugify(name)}.png"
    try:
        page.screenshot(path=str(path), full_page=True)
        log(f"[debug] screenshot saved: {path}")
    except Exception:
        pass


def dump_html(page: Page, name: str):
    path = DEBUG_DIR / f"{now_stamp()}_{slugify(name)}.html"
    try:
        save_text(path, page.content())
        log(f"[debug] html saved: {path}")
    except Exception:
        pass


# ============================================================
# PLAYWRIGHT HELPERS
# ============================================================

def install_common_handlers(page: Page):
    page.set_default_timeout(CONFIG.page_timeout_ms)

    # Occasionally popups/overlays appear; try to dismiss them opportunistically.
    # This is intentionally broad and safe-ish.
    dismiss_texts = [
        "Accept all",
        "I agree",
        "Got it",
        "No thanks",
        "Dismiss",
        "Close",
        "Not now",
    ]

    def try_dismiss():
        for txt in dismiss_texts:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(txt)}$", re.I))
                if btn.count() > 0:
                    if try_click(btn.first, 1200):
                        page.wait_for_timeout(300)
            except Exception:
                pass

    try_dismiss()
    page.on("framenavigated", lambda frame: None)


def open_google_flights(page: Page):
    log("[step] opening Google Flights")
    page.goto(URL, wait_until="domcontentloaded")
    wait_for_settle(page, 2500)

    # Occasionally "unsupported" interstitial appears in certain browser configs.
    # Try to click "Proceed anyway" if present.
    try:
        btn = page.get_by_role("button", name=re.compile(r"Proceed anyway", re.I))
        if btn.count() > 0:
            try_click(btn.first, 3000)
            wait_for_settle(page, 2000)
    except Exception:
        pass

    try:
        link = page.get_by_text(re.compile(r"Proceed anyway", re.I))
        if link.count() > 0:
            try_click(link.first, 3000)
            wait_for_settle(page, 2000)
    except Exception:
        pass

    screenshot(page, "after_open")

    if CONFIG.pause_after_first_load:
        log(
            "\n[manual step] If Google shows consent/cookie/account prompts, "
            "handle them now in the browser.\n"
            "Then press ENTER here to continue..."
        )
        input()
        CONFIG.pause_after_first_load = False


def find_where_from_button(page: Page) -> Optional[Locator]:
    candidates = [
        page.get_by_label(re.compile(r"Where from", re.I)),
        page.get_by_role("button", name=re.compile(r"Where from", re.I)),
        page.get_by_text(re.compile(r"Where from", re.I)),
    ]
    for c in candidates:
        vis = first_visible(c)
        if vis is not None:
            return vis
    return None


def find_where_to_button(page: Page) -> Optional[Locator]:
    candidates = [
        page.get_by_label(re.compile(r"Where to", re.I)),
        page.get_by_role("button", name=re.compile(r"Where to", re.I)),
        page.get_by_text(re.compile(r"Where to", re.I)),
    ]
    for c in candidates:
        vis = first_visible(c)
        if vis is not None:
            return vis
    return None


def clear_active_textbox(page: Page):
    page.keyboard.press("Control+A")
    page.keyboard.press("Backspace")


def choose_airport_from_autosuggest(page: Page, code_or_city: str) -> bool:
    # Wait for suggestion listbox or generic options.
    wait_for_settle(page, 900)

    option_patterns = [
        re.compile(rf"\b{re.escape(code_or_city)}\b", re.I),
    ]

    # Best shot: listbox option containing airport code.
    try:
        options = page.get_by_role("option")
        count = min(options.count(), 20)
        for i in range(count):
            opt = options.nth(i)
            txt = safe_inner_text(opt)
            if any(p.search(txt) for p in option_patterns):
                if try_click(opt, 3000):
                    return True
    except Exception:
        pass

    # Fallback: generic buttons/list items
    generic = page.locator("li, [role='option'], [role='button'], div")
    try:
        count = min(generic.count(), 60)
        for i in range(count):
            el = generic.nth(i)
            txt = safe_inner_text(el)
            if any(p.search(txt) for p in option_patterns):
                if try_click(el, 1500):
                    return True
    except Exception:
        pass

    # Final fallback: press Enter and hope first suggestion is correct.
    try:
        page.keyboard.press("Enter")
        return True
    except Exception:
        return False


def set_origin_destination(page: Page, origin: str, destination: str):
    log(f"[step] setting route {origin} -> {destination}")

    from_btn = find_where_from_button(page)
    if from_btn is None:
        screenshot(page, "where_from_not_found")
        dump_html(page, "where_from_not_found")
        raise RuntimeError("Could not find 'Where from' control.")

    if not try_click(from_btn, 5000):
        raise RuntimeError("Could not click 'Where from' control.")

    wait_for_settle(page, 700)
    clear_active_textbox(page)
    page.keyboard.type(origin, delay=50)
    if not choose_airport_from_autosuggest(page, origin):
        raise RuntimeError(f"Could not choose origin suggestion for {origin!r}.")

    wait_for_settle(page, 800)

    to_btn = find_where_to_button(page)
    if to_btn is None:
        screenshot(page, "where_to_not_found")
        dump_html(page, "where_to_not_found")
        raise RuntimeError("Could not find 'Where to' control.")

    if not try_click(to_btn, 5000):
        raise RuntimeError("Could not click 'Where to' control.")

    wait_for_settle(page, 700)
    clear_active_textbox(page)
    page.keyboard.type(destination, delay=50)
    if not choose_airport_from_autosuggest(page, destination):
        raise RuntimeError(f"Could not choose destination suggestion for {destination!r}.")

    wait_for_settle(page, 1200)
    screenshot(page, f"route_set_{origin}_{destination}")


def ensure_trip_mode(page: Page, round_trip: bool = True):
    target_label = "Round trip" if round_trip else "One way"
    log(f"[step] ensuring trip mode: {target_label}")

    # If target mode is already clearly visible, assume it's set.
    try:
        current = first_visible(page.get_by_text(re.compile(rf"^{re.escape(target_label)}$", re.I)))
        if current is not None:
            return
    except Exception:
        pass

    opener_candidates = [
        page.get_by_role("button", name=re.compile(r"Trip type", re.I)),
        page.get_by_role("button", name=re.compile(r"Round trip|One way", re.I)),
        page.get_by_text(re.compile(r"Round trip|One way", re.I)),
    ]

    opened = False
    for cand in opener_candidates:
        vis = first_visible(cand)
        if vis and try_click(vis, 2000):
            wait_for_settle(page, 500)
            opened = True
            break

    if not opened:
        return

    choice_candidates = [
        page.get_by_role("option", name=re.compile(rf"^{re.escape(target_label)}$", re.I)),
        page.get_by_role("menuitem", name=re.compile(rf"^{re.escape(target_label)}$", re.I)),
        page.get_by_text(re.compile(rf"^{re.escape(target_label)}$", re.I)),
    ]
    for choice in choice_candidates:
        vis = first_visible(choice)
        if vis and try_click(vis, 2000):
            wait_for_settle(page, 800)
            return


def set_passengers(page: Page, adults: int = 1):
    adults = max(1, int(adults))
    log(f"[step] setting passengers (adults={adults})")

    opener_candidates = [
        page.get_by_role("button", name=re.compile(r"Passengers|Adults|Traveler", re.I)),
        page.get_by_text(re.compile(r"Passengers|Adults|Traveler", re.I)),
    ]

    opened = False
    for cand in opener_candidates:
        vis = first_visible(cand)
        if vis and try_click(vis, 2500):
            wait_for_settle(page, 600)
            opened = True
            break

    if not opened:
        log("[warn] passenger selector could not be opened")
        return

    body_text = safe_inner_text(page.locator("body"))
    current = None
    m = re.search(r"Adults?\D+(\d+)", body_text, re.I)
    if m:
        current = int(m.group(1))
    else:
        m = re.search(r"\b(\d+)\s+adults?\b", body_text, re.I)
        if m:
            current = int(m.group(1))

    if current is None:
        log("[warn] could not confidently detect current adult count; leaving unchanged")
        close_date_picker(page)
        return

    inc_candidates = [
        page.get_by_role("button", name=re.compile(r"(Increase|Add).*(Adult|Passenger|Traveler)", re.I)),
        page.get_by_role("button", name=re.compile(r"plus", re.I)),
    ]
    dec_candidates = [
        page.get_by_role("button", name=re.compile(r"(Decrease|Remove).*(Adult|Passenger|Traveler)", re.I)),
        page.get_by_role("button", name=re.compile(r"minus", re.I)),
    ]

    while current < adults:
        btn = None
        for cand in inc_candidates:
            vis = first_visible(cand)
            if vis is not None:
                btn = vis
                break
        if btn is None or not try_click(btn, 1500):
            log("[warn] could not increase passengers to requested value")
            break
        current += 1
        wait_for_settle(page, 250)

    while current > adults:
        btn = None
        for cand in dec_candidates:
            vis = first_visible(cand)
            if vis is not None:
                btn = vis
                break
        if btn is None or not try_click(btn, 1500):
            log("[warn] could not decrease passengers to requested value")
            break
        current -= 1
        wait_for_settle(page, 250)

    # Best-effort close.
    for label in ["Done", "Apply", "OK"]:
        try:
            btn = page.get_by_role("button", name=re.compile(rf"^{label}$", re.I))
            vis = first_visible(btn)
            if vis and try_click(vis, 1200):
                wait_for_settle(page, 400)
                return
        except Exception:
            pass
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


def open_date_picker(page: Page):
    # Try a handful of likely date controls.
    candidates = [
        page.get_by_label(re.compile(r"Departure", re.I)),
        page.get_by_role("button", name=re.compile(r"Departure", re.I)),
        page.get_by_role("button", name=re.compile(r"Dates", re.I)),
        page.get_by_text(re.compile(r"Departure", re.I)),
    ]

    for c in candidates:
        vis = first_visible(c)
        if vis and try_click(vis, 4000):
            wait_for_settle(page, 800)
            return

    # Generic fallback: click any button containing month text or date-ish text
    generic = page.locator("button")
    try:
        count = min(generic.count(), 40)
        for i in range(count):
            btn = generic.nth(i)
            txt = safe_inner_text(btn)
            if re.search(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b", txt):
                if try_click(btn, 2000):
                    wait_for_settle(page, 800)
                    return
    except Exception:
        pass

    raise RuntimeError("Could not open date picker.")


def click_calendar_day(page: Page, dt: date) -> bool:
    human = fmt_month_day_year(dt)

    # Most robust guess: the calendar cell/button has accessible name with full date.
    candidate_patterns = [
        re.compile(re.escape(human), re.I),
        re.compile(rf"{MONTH_NAMES[dt.month]}\s+{dt.day},\s+{dt.year}", re.I),
        re.compile(rf"\b{dt.day}\b"),
    ]

    # Try role button or gridcell first
    locators = [
        page.get_by_role("button", name=candidate_patterns[0]),
        page.get_by_role("gridcell", name=candidate_patterns[0]),
        page.get_by_label(candidate_patterns[0]),
        page.get_by_text(candidate_patterns[0]),
    ]

    for loc in locators:
        vis = first_visible(loc, max_count=10)
        if vis and try_click(vis, 1800):
            return True

    # Fallback: search visible calendar region for matching text
    calendar = page.locator("[role='dialog'], [role='grid'], div")
    try:
        count = min(calendar.count(), 200)
        for i in range(count):
            el = calendar.nth(i)
            txt = safe_inner_text(el)
            if any(p.search(txt) for p in candidate_patterns[:2]):
                if try_click(el, 1200):
                    return True
    except Exception:
        pass

    return False


def go_to_month_in_date_picker(page: Page, dt: date, max_jumps: int = 8) -> bool:
    target_month = MONTH_NAMES[dt.month]
    target_year = str(dt.year)

    for _ in range(max_jumps):
        # If target month label visible, stop.
        visible_text = safe_inner_text(page.locator("body"))
        if target_month in visible_text and target_year in visible_text:
            return True

        # Try next month buttons.
        next_candidates = [
            page.get_by_role("button", name=re.compile(r"Next month", re.I)),
            page.get_by_role("button", name=re.compile(r"Next", re.I)),
            page.get_by_label(re.compile(r"Next month", re.I)),
        ]
        moved = False
        for nc in next_candidates:
            vis = first_visible(nc)
            if vis and try_click(vis, 1500):
                wait_for_settle(page, 450)
                moved = True
                break
        if not moved:
            break

    # Even if exact month label wasn't detected, we may still be close enough.
    return True


def close_date_picker(page: Page):
    # Common close methods.
    for _ in range(2):
        try:
            page.keyboard.press("Escape")
            wait_for_settle(page, 300)
        except Exception:
            pass

    # If an explicit Done button exists, click it.
    for label in ["Done", "Apply", "OK"]:
        try:
            btn = page.get_by_role("button", name=re.compile(rf"^{label}$", re.I))
            if btn.count() > 0 and try_click(btn.first, 1000):
                wait_for_settle(page, 500)
                return
        except Exception:
            pass


def set_dates(page: Page, depart: date, ret: date):
    log(f"[step] setting dates {depart.isoformat()} -> {ret.isoformat()}")
    open_date_picker(page)

    # Navigate toward departure month
    go_to_month_in_date_picker(page, depart)
    if not click_calendar_day(page, depart):
        screenshot(page, f"depart_click_failed_{depart}")
        dump_html(page, f"depart_click_failed_{depart}")
        raise RuntimeError(f"Could not click departure date {depart}.")

    wait_for_settle(page, 500)

    if CONFIG.round_trip:
        # Navigate toward return month
        go_to_month_in_date_picker(page, ret)
        if not click_calendar_day(page, ret):
            screenshot(page, f"return_click_failed_{ret}")
            dump_html(page, f"return_click_failed_{ret}")
            raise RuntimeError(f"Could not click return date {ret}.")

        wait_for_settle(page, 600)
    close_date_picker(page)
    wait_for_settle(page, 1200)


def click_search_or_refresh(page: Page):
    # Google Flights often refreshes automatically; still, if a Search/Done button exists, use it.
    for pat in [r"Search", r"Done", r"Explore"]:
        try:
            btn = page.get_by_role("button", name=re.compile(pat, re.I))
            vis = first_visible(btn)
            if vis and try_click(vis, 1500):
                wait_for_settle(page, 2000)
                return
        except Exception:
            pass

    # Fallback: short wait for auto-refresh
    wait_for_settle(page, 2500)


def click_cheapest_tab_if_present(page: Page):
    # If "Cheapest" tab/chip exists, click it so first cards are closer to your objective.
    for pat in [r"Cheapest", r"Price"]:
        try:
            btn = page.get_by_role("button", name=re.compile(rf"^{pat}$", re.I))
            vis = first_visible(btn)
            if vis:
                try_click(vis, 1500)
                wait_for_settle(page, 1000)
                return
        except Exception:
            pass

    try:
        txt = page.get_by_text(re.compile(r"Cheapest", re.I))
        vis = first_visible(txt)
        if vis:
            try_click(vis, 1200)
            wait_for_settle(page, 1000)
    except Exception:
        pass


def get_main_cards(page: Page) -> List[Locator]:
    """
    Best-effort collector for visible flight result cards.
    This intentionally tries several generic patterns.
    """
    pools = [
        page.locator("[role='main'] [role='listitem']"),
        page.locator("[role='main'] li"),
        page.locator("[role='main'] button"),
        page.locator("main [role='listitem']"),
        page.locator("main li"),
        page.locator("main button"),
    ]

    cards: List[Locator] = []

    for pool in pools:
        try:
            count = min(pool.count(), 200)
        except Exception:
            continue

        for i in range(count):
            item = pool.nth(i)
            txt = safe_inner_text(item)
            if not txt:
                continue
            if "$" not in txt:
                continue
            if len(txt) < 20:
                continue
            cards.append(item)

        if cards:
            break

    # Deduplicate by text
    unique: List[Locator] = []
    seen = set()
    for c in cards:
        txt = safe_inner_text(c)
        if txt not in seen:
            seen.add(txt)
            unique.append(c)

    return unique


def parse_flight_card_text(kind: str, raw_text: str, index: int) -> FlightCard:
    price_text = None
    price_value = None
    depart_text = None
    arrive_text = None
    depart_t = None
    arrive_t = None
    stops_text = None
    airline_text = None

    pm = PRICE_RE.search(raw_text)
    if pm:
        price_text = f"${pm.group(1)}"
        price_value = int(pm.group(1).replace(",", ""))

    times = TIME_TOKEN_RE.findall(raw_text)
    if len(times) >= 2:
        depart_text = times[0].upper().replace("  ", " ")
        arrive_text = times[1].upper().replace("  ", " ")
        depart_t = parse_time_token(depart_text)
        arrive_t = parse_time_token(arrive_text)

    sm = STOPS_RE.search(raw_text)
    if sm:
        stops_text = sm.group(1)

    # Crude airline extraction:
    # take first non-empty line that isn't obviously price/time/stops/duration.
    lines = [x.strip() for x in raw_text.splitlines() if x.strip()]
    for line in lines[:8]:
        if PRICE_RE.search(line):
            continue
        if TIME_TOKEN_RE.search(line):
            continue
        if STOPS_RE.search(line):
            continue
        if re.search(r"\bhr\b|\bmin\b", line, re.I):
            continue
        if len(line) > 2:
            airline_text = line
            break

    return FlightCard(
        kind=kind,
        price_text=price_text,
        price_value=price_value,
        depart_time_text=depart_text,
        arrive_time_text=arrive_text,
        depart_time=depart_t,
        arrive_time=arrive_t,
        stops_text=stops_text,
        airline_text=airline_text,
        raw_text=raw_text,
        index=index,
    )


def collect_flight_cards(page: Page, kind: str, limit: int) -> List[FlightCard]:
    click_cheapest_tab_if_present(page)
    wait_for_settle(page, 1200)

    raw_cards = get_main_cards(page)
    parsed: List[FlightCard] = []

    for idx, loc in enumerate(raw_cards[:limit * 3]):
        txt = safe_inner_text(loc)
        if not txt:
            continue
        card = parse_flight_card_text(kind, txt, idx)
        if card.price_value is None:
            continue
        parsed.append(card)

    # Sort by price ascending, then keep earliest items
    parsed.sort(key=lambda x: (x.price_value if x.price_value is not None else math.inf, x.index))
    return parsed[:limit]


def outbound_card_allowed(card: FlightCard) -> bool:
    return (
        time_ok_for_departure(card.depart_time)
        and arrival_is_valid(card.depart_time, card.arrive_time)
        and airline_allowed(card)
    )


def return_card_allowed(card: FlightCard) -> bool:
    return (
        time_ok_for_departure(card.depart_time)
        and arrival_is_valid(card.depart_time, card.arrive_time)
        and airline_allowed(card)
    )


def airline_allowed(card: FlightCard) -> bool:
    if not CONFIG.allowed_airlines:
        return True
    if not card.airline_text:
        return False
    hay = card.airline_text.lower()
    return any(needle.lower() in hay for needle in CONFIG.allowed_airlines)

def click_card_by_text(page: Page, raw_text: str) -> bool:
    """
    Re-find a card using a stable slice of its text.
    """
    lines = [x.strip() for x in raw_text.splitlines() if x.strip()]
    anchors = []

    # Prefer price + first time + maybe airline-ish line
    price_match = PRICE_RE.search(raw_text)
    if price_match:
        anchors.append(f"${price_match.group(1)}")

    times = TIME_TOKEN_RE.findall(raw_text)
    if times:
        anchors.append(times[0])

    for line in lines[:5]:
        if len(line) >= 4 and "$" not in line:
            anchors.append(line[:40])
            break

    candidates = page.locator("[role='main'] button, [role='main'] li, main button, main li")
    try:
        count = min(candidates.count(), 250)
    except Exception:
        count = 0

    for i in range(count):
        el = candidates.nth(i)
        txt = safe_inner_text(el)
        if not txt:
            continue
        hits = 0
        for a in anchors:
            if a and a in txt:
                hits += 1
        if hits >= max(1, min(2, len(anchors))):
            if try_click(el, 2000):
                wait_for_settle(page, 1800)
                return True

    return False


def extract_total_price_from_page(page: Page) -> Tuple[Optional[int], Optional[str]]:
    body = safe_inner_text(page.locator("body"))

    # Try more specific labels first
    specific_patterns = [
        re.compile(r"Total[^$]{0,40}\$([\d,]+)", re.I),
        re.compile(r"Price[^$]{0,40}\$([\d,]+)", re.I),
    ]
    for pat in specific_patterns:
        m = pat.search(body)
        if m:
            return int(m.group(1).replace(",", "")), f"${m.group(1)}"

    # Fallback: just first visible price on page after full selection
    m = PRICE_RE.search(body)
    if m:
        return int(m.group(1).replace(",", "")), f"${m.group(1)}"

    return None, None


def back_one(page: Page):
    try:
        page.go_back(wait_until="domcontentloaded")
        wait_for_settle(page, 1800)
    except Exception:
        try:
            page.keyboard.press("Alt+Left")
            wait_for_settle(page, 1800)
        except Exception:
            pass


def search_combo_and_pick_best(page: Page, depart: date, ret: date) -> RoundTripResult:
    set_dates(page, depart, ret)
    click_search_or_refresh(page)

    # Phase 1: outbound results
    log("[step] collecting outbound cards")
    outbound_cards = collect_flight_cards(page, "outbound", CONFIG.max_outbound_cards_to_try)

    if not outbound_cards:
        screenshot(page, f"no_outbound_cards_{depart}_{ret}")
        dump_html(page, f"no_outbound_cards_{depart}_{ret}")
        return RoundTripResult(
            depart_date=depart.isoformat(),
            return_date=ret.isoformat(),
            trip_days=(ret - depart).days,
            total_price=None,
            total_price_text=None,
            outbound_airline=None,
            outbound_depart=None,
            outbound_arrive=None,
            outbound_stops=None,
            return_airline=None,
            return_depart=None,
            return_arrive=None,
            return_stops=None,
            notes="No outbound cards found",
            success=False,
        )

    outbound_cards = [c for c in outbound_cards if outbound_card_allowed(c)]
    if not outbound_cards:
        return RoundTripResult(
            depart_date=depart.isoformat(),
            return_date=ret.isoformat(),
            trip_days=(ret - depart).days,
            total_price=None,
            total_price_text=None,
            outbound_airline=None,
            outbound_depart=None,
            outbound_arrive=None,
            outbound_stops=None,
            return_airline=None,
            return_depart=None,
            return_arrive=None,
            return_stops=None,
            notes="No outbound cards met time filters",
            success=False,
        )

    best_result: Optional[RoundTripResult] = None

    # Try a few outbound candidates in ascending price order
    for ob in outbound_cards[:CONFIG.max_outbound_cards_to_try]:
        log(
            f"[try outbound] {ob.price_text} | {ob.depart_time_text} -> {ob.arrive_time_text} | {ob.airline_text}"
        )

        if not click_card_by_text(page, ob.raw_text):
            continue

        wait_for_settle(page, 2200)

        # Phase 2: return results
        return_cards = collect_flight_cards(page, "return", CONFIG.max_return_cards_to_try)
        return_cards = [c for c in return_cards if return_card_allowed(c)]

        if not return_cards:
            back_one(page)
            continue

        # Try cheapest valid return first
        for rb in return_cards[:CONFIG.max_return_cards_to_try]:
            log(
                f"[try return] {rb.price_text} | {rb.depart_time_text} -> {rb.arrive_time_text} | {rb.airline_text}"
            )
            if not click_card_by_text(page, rb.raw_text):
                continue

            wait_for_settle(page, 2500)
            total_value, total_text = extract_total_price_from_page(page)

            current = RoundTripResult(
                depart_date=depart.isoformat(),
                return_date=ret.isoformat(),
                trip_days=(ret - depart).days,
                total_price=total_value,
                total_price_text=total_text,
                outbound_airline=ob.airline_text,
                outbound_depart=ob.depart_time_text,
                outbound_arrive=ob.arrive_time_text,
                outbound_stops=ob.stops_text,
                return_airline=rb.airline_text,
                return_depart=rb.depart_time_text,
                return_arrive=rb.arrive_time_text,
                return_stops=rb.stops_text,
                notes="OK" if total_value is not None else "Selected flights but total not found",
                success=total_value is not None,
            )

            if current.success:
                if best_result is None or (
                    current.total_price is not None
                    and best_result.total_price is not None
                    and current.total_price < best_result.total_price
                ):
                    best_result = current

            # After selecting return, go back to return choices for another candidate.
            back_one(page)
            wait_for_settle(page, 1200)

            # If we got one success, that's often enough for a first pass.
            if best_result is not None:
                break

        # Go back to outbound result list
        back_one(page)
        wait_for_settle(page, 1800)

        if best_result is not None:
            break

    if best_result is not None:
        return best_result

    return RoundTripResult(
        depart_date=depart.isoformat(),
        return_date=ret.isoformat(),
        trip_days=(ret - depart).days,
        total_price=None,
        total_price_text=None,
        outbound_airline=None,
        outbound_depart=None,
        outbound_arrive=None,
        outbound_stops=None,
        return_airline=None,
        return_depart=None,
        return_arrive=None,
        return_stops=None,
        notes="No valid round-trip combination found for this combo",
        success=False,
    )


def search_one_way_best(page: Page, depart: date) -> RoundTripResult:
    set_dates(page, depart, depart)
    click_search_or_refresh(page)

    log("[step] collecting one-way outbound cards")
    outbound_cards = collect_flight_cards(page, "outbound", CONFIG.max_outbound_cards_to_try)
    outbound_cards = [c for c in outbound_cards if outbound_card_allowed(c)]

    if not outbound_cards:
        return RoundTripResult(
            depart_date=depart.isoformat(),
            return_date="",
            trip_days=0,
            total_price=None,
            total_price_text=None,
            outbound_airline=None,
            outbound_depart=None,
            outbound_arrive=None,
            outbound_stops=None,
            return_airline=None,
            return_depart=None,
            return_arrive=None,
            return_stops=None,
            notes="No valid one-way outbound cards found",
            success=False,
        )

    best = outbound_cards[0]
    return RoundTripResult(
        depart_date=depart.isoformat(),
        return_date="",
        trip_days=0,
        total_price=best.price_value,
        total_price_text=best.price_text,
        outbound_airline=best.airline_text,
        outbound_depart=best.depart_time_text,
        outbound_arrive=best.arrive_time_text,
        outbound_stops=best.stops_text,
        return_airline=None,
        return_depart=None,
        return_arrive=None,
        return_stops=None,
        notes="OK",
        success=best.price_value is not None,
    )


def summarize_best_by_depart_date(results: List[RoundTripResult]) -> pd.DataFrame:
    df = pd.DataFrame([asdict(r) for r in results])

    if df.empty:
        return df

    # For each departure date, keep cheapest successful row; if no success exists, keep first failure row.
    final_rows = []
    for depart_date, g in df.groupby("depart_date", sort=True):
        ok = g[g["success"] == True].copy()
        if not ok.empty:
            ok = ok.sort_values(["total_price", "trip_days"], ascending=[True, True], na_position="last")
            final_rows.append(ok.iloc[0])
        else:
            final_rows.append(g.iloc[0])

    out = pd.DataFrame(final_rows).sort_values("depart_date").reset_index(drop=True)
    return out


def write_excel(all_results: List[RoundTripResult], best_results_df: pd.DataFrame):
    raw_df = pd.DataFrame([asdict(r) for r in all_results])

    with pd.ExcelWriter(CONFIG.output_excel, engine="openpyxl") as writer:
        best_results_df.to_excel(writer, sheet_name="Best by Depart Date", index=False)
        raw_df.to_excel(writer, sheet_name="All Combos Tried", index=False)

        params_df = pd.DataFrame(
            [{"parameter": k, "value": str(v)} for k, v in asdict(CONFIG).items()]
        )
        params_df.to_excel(writer, sheet_name="Run Config", index=False)

    log(f"[done] Excel written: {CONFIG.output_excel}")

def append_to_excel(result):
    df = pd.DataFrame([asdict(result)])

    with pd.ExcelWriter(
        CONFIG.output_excel,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="overlay"
    ) as writer:

        sheet = writer.book.active
        start_row = sheet.max_row

        df.to_excel(writer, index=False, header=False, startrow=start_row)


# ============================================================
# MAIN
# ============================================================

def main():
    
    import os

    if os.path.exists(CONFIG.output_excel):
        os.remove(CONFIG.output_excel)

    columns = [
        "depart_date", "return_date", "trip_days",
        "total_price", "total_price_text",
        "outbound_airline", "outbound_depart", "outbound_arrive", "outbound_stops",
        "return_airline", "return_depart", "return_arrive", "return_stops",
        "notes", "success"
    ]

    pd.DataFrame(columns=columns).to_excel(CONFIG.output_excel, index=False)
    
    all_results: List[RoundTripResult] = []

    user_dir = Path(CONFIG.user_data_dir)
    ensure_dir(user_dir)
    ensure_dir(DEBUG_DIR)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(user_dir),
            headless=CONFIG.headless,
            slow_mo=CONFIG.slow_mo_ms,
            viewport={"width": 1440, "height": 1100},
            args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        page = context.pages[0] if context.pages else context.new_page()
        install_common_handlers(page)

        try:
            open_google_flights(page)
            ensure_trip_mode(page, CONFIG.round_trip)
            set_origin_destination(page, CONFIG.origin, CONFIG.destination)
            set_passengers(page, CONFIG.passengers)

            # Main sweep
            for depart in daterange(CONFIG.start_date, CONFIG.end_date):
                best_for_depart: Optional[RoundTripResult] = None

                if not CONFIG.round_trip:
                    combo_result = None
                    for attempt in range(1, CONFIG.max_total_attempts_per_combo + 1):
                        log(
                            f"\n=== one-way {depart.isoformat()} "
                            f"(attempt {attempt}/{CONFIG.max_total_attempts_per_combo}) ==="
                        )
                        try:
                            combo_result = search_one_way_best(page, depart)
                            break
                        except PlaywrightTimeoutError:
                            log("[warn] Playwright timeout; retrying one-way search")
                            screenshot(page, f"timeout_oneway_{depart}_attempt{attempt}")
                            dump_html(page, f"timeout_oneway_{depart}_attempt{attempt}")
                            try:
                                page.goto(URL, wait_until="domcontentloaded")
                                wait_for_settle(page, 2500)
                                ensure_trip_mode(page, CONFIG.round_trip)
                                set_origin_destination(page, CONFIG.origin, CONFIG.destination)
                                set_passengers(page, CONFIG.passengers)
                            except Exception:
                                pass
                        except Exception as e:
                            log(f"[warn] one-way combo failed: {e}")
                            screenshot(page, f"combo_error_oneway_{depart}_attempt{attempt}")
                            dump_html(page, f"combo_error_oneway_{depart}_attempt{attempt}")
                            try:
                                page.goto(URL, wait_until="domcontentloaded")
                                wait_for_settle(page, 2500)
                                ensure_trip_mode(page, CONFIG.round_trip)
                                set_origin_destination(page, CONFIG.origin, CONFIG.destination)
                                set_passengers(page, CONFIG.passengers)
                            except Exception:
                                pass

                    if combo_result is None:
                        combo_result = RoundTripResult(
                            depart_date=depart.isoformat(),
                            return_date="",
                            trip_days=0,
                            total_price=None,
                            total_price_text=None,
                            outbound_airline=None,
                            outbound_depart=None,
                            outbound_arrive=None,
                            outbound_stops=None,
                            return_airline=None,
                            return_depart=None,
                            return_arrive=None,
                            return_stops=None,
                            notes="One-way combo failed after retries",
                            success=False,
                        )

                    all_results.append(combo_result)
                    append_to_excel(combo_result)
                    if combo_result.success:
                        best_for_depart = combo_result
                else:
                    for trip_days in range(CONFIG.min_trip_days, CONFIG.max_trip_days + 1):
                        ret = depart + timedelta(days=trip_days)

                        # Skip combos where return goes absurdly beyond desired horizon only if you want.
                        # Here we allow it.
                        combo_result = None
                        for attempt in range(1, CONFIG.max_total_attempts_per_combo + 1):
                            log(
                                f"\n=== {depart.isoformat()} + {trip_days}d "
                                f"(attempt {attempt}/{CONFIG.max_total_attempts_per_combo}) ==="
                            )
                            try:
                                combo_result = search_combo_and_pick_best(page, depart, ret)
                                break
                            except PlaywrightTimeoutError:
                                log("[warn] Playwright timeout; retrying combo")
                                screenshot(page, f"timeout_{depart}_{ret}_attempt{attempt}")
                                dump_html(page, f"timeout_{depart}_{ret}_attempt{attempt}")
                                try:
                                    page.goto(URL, wait_until="domcontentloaded")
                                    wait_for_settle(page, 2500)
                                    ensure_trip_mode(page, CONFIG.round_trip)
                                    set_origin_destination(page, CONFIG.origin, CONFIG.destination)
                                    set_passengers(page, CONFIG.passengers)
                                except Exception:
                                    pass
                            except Exception as e:
                                log(f"[warn] combo failed: {e}")
                                screenshot(page, f"combo_error_{depart}_{ret}_attempt{attempt}")
                                dump_html(page, f"combo_error_{depart}_{ret}_attempt{attempt}")
                                try:
                                    page.goto(URL, wait_until="domcontentloaded")
                                    wait_for_settle(page, 2500)
                                    ensure_trip_mode(page, CONFIG.round_trip)
                                    set_origin_destination(page, CONFIG.origin, CONFIG.destination)
                                    set_passengers(page, CONFIG.passengers)
                                except Exception:
                                    pass

                        if combo_result is None:
                            combo_result = RoundTripResult(
                                depart_date=depart.isoformat(),
                                return_date=ret.isoformat(),
                                trip_days=trip_days,
                                total_price=None,
                                total_price_text=None,
                                outbound_airline=None,
                                outbound_depart=None,
                                outbound_arrive=None,
                                outbound_stops=None,
                                return_airline=None,
                                return_depart=None,
                                return_arrive=None,
                                return_stops=None,
                                notes="Combo failed after retries",
                                success=False,
                            )

                        all_results.append(combo_result)
                        append_to_excel(combo_result)

                        if combo_result.success:
                            if (
                                best_for_depart is None
                                or (
                                    combo_result.total_price is not None
                                    and best_for_depart.total_price is not None
                                    and combo_result.total_price < best_for_depart.total_price
                                )
                            ):
                                best_for_depart = combo_result

                if best_for_depart:
                    log(
                        f"[best for {depart.isoformat()}] "
                        f"{best_for_depart.total_price_text} "
                        f"return {best_for_depart.return_date}"
                    )
                else:
                    log(f"[best for {depart.isoformat()}] no valid result")

        finally:
            try:
                context.close()
            except Exception:
                pass

    best_df = summarize_best_by_depart_date(all_results)
    write_excel(all_results, best_df)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
        sys.exit(1)
    except Exception as exc:
        print("\nFATAL ERROR")
        print(exc)
        traceback.print_exc()
        sys.exit(2)
