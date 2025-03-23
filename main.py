import requests
import time
import random
import os
import json
import logging
import asyncio
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
from rapidfuzz import fuzz, utils
from tenacity import retry, stop_after_attempt, wait_exponential
import asyncpg
from datetime import datetime, timezone, timedelta, time as dt_time
from selectolax.parser import HTMLParser
from pythonjsonlogger import jsonlogger
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
import signal
import sys
import socket
from logging.handlers import RotatingFileHandler
import pytz
import gc
from dateutil import parser as date_parser
import re
from dotenv import load_dotenv
from flask import Flask, request, jsonify, Response
from concurrent.futures import ThreadPoolExecutor

# Load environment variables from .env file
load_dotenv()

# ----------------- Configuration -------------------
# Single port configuration for all services
PORT = int(os.getenv("PORT", 5000))        # For all services (Flask API, Metrics, Health)

# ----------------- Other Configuration -------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER") or sys.exit("Error: DB_USER not set")
DB_PASS = os.getenv("DB_PASS") or sys.exit("Error: DB_PASS not set")
DB_NAME = os.getenv("DB_NAME", "cricket_db")
CRICBUZZ_URLS = {
    "live": "https://www.cricbuzz.com/live-cricket-scores",
    "upcoming":
    "https://www.cricbuzz.com/cricket-match/live-scores/upcoming-matches",
    "recent":
    "https://www.cricbuzz.com/cricket-match/live-scores/recent-matches"
}
COMPLETED_THRESHOLD = int(os.getenv("COMPLETED_THRESHOLD", 85))
LIVE_THRESHOLD = int(os.getenv("LIVE_THRESHOLD", 75))
UPCOMING_THRESHOLD = int(os.getenv("UPCOMING_THRESHOLD", 70))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 2))
MAX_TIMEOUT = int(os.getenv("MAX_TIMEOUT", 30))
RENDER_URL = os.getenv("RENDER_URL")

# ----------------- Logging Setup -------------------
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Modified log handler to overwrite logs instead of rotating
json_handler = RotatingFileHandler('cricket_scraper.log',
                                   maxBytes=1e6,
                                   backupCount=0,  # No backups, just overwrite when full
                                   )
json_handler.setFormatter(
    jsonlogger.JsonFormatter(fmt='%(asctime)s %(levelname)s %(message)s',
                             datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(json_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    jsonlogger.JsonFormatter(fmt='%(asctime)s %(levelname)s %(message)s',
                             datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(console_handler)

# Create Flask app
app = Flask(__name__)
app.config['FLASK_APP'] = __name__
app.config['FLASK_ENV'] = 'production'

# ----------------- Flask Routes -------------------
@app.route('/')
async def home():
    render_url = os.getenv('RENDER_URL')  # Render link
    if not render_url:
        return "RENDER_URL not set! Please add it to your .env file.", 500

    logger.info(f"Checking if {render_url} is live...")

    # Add timeout for render URL checks to avoid infinite loops
    max_attempts = 10
    attempts = 0
    
    # Non-blocking check with a maximum number of attempts
    async with aiohttp.ClientSession() as session:
        while attempts < max_attempts:
            try:
                async with session.get(render_url, timeout=5) as response:  # 5 sec timeout
                    if response.status == 200:
                        logger.info(f"{render_url} is now live!")
                        return f"Render Link {render_url} is Live!"
            except Exception:
                logger.warning(f"{render_url} is not live yet, retrying... ({attempts+1}/{max_attempts})")
            
            attempts += 1
            await asyncio.sleep(3)  # Retry every 3 seconds
    
    # If we reach here, the URL wasn't live after max_attempts
    return f"Render URL {render_url} could not be reached after {max_attempts} attempts. It might not be live yet.", 503

@app.route('/check-render')
async def check_render():
    """
    Route for checking render URL status without waiting - immediate response.
    """
    render_url = os.getenv('RENDER_URL')
    if not render_url:
        return jsonify({"status": "error", "message": "RENDER_URL not set"}), 500
    
    try:
        # Use aiohttp for non-blocking HTTP requests instead of requests
        async with aiohttp.ClientSession() as session:
            async with session.get(render_url, timeout=5) as response:
                return jsonify({
                    "status": "success", 
                    "is_live": response.status == 200,
                    "status_code": response.status
                })
    except Exception as e:
        return jsonify({
            "status": "error", 
            "is_live": False,
            "message": str(e)
        }), 503

@app.route('/api/matches')
async def get_matches():
    match_type = request.args.get('type', 'all')
    
    if not hasattr(app, 'db_pool'):
        return jsonify({"error": "Database not connected"}), 500
    
    try:
        async with app.db_pool.acquire() as conn:
            if match_type.lower() == 'all':
                query = "SELECT * FROM matches ORDER BY last_updated DESC"
                rows = await conn.fetch(query)
            else:
                query = "SELECT * FROM matches WHERE match_type = $1 ORDER BY last_updated DESC"
                rows = await conn.fetch(query, match_type.capitalize())
                
            matches = [dict(row) for row in rows]
            return jsonify({"matches": matches})
    except Exception as e:
        logger.error({"message": "API error", "error": str(e)})
        return jsonify({"error": str(e)}), 500

# Health check endpoint
@app.route('/health')
async def health():
    """Simple health check endpoint"""
    return jsonify({"status": "OK", "timestamp": datetime.now().isoformat()})

# Prometheus metrics endpoint
@app.route('/metrics')
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

# ----------------- User Agents -------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
]
LANGUAGES = ["en-US", "en-IN", "hi-IN"]
PROXIES = []
ETAG_FILE = "last_etag.txt"
FALLBACK_FILE = "fallback_matches.json"

COMPLETED_KEYWORDS = [
    "match over", "won by", "lost by", "abandoned", "no result", "tied",
    "innings defeat", "completed", "drawn", "concluded", "dls method"
]
LIVE_KEYWORDS = [
    "overs", "innings", "resumes", "break", "play in progress", 
    "live", "batting", "bowling", "runs", "wicket", "crease", 
    "partnership", "powerplay", "current run rate", "req run rate",
    "innings break", "target", "required", "chasing", "ball remaining"
]
UPCOMING_KEYWORDS = [
    "starts at", "yet to begin", "scheduled", "upcoming", "tomorrow",
    "today at", "to commence"
]
TEST_KEYWORDS = [
    "day", "stumps", "lunch", "tea", "innings break", "close of play",
    "session"
]
SPECIAL_CASES = {
    r"\bno result\b": 100,
    r"\bdls method\b": 100,
    r"\btied\b": 95,
    r"\babandoned\b": 90
}

REQUESTS_TOTAL = Counter('requests_total', 'Total HTTP requests')
ERRORS_TOTAL = Counter('errors_total', 'Total errors')
DB_ERRORS = Counter('db_errors', 'Database errors')
PARSING_ERRORS = Counter('parsing_errors', 'HTML parsing errors')
CORRECT_CLASSIFICATIONS = Counter('correct_classifications',
                                  'Correct status classifications',
                                  ['match_type'])

# ----------------- Database Connection -------------------
@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=2, min=4, max=60))
async def init_db_pool():
    attempt = init_db_pool.retry.statistics.get("attempt_number", 1)
    try:
        # Create connection pool
        pool = await asyncpg.create_pool(host=DB_HOST,
                                         user=DB_USER,
                                         password=DB_PASS,
                                         database=DB_NAME,
                                         min_size=1,
                                         max_size=10,
                                         command_timeout=30)
        
        # Initialize tables
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS matches (
                    match_id TEXT PRIMARY KEY,
                    match_title TEXT NOT NULL,
                    score TEXT,
                    match_type TEXT NOT NULL,
                    series_name TEXT,
                    last_updated TIMESTAMP WITH TIME ZONE,
                    match_date DATE,
                    start_time TIME
                )
            """)
            
            # Create indexes for better query performance
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_matches_match_type 
                ON matches(match_type);
                
                CREATE INDEX IF NOT EXISTS idx_matches_last_updated 
                ON matches(last_updated);
            """)
            
        logger.info({
            "message": "Database pool initialized",
            "pid": os.getpid()
        })
        return pool
    except Exception as e:
        # Log sanitized connection details (without password)
        conn_details = {
            "host": DB_HOST,
            "user": DB_USER,
            "database": DB_NAME,
            "attempt": attempt
        }
        logger.error({
            "message": "Database connection failed",
            "error": str(e).replace(DB_PASS, "********"),  # Redact password from error
            "connection": conn_details
        })
        raise


# ----------------- Helper Functions -------------------
def get_status_score(status, keywords):
    status_lower = status.lower()
    return max([
        fuzz.partial_ratio(kwd, status_lower, processor=utils.default_process)
        for kwd in keywords
    ],
               default=0)


def get_special_score(status):
    status_lower = status.lower()
    for pattern, score in SPECIAL_CASES.items():
        if re.search(pattern, status_lower):
            return score
    return 0


def classify_status(status):
    status_lower = status.lower()

    # Direct pattern matching for more accuracy
    if any(pattern in status_lower for pattern in ["match ended", "won by", "lost by", "drawn"]):
        logger.debug({"message": "Direct match for Completed status", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Completed").inc()
        return "Completed"

    if any(pattern in status_lower for pattern in ["live", "batting", "bowling", "crease"]):
        logger.debug({"message": "Direct match for Live status", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"

    # Check for cricket scoreboard patterns

    # Standard scoreboard pattern (runs/wickets)
    if re.search(r'\d+/\d+', status_lower) or re.search(r'\d+\s*&\s*\d+/\d+', status_lower):
        logger.debug({"message": "Standard score pattern detected - Live match", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"
        
    # IPL/T20 style format with team names: "KKR150-5 (15.3 Ovs)RCB..."
    if re.search(r'\d+-\d+\s*\(\d+\.?\d*\s*ovs?\)', status_lower, re.IGNORECASE):
        logger.debug({"message": "IPL format score pattern detected - Live match", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"
        
    # Alternative IPL format with team codes and innings break: "KKR174-8 (20 Ovs)RCBInnings Break"
    if re.search(r'[a-z]{2,5}\d+-\d+', status_lower, re.IGNORECASE) or re.search(r'innings break', status_lower, re.IGNORECASE):
        logger.debug({"message": "Team code with score pattern detected - Live match", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"
        
    # Handle formats with team runs only: "MI 223 CSK 224-3"
    if re.search(r'[a-z]{2,5}\s*\d+.*[a-z]{2,5}\s*\d+', status_lower, re.IGNORECASE):
        logger.debug({"message": "Multiple team scores detected - Live match", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"
        
    # Target or chase related formats
    if re.search(r'target|chase|need|require', status_lower):
        logger.debug({"message": "Chase/target format detected - Live match", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"
    
    # DLS related scenario but match is still in progress
    if re.search(r'dls|d/l.*target|revised', status_lower) and not re.search(r'won by|lost by', status_lower):
        logger.debug({"message": "DLS active match detected - Live match", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"

    # Super Over scenario
    if re.search(r'super over|tied|scores level', status_lower) and not re.search(r'won by|lost by', status_lower):
        logger.debug({"message": "Super Over detected - Live match", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"

    # Fuzzy matching with scores for other cases
    scores = {
        "completed": get_status_score(status, COMPLETED_KEYWORDS),
        "live": get_status_score(status, LIVE_KEYWORDS),
        "upcoming": get_status_score(status, UPCOMING_KEYWORDS),
        "test": get_status_score(status, TEST_KEYWORDS),
        "special": get_special_score(status)
    }

    logger.debug({
        "message": "Status scores",
        "status": status,
        **{f"{k}_score": v for k, v in scores.items()}
    })

    if scores["special"] >= 90:
        logger.info({"message": "Special case detected", "status": status, "score": scores["special"]})
        CORRECT_CLASSIFICATIONS.labels(match_type="Completed").inc()
        return "Completed"

    # Find max score and corresponding type
    max_type = max(scores.items(), key=lambda x: x[1] if x[0] != "special" else 0)[0]
    max_score = scores[max_type]

    # Map fuzzy match type to actual status
    status_map = {
        "completed": ("Completed", COMPLETED_THRESHOLD),
        "test": ("Test", LIVE_THRESHOLD),
        "live": ("Live", LIVE_THRESHOLD),
        "upcoming": ("Upcoming", UPCOMING_THRESHOLD)
    }

    # Check if score exceeds threshold for the best match
    if max_type in status_map and max_score >= status_map[max_type][1]:
        match_type = status_map[max_type][0]
        if match_type != "Upcoming":  # Reduce log noise for common case
            logger.info({
                "message": f"{match_type} match detected",
                "status": status,
                "score": max_score
            })
        CORRECT_CLASSIFICATIONS.labels(match_type=match_type).inc()
        return match_type

    # Default case - if nothing matches well, check if it has any signs of being live
    if any(kw in status_lower for kw in ["over", "ovs", "ball", "run", "score", "inning", "wicket", "bat", "bowl"]):
        logger.info({"message": "Possible live match with low confidence", "status": status})
        CORRECT_CLASSIFICATIONS.labels(match_type="Live").inc()
        return "Live"

    # Fall back to Upcoming for anything else
    logger.info({"message": "Default to Upcoming match", "status": status, "max_score": max_score})
    CORRECT_CLASSIFICATIONS.labels(match_type="Upcoming").inc()
    return "Upcoming"


def parse_upcoming_datetime(date_text, time_text, timestamp=None):
    try:
        today = datetime.now(pytz.UTC).date()
        match_date = None
        start_time = None
        ist = pytz.timezone("Asia/Kolkata")

        if timestamp:
            dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
            dt_ist = dt.astimezone(ist)
            match_date = dt_ist.date()
            start_time = dt_ist.time()
        else:
            if date_text:
                date_lower = date_text.lower()
                if date_lower == "today":
                    match_date = today
                elif date_lower == "tomorrow":
                    match_date = today + timedelta(days=1)
                else:
                    current_year = datetime.now().year
                    full_date_str = f"{date_text}, {current_year}"
                    match_date = date_parser.parse(full_date_str).date()

            if time_text:
                parsed_time = date_parser.parse(time_text)
                dt_ist = ist.localize(parsed_time)
                start_time = dt_ist.time()

        logger.debug({
            "message":
            "Parsed datetime",
            "date_text":
            date_text,
            "time_text":
            time_text,
            "timestamp":
            timestamp,
            "match_date":
            str(match_date) if match_date else None,
            "start_time":
            start_time.strftime('%I:%M:%S %p') if start_time else None
        })
        return match_date, start_time
    except Exception as e:
        logger.error({"message": "Failed to parse datetime", "error": str(e)})
        return None, None


def parse_time_with_timezone(status):
    if not status:
        return datetime.now(pytz.UTC)
        
    # Skip parsing for known match statuses and cricket scores
    if status.strip() in ["Upcoming", "Match abandoned due to rain (No toss)", "Match abandoned without toss"] or \
       any(kwd in status.lower() for kwd in ["won by", "lost by", "drawn", "no result", "tied", "stumps", "overs", "day", "session", "tea", "lunch"]):
        return datetime.now(pytz.UTC)
        
    # Check if this is an IPL or cricket score format (team runs-wickets pattern)
    if re.search(r'[A-Z]{2,4}\d+[-/]\d+\s*\([^)]*\)', status):
        logger.debug({"message": "Cricket score format detected, skipping time parsing", "status": status})
        return datetime.now(pytz.UTC)
    
    # Check for "need X runs in Y balls" pattern
    if re.search(r'need \d+ runs in \d+ balls', status, re.IGNORECASE):
        logger.debug({"message": "Run chase format detected, skipping time parsing", "status": status})
        return datetime.now(pytz.UTC)

    try:
        ist = pytz.timezone("Asia/Kolkata")
        parsed_time = date_parser.parse(status, fuzzy=True)
        return ist.localize(parsed_time).astimezone(pytz.UTC)
    except Exception as e:
        logger.error({"message": "Time parsing failed", "error": str(e)})
        return datetime.now(pytz.UTC)


async def load_etag():
    if os.path.exists(ETAG_FILE):
        with open(ETAG_FILE, 'r') as f:
            return f.read().strip()
    return None


async def save_etag(etag):
    with open(ETAG_FILE, 'w') as f:
        f.write(etag)


async def fetch_page(url, last_etag=None):
    retry_options = ExponentialRetry(attempts=MAX_RETRIES,
                                     start_timeout=RETRY_DELAY,
                                     max_timeout=MAX_TIMEOUT)
    async with RetryClient(raise_for_status=False,
                           retry_options=retry_options) as retry_client:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Referer": "https://www.google.com",
            "If-None-Match": last_etag if last_etag else "",
            "Accept-Language": random.choice(LANGUAGES),
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Cache-Control": "max-age=0"
        }
        REQUESTS_TOTAL.inc()
        try:
            async with retry_client.get(url, headers=headers,
                                        timeout=MAX_TIMEOUT) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning({
                        "message": "Rate limit hit",
                        "status": 429,
                        "retry_after": retry_after,
                        "url": url
                    })
                    await asyncio.sleep(retry_after)
                    return None, last_etag
                if response.status == 304:
                    logger.info({"message": "No new data", "status": 304, "url": url})
                    return None, last_etag
                if response.status >= 400:
                    logger.error({
                        "message": f"HTTP error {response.status}",
                        "status": response.status,
                        "url": url
                    })
                    ERRORS_TOTAL.inc()
                    return None, last_etag
                response.raise_for_status()
                html = await response.text()
                return html, response.headers.get("ETag")
        except aiohttp.ClientTimeout as e:
            logger.error({"message": "Request timed out", "error": str(e), "url": url})
            ERRORS_TOTAL.inc()
            return None, last_etag
        except aiohttp.ClientError as e:
            logger.error({"message": "Client error", "error": str(e), "url": url})
            ERRORS_TOTAL.inc()
            return None, last_etag
        except Exception as e:
            logger.error({"message": "Unexpected error in fetch_page", "error": str(e), "url": url})
            ERRORS_TOTAL.inc()
            return None, last_etag


def parse_matches(html, page_type):
    parser = HTMLParser(html)
    matches = parser.css("div.cb-mtch-lst") or parser.css(
        "div.cb-schdl-list") or parser.css("div.cb-lv-main")
    if not matches:
        logger.warning({"message": "No matches found", "page_type": page_type})
        return []

    parsed_match_data = []

    for match in matches:
        try:
            # Match link and title
            link = match.css_first("a.cb-lv-scr-mtch-hdr") or match.css_first(
                "a.text-hvr-underline") or match.css_first("a.cb-scr-lnk")
            match_id = link.attributes.get(
                "href", f"temp_{random.randint(10**6, 10**7)}"
            ).split(
                "/")[-1] if link else f"temp_{random.randint(10**6, 10**7)}"
            match_title = link.text(strip=True) if link and link.text(
                strip=True) else "Unknown Match"

            # Find series name from header elements
            series_name = None
            series_header = match.parent.css_first(
                "div.cb-lv-grn-strip, div.cb-schdl-hdr, div.cb-scr-card-hdr, h2.cb-lv-scr-mtch-hdr"
            )

            if series_header:
                series_name = series_header.text(strip=True).upper()
                logger.debug({
                    "message": "Assigned series from header",
                    "match_title": match_title,
                    "series_name": series_name
                })

            # Fallback: Use parent block's header if available
            if not series_name:
                parent_block = match.parent
                if parent_block:
                    series_header = parent_block.css_first(
                        "div.cb-lv-grn-strip, div.cb-schdl-hdr")
                    if series_header:
                        series_name = series_header.text(strip=True).upper()
                        logger.debug({
                            "message": "Assigned series from parent header",
                            "match_title": match_title,
                            "series_name": series_name
                        })

            # Final fallback: Look for title attribute in links
            if not series_name:
                series_link = match.css_first("a[title]")
                if series_link and series_link.attributes.get("title"):
                    series_name = series_link.attributes["title"].upper()
                    logger.debug({
                        "message": "Assigned series from link title",
                        "match_title": match_title,
                        "series_name": series_name
                    })

            # Validate: Ensure series_name isn't the same as match_title
            if series_name and series_name == match_title.upper():
                series_name = None
                logger.debug({
                    "message":
                    "Series name matches match title, reset to None",
                    "match_title": match_title
                })

            # Status
            status_node = match.css_first("div.cb-lv-scrs-col") or match.css_first("div.cb-text-live") or \
                          match.css_first("div.cb-text-complete") or match.css_first("div.cb-text-preview")
            status = status_node.text(
                strip=True) if status_node and status_node.text(
                    strip=True) else "Upcoming"

            # Date and time parsing
            date_text = None
            time_text = None
            schedule_node = match.css_first("div.text-gray")
            raw_schedule = schedule_node.text(
                strip=True) if schedule_node else None

            time_node = match.css_first("span[ng-bind*=\"|date:'h:mm a'\"]")
            timestamp = None
            if time_node and "ng-bind" in time_node.attributes:
                ng_bind = time_node.attributes["ng-bind"]
                timestamp_match = re.search(r"(\d+)\s*\|\s*date:'h:mm a'",
                                            ng_bind)
                if timestamp_match:
                    timestamp = int(timestamp_match.group(1)) / 1000

            if not timestamp and raw_schedule:
                parts = re.split(r"[•,]\s*", raw_schedule)
                for part in parts:
                    part = part.strip()
                    if re.match(r"[A-Za-z]{3}\s+\d{1,2}",
                                part) or part.lower() in ["today", "tomorrow"]:
                        date_text = part
                    if re.match(r"\d{1,2}:\d{2}\s*[APM]{2}", part):
                        time_text = part

            match_type = classify_status(status)
            last_updated = parse_time_with_timezone(status)
            match_date, start_time = parse_upcoming_datetime(
                date_text, time_text, timestamp)

            if not match_id or not match_title:
                logger.warning({
                    "message": "Invalid match data",
                    "match_id": match_id
                })
                continue

            parsed_match_data.append({
                "match_id": str(match_id),
                "match_title": str(match_title),
                "score": str(status),
                "match_type": match_type,
                "series_name": series_name,
                "last_updated": last_updated,
                "match_date": match_date,
                "start_time": start_time
            })
            logger.info({
                "message":
                "Match parsed",
                "match_id":
                match_id,
                "match_title":
                match_title,
                "match_type":
                match_type,
                "series_name":
                series_name,
                "match_date":
                str(match_date) if match_date else None,
                "start_time":
                start_time.strftime('%I:%M:%S %p') if start_time else None
            })
        except Exception as e:
            logger.error({"message": "Error parsing match", "error": str(e)})
            PARSING_ERRORS.inc()
    return parsed_match_data


def merge_data(all_parsed_data):
    merged = {}
    priority = {"Live": 3, "Test": 3, "Completed": 2, "Upcoming": 1}

    for match in all_parsed_data:
        match["priority"] = priority.get(match["match_type"], 0)
        if match["match_id"] in merged:
            existing_match = merged[match["match_id"]]
            if match["priority"] > existing_match["priority"]:
                # Log status change
                if existing_match["match_type"] != match["match_type"]:
                    logger.info({
                        "message": "Match status changed",
                        "match_id": match["match_id"],
                        "match_title": match["match_title"],
                        "old_status": existing_match["match_type"],
                        "new_status": match["match_type"]
                    })
                # Update the match in the merged dict
                merged[match["match_id"]] = match
        else:
            merged[match["match_id"]] = match
    
    return list(merged.values())


@retry(stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=2, min=4, max=60))
async def update_database(pool, parsed_match_data):
    if not parsed_match_data:
        logger.info({"message": "No match data to update", "count": 0})
        return

    async with pool.acquire() as conn:
        try:
            formatted_data = [
                (m["match_id"], m["match_title"], m["score"], m["match_type"],
                 m["series_name"], m["last_updated"], m["match_date"],
                 m["start_time"]) for m in parsed_match_data
                if m["match_title"] and m["score"]
            ]
            await conn.executemany(
                """
                INSERT INTO matches (match_id, match_title, score, match_type, series_name, last_updated, match_date, start_time) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
                ON CONFLICT (match_id) DO UPDATE SET 
                    match_title = EXCLUDED.match_title, 
                    score = EXCLUDED.score, 
                    match_type = EXCLUDED.match_type, 
                    series_name = EXCLUDED.series_name,
                    last_updated = EXCLUDED.last_updated,
                    match_date = EXCLUDED.match_date,
                    start_time = EXCLUDED.start_time
            """, formatted_data)
            logger.info({
                "message": "Database updated",
                "count": len(parsed_match_data)
            })
        except asyncpg.exceptions.PostgresError as e:
            logger.error({"message": "Database error", "error": str(e)})
            DB_ERRORS.inc()
            raise


async def scraping_task(pool, last_etag=None):
    all_parsed_match_data = []
    for page_type, url in CRICBUZZ_URLS.items():
        html, new_etag = await fetch_page(url, last_etag)
        if html:
            parsed_match_data = parse_matches(html, page_type)
            all_parsed_match_data.extend(parsed_match_data)
    if all_parsed_match_data:
        merged_data = merge_data(all_parsed_match_data)
        await update_database(pool, merged_data)
            
    return new_etag if 'new_etag' in locals() else last_etag


async def shutdown(pool):
    logger.info({"message": "Shutting down gracefully", "pid": os.getpid()})
    if pool:
        await pool.close()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


# ----------------- Flask App Runner -------------------
async def run_flask():
    """Run the Flask API server."""
    logger.info({"message": f"Starting unified server on port {PORT}"})
    
    try:
        # First check if hypercorn is available
        from hypercorn.asyncio import serve
        from hypercorn.config import Config
        
        config = Config()
        config.bind = [f"0.0.0.0:{PORT}"]
        config.use_reloader = False
        config.asyncio = True  # Enable asyncio support
        
        await serve(app, config)
    except ImportError:
        # Fall back to regular Flask if hypercorn is not available
        logger.warning({"message": "Hypercorn not found, using Flask's built-in server (not production ready)"})
        
        # Have to run in a separate thread as Flask's development server is blocking
        import threading
        
        def run_flask_dev_server():
            # For development server, we need to use flask sync mode
            # and ensure our routes are properly wrapped
            app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)
            
        threading.Thread(target=run_flask_dev_server, daemon=True).start()
        
        # Keep the async task alive
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour at a time
    except Exception as e:
        logger.error({"message": "Flask server error", "error": str(e)})


async def main():
    global set_db_pool  # For signal handlers

    # Clear the log file at startup
    try:
        with open('cricket_scraper.log', 'w') as f:
            f.write("")  # Truncate the file
        logger.info({"message": "Log file cleared at startup"})
    except Exception as e:
        logger.error({"message": "Failed to clear log file", "error": str(e)})
    
    # Setup database connection
    pool = None
    try:
        pool = await init_db_pool()
        # Store pool in the Flask app context
        app.db_pool = pool
        
        # Register the pool with signal handlers if function exists
        if 'set_db_pool' in globals():
            set_db_pool(pool)
            
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
            logger.info({
                "message": "Database connection verified",
                "pid": os.getpid()
            })
    except Exception as e:
        logger.error({
            "message": "Failed to initialize database",
            "error": str(e)
        })
        sys.exit(1)

    # Start Flask server in a separate task
    flask_task = asyncio.create_task(run_flask())
    logger.info({"message": f"Unified server task started on port {PORT}", "pid": os.getpid()})

    # Start the main cricket data scraper
    last_etag = await load_etag()
    logger.info({
        "message": "Cricket scraper started",
        "pid": os.getpid(),
        "timestamp": time.time()
    })
    print("Scraper chalu hai. Logs dekho 'cricket_scraper.log' mein.")
    
    # Summary of active servers
    print(f"\nUnified server running on port {PORT}:")
    print(f"- API endpoints: http://localhost:{PORT}/api/matches")
    print(f"- Health check: http://localhost:{PORT}/health")
    print(f"- Prometheus metrics: http://localhost:{PORT}/metrics")
    print(f"- Render URL check: http://localhost:{PORT}/")
    print("\n")
    
    # Reduced base delay for more frequent updates
    base_delay = 30  # Changed from 60 to 30 seconds
    
    # Add dynamic delay adjustment - check more frequently if live matches are ongoing
    live_match_count = 0
    error_count = 0
    max_consecutive_errors = 5
    
    # For tracking the last time we pinged the Render URL
    last_render_ping_time = 0
    # Random ping interval between 1 minute and 2.5 minutes (60-150 seconds)
    min_ping_interval = 60  # 1 minute
    max_ping_interval = 150  # 2.5 minutes
    
    try:
        while True:
            try:
                # Current time for timing operations
                current_time = time.time()
                
                # Generate a random interval for this cycle
                render_ping_interval = random.uniform(min_ping_interval, max_ping_interval)
                
                # Check if we have live matches in the database and adjust checking frequency
                async with pool.acquire() as conn:
                    live_match_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM matches WHERE match_type IN ('Live', 'Test')")
                
                # If we have live matches, check more frequently
                current_delay = max(15, base_delay - (live_match_count * 2))
                
                # Ping the Render URL to keep it alive (random interval between 1-2.5 minutes)
                render_url = os.getenv('RENDER_URL')
                if render_url and (current_time - last_render_ping_time) > render_ping_interval:
                    logger.info({"message": f"Pinging Render URL to keep service alive: {render_url}", "interval": round(render_ping_interval, 1)})
                    try:
                        # Use random User-Agent to mimic different browsers
                        headers = {"User-Agent": random.choice(USER_AGENTS)}
                        
                        # Randomly choose between different endpoints to look more like real user traffic
                        endpoints = ["", "/health", "/check-render"]
                        chosen_endpoint = random.choice(endpoints)
                        ping_url = f"{render_url}{chosen_endpoint}"
                        
                        async with aiohttp.ClientSession() as session:
                            async with session.get(ping_url, headers=headers, timeout=10) as response:
                                if response.status == 200:
                                    logger.info({"message": f"Successfully pinged Render URL: {ping_url}"})
                                else:
                                    logger.warning({"message": f"Render URL returned status {response.status}"})
                        last_render_ping_time = current_time
                    except Exception as ping_error:
                        logger.error({"message": "Failed to ping Render URL", "error": str(ping_error)})
                
                last_etag = await scraping_task(pool, last_etag)
                if last_etag:
                    await save_etag(last_etag)
                
                # Reset error count on successful run
                error_count = 0
                
                # Memory management
                gc.collect()
                
                # Use a shorter delay with small random variation
                delay = random.uniform(current_delay, current_delay + 15)
                logger.info({
                    "message": "Sleeping", 
                    "delay": delay,
                    "live_matches": live_match_count
                })
                await asyncio.sleep(delay)
            except asyncpg.exceptions.PostgresConnectionError as e:
                error_count += 1
                logger.error({
                    "message": "Database connection lost",
                    "error": str(e).replace(DB_PASS, "********"),
                    "error_count": error_count
                })
                
                # Try to reconnect to the database
                if error_count >= max_consecutive_errors:
                    logger.critical({
                        "message": f"Too many consecutive errors ({error_count}), attempting to reinitialize pool"
                    })
                    try:
                        if pool:
                            await pool.close()
                        pool = await init_db_pool()
                        app.db_pool = pool  # Update Flask app's pool reference
                        if 'set_db_pool' in globals():
                            set_db_pool(pool)
                        error_count = 0
                    except Exception as reconnect_error:
                        logger.critical({
                            "message": "Failed to reinitialize pool",
                            "error": str(reconnect_error).replace(DB_PASS, "********")
                        })
                
                await asyncio.sleep(min(30 * error_count, 300))  # Backoff with max 5 minutes
            except Exception as e:
                error_count += 1
                logger.error({
                    "message": "Scraping task failed",
                    "error": str(e),
                    "error_count": error_count
                })
                if error_count >= max_consecutive_errors:
                    logger.critical({
                        "message": f"Too many consecutive errors ({error_count}), restarting main loop"
                    })
                    error_count = 0
                await asyncio.sleep(min(30 * error_count, 300))  # Backoff with max 5 minutes
    except asyncio.CancelledError:
        logger.info({"message": "Main task cancelled"})
    except Exception as e:
        logger.error({"message": "Main task exception", "error": str(e)})
    finally:
        # Cancel background task
        flask_task.cancel()
        try:
            await flask_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    logger.info({
        "message": "Check robots.txt at https://www.cricbuzz.com/robots.txt before running",
        "pid": os.getpid()
    })
    
    # Global variable for database pool that can be accessed by signal handlers
    db_pool = None
    
    # Function to set the pool from main()
    def set_db_pool(pool):
        global db_pool
        db_pool = pool
        
    # Make function available globally
    globals()['set_db_pool'] = set_db_pool
    
    try:
        # Handle signals for graceful shutdown on non-Windows platforms
        if sys.platform != 'win32':
            loop = asyncio.get_event_loop()
            
            def handle_shutdown():
                if db_pool:
                    asyncio.create_task(shutdown(db_pool))
                    
            # Install signal handlers
            loop.add_signal_handler(signal.SIGINT, handle_shutdown)
            loop.add_signal_handler(signal.SIGTERM, handle_shutdown)
            
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info({"message": "Keyboard interrupt received, shutting down"})
    except Exception as e:
        logger.error({"message": "Main execution failed", "error": str(e)})
        sys.exit(1)
