import asyncio
import logging
import os
import re
import sqlite3
import threading
import time
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Set
from urllib.parse import quote
from pathlib import Path

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("bdo-kr-fast-record")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "bdo_fast_record.db")
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

SCAN_LOOP_SECONDS = int(os.getenv("SCAN_LOOP_SECONDS", "10"))
DEFAULT_SCAN_INTERVAL_MIN = int(os.getenv("DEFAULT_SCAN_INTERVAL_MIN", "15"))
PRIORITY_SCAN_INTERVAL_MIN = int(os.getenv("PRIORITY_SCAN_INTERVAL_MIN", "10"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "2"))
REQUEST_RETRY_COUNT = int(os.getenv("REQUEST_RETRY_COUNT", "2"))
RENAME_WINDOW_MINUTES = int(os.getenv("RENAME_WINDOW_MINUTES", "30"))

# 안정형 설정
MIN_VALID_MEMBER_COUNT = int(os.getenv("MIN_VALID_MEMBER_COUNT", "1"))
MAX_HISTORY_LINES = int(os.getenv("MAX_HISTORY_LINES", "30"))
MAX_FIELD_CHARS = int(os.getenv("MAX_FIELD_CHARS", "950"))
MAX_CHANGE_RATIO = float(os.getenv("MAX_CHANGE_RATIO", "0.65"))
DUPLICATE_EVENT_COOLDOWN_HOURS = int(os.getenv("DUPLICATE_EVENT_COOLDOWN_HOURS", "24"))
PENDING_CONFIRM_MINUTES = int(os.getenv("PENDING_CONFIRM_MINUTES", "5"))
PENDING_EXPIRE_MINUTES = int(os.getenv("PENDING_EXPIRE_MINUTES", "30"))
ANTI_BLOCK_MIN_DELAY_SECONDS = float(os.getenv("ANTI_BLOCK_MIN_DELAY_SECONDS", "4.0"))
ANTI_BLOCK_MAX_DELAY_SECONDS = float(os.getenv("ANTI_BLOCK_MAX_DELAY_SECONDS", "9.0"))
FIRST_SCAN_RETRY_COUNT = int(os.getenv("FIRST_SCAN_RETRY_COUNT", "2"))
FIRST_SCAN_RETRY_DELAY_SECONDS = float(os.getenv("FIRST_SCAN_RETRY_DELAY_SECONDS", "6.0"))
NIGHT_SLOWDOWN_ENABLED = os.getenv("NIGHT_SLOWDOWN_ENABLED", "true").lower() == "true"
NIGHT_SLOWDOWN_START_HOUR = int(os.getenv("NIGHT_SLOWDOWN_START_HOUR", "2"))
NIGHT_SLOWDOWN_END_HOUR = int(os.getenv("NIGHT_SLOWDOWN_END_HOUR", "8"))
NIGHT_SLOWDOWN_MULTIPLIER = float(os.getenv("NIGHT_SLOWDOWN_MULTIPLIER", "2.0"))
FAIL_BACKOFF_MINUTES = int(os.getenv("FAIL_BACKOFF_MINUTES", "30"))


# Playwright 브라우저 모드
BROWSER_HEADLESS = os.getenv("BROWSER_HEADLESS", "false").lower() == "true"
BROWSER_PAGE_TIMEOUT = int(os.getenv("BROWSER_PAGE_TIMEOUT", "8"))
BROWSER_WAIT_SECONDS = int(os.getenv("BROWSER_WAIT_SECONDS", "2"))
BROWSER_COOLDOWN_SECONDS = float(os.getenv("BROWSER_COOLDOWN_SECONDS", "0.3"))

# 채널 이름. 디스코드 채널명과 정확히 같아야 함.

RENAME_ALERT_WEEKDAY_ONLY = os.getenv("RENAME_ALERT_WEEKDAY_ONLY", "true").lower() == "true"
RENAME_ALERT_ALLOWED_WEEKDAY = int(os.getenv("RENAME_ALERT_ALLOWED_WEEKDAY", "2"))
RENAME_ALERT_WINDOW_HOURS = int(os.getenv("RENAME_ALERT_WINDOW_HOURS", "24"))

TRACK_CHANNEL_NAME = os.getenv("TRACK_CHANNEL_NAME", "추적_봇")
ADMIN_CHANNEL_NAME = os.getenv("ADMIN_CHANNEL_NAME", "추적_관리자")

# 안정형 설정
MIN_VALID_MEMBER_COUNT = int(os.getenv("MIN_VALID_MEMBER_COUNT", "1"))
MAX_HISTORY_LINES = int(os.getenv("MAX_HISTORY_LINES", "30"))
MAX_FIELD_CHARS = int(os.getenv("MAX_FIELD_CHARS", "950"))
MAX_CHANGE_RATIO = float(os.getenv("MAX_CHANGE_RATIO", "0.65"))
DUPLICATE_EVENT_COOLDOWN_HOURS = int(os.getenv("DUPLICATE_EVENT_COOLDOWN_HOURS", "24"))
PENDING_CONFIRM_MINUTES = int(os.getenv("PENDING_CONFIRM_MINUTES", "5"))
PENDING_EXPIRE_MINUTES = int(os.getenv("PENDING_EXPIRE_MINUTES", "30"))
ANTI_BLOCK_MIN_DELAY_SECONDS = float(os.getenv("ANTI_BLOCK_MIN_DELAY_SECONDS", "4.0"))
ANTI_BLOCK_MAX_DELAY_SECONDS = float(os.getenv("ANTI_BLOCK_MAX_DELAY_SECONDS", "9.0"))
FIRST_SCAN_RETRY_COUNT = int(os.getenv("FIRST_SCAN_RETRY_COUNT", "2"))
FIRST_SCAN_RETRY_DELAY_SECONDS = float(os.getenv("FIRST_SCAN_RETRY_DELAY_SECONDS", "6.0"))
NIGHT_SLOWDOWN_ENABLED = os.getenv("NIGHT_SLOWDOWN_ENABLED", "true").lower() == "true"
NIGHT_SLOWDOWN_START_HOUR = int(os.getenv("NIGHT_SLOWDOWN_START_HOUR", "2"))
NIGHT_SLOWDOWN_END_HOUR = int(os.getenv("NIGHT_SLOWDOWN_END_HOUR", "8"))
NIGHT_SLOWDOWN_MULTIPLIER = float(os.getenv("NIGHT_SLOWDOWN_MULTIPLIER", "2.0"))
FAIL_BACKOFF_MINUTES = int(os.getenv("FAIL_BACKOFF_MINUTES", "30"))


# 채널 이름. 디스코드 채널명과 정확히 같아야 함.
TRACK_CHANNEL_NAME = os.getenv("TRACK_CHANNEL_NAME", "추적_봇")
ADMIN_CHANNEL_NAME = os.getenv("ADMIN_CHANNEL_NAME", "추적_관리자")

GUILD_PROFILE_URL = "https://www.kr.playblackdesert.com/ko-KR/Adventure/Guild/GuildProfile?guildName={guild_name}&region=KR"

if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN 이 비어 있습니다. .env 파일에 넣으세요.")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_text() -> str:
    return utc_now().isoformat(timespec="seconds")


def parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text)


def kst_display(text: Optional[str], short: bool = False) -> str:
    if not text:
        return "-"
    try:
        dt = parse_iso(text).astimezone(timezone(timedelta(hours=9)))
        return dt.strftime("%m-%d %H:%M KST") if short else dt.strftime("%Y-%m-%d %H:%M:%S KST")
    except Exception:
        return text


def kst_date_only(text: Optional[str]) -> str:
    if not text:
        return "-"
    try:
        dt = parse_iso(text).astimezone(timezone(timedelta(hours=9)))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return text


def kst_date_only(text: Optional[str]) -> str:
    if not text:
        return "-"
    try:
        dt = parse_iso(text).astimezone(timezone(timedelta(hours=9)))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return text


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip()


def build_guild_url(guild_name: str) -> str:
    return GUILD_PROFILE_URL.format(guild_name=quote(guild_name))


def limit_text(lines, max_chars=MAX_FIELD_CHARS):
    out = []
    total = 0
    for line in lines:
        add = len(line) + 1
        if total + add > max_chars:
            remain = len(lines) - len(out)
            if remain > 0:
                out.append(f"...외 {remain}개")
            break
        out.append(line)
        total += add
    return "\n".join(out) if out else "-"


def chunk_lines_for_embed(lines: list[str], max_chars: int = 950, max_chunks: int = 5):
    chunks = []
    current = []
    current_len = 0

    for line in lines:
        add_len = len(line) + 1
        if current and current_len + add_len > max_chars:
            chunks.append("\n".join(current))
            current = []
            current_len = 0

            if len(chunks) >= max_chunks:
                break

        current.append(line)
        current_len += add_len

    if current and len(chunks) < max_chunks:
        chunks.append("\n".join(current))

    shown = sum(chunk.count("\n") + 1 for chunk in chunks if chunk)
    remaining = max(0, len(lines) - shown)

    if remaining and chunks:
        chunks[-1] += f"\n...외 {remaining}개"

    return chunks


def add_member_fields_to_embed(embed, members: list[str], per_field: int = 20, max_fields: int = 20):
    if not members:
        embed.add_field(name="현재 구성원", value="기록 없음", inline=False)
        return

    total = len(members)
    max_members = per_field * max_fields
    shown_members = members[:max_members]

    for idx in range(0, len(shown_members), per_field):
        chunk = shown_members[idx:idx + per_field]
        lines = [f"• `{name}`" for name in chunk]

        if idx + per_field >= max_members and total > max_members:
            lines.append(f"...외 {total - max_members}명")

        embed.add_field(
            name=f"구성원 {idx + 1}-{idx + len(chunk)}",
            value="\n".join(lines) if lines else "-",
            inline=False
        )



@dataclass
class GuildScanResult:
    guild_name: str
    members: Set[str]
    source_url: str
    fetched_at: str


class Database:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS watched_guilds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_name TEXT NOT NULL UNIQUE,
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 0,
            scan_interval_min INTEGER NOT NULL DEFAULT 15,
            next_scan_after TEXT,
            last_success_at TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_name TEXT NOT NULL,
            scanned_at TEXT NOT NULL,
            member_count INTEGER NOT NULL,
            source_url TEXT
        );

        CREATE TABLE IF NOT EXISTS snapshot_members (
            snapshot_id INTEGER NOT NULL,
            family_name TEXT NOT NULL,
            PRIMARY KEY (snapshot_id, family_name),
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            family_name TEXT NOT NULL,
            guild_name TEXT NOT NULL,
            event_type TEXT NOT NULL CHECK (event_type IN ('join', 'leave')),
            detected_at TEXT NOT NULL,
            source_url TEXT
        );


        CREATE TABLE IF NOT EXISTS watched_families (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            family_name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_watched_families_name
        ON watched_families(family_name);

        CREATE TABLE IF NOT EXISTS pending_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_name TEXT NOT NULL,
            family_name TEXT NOT NULL,
            event_type TEXT NOT NULL CHECK (event_type IN ('join', 'leave')),
            first_detected_at TEXT NOT NULL,
            confirm_after TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            source_url TEXT,
            UNIQUE(guild_name, family_name, event_type)
        );

        CREATE INDEX IF NOT EXISTS idx_pending_events_confirm ON pending_events(confirm_after);
        CREATE INDEX IF NOT EXISTS idx_pending_events_key ON pending_events(guild_name, family_name, event_type);

        CREATE TABLE IF NOT EXISTS rename_suspicions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            old_family_name TEXT NOT NULL,
            new_family_name TEXT NOT NULL,
            guild_name TEXT,
            confidence TEXT NOT NULL,
            score INTEGER NOT NULL,
            reason TEXT NOT NULL,
            detected_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS guild_settings (
            discord_guild_id TEXT PRIMARY KEY,
            notify_channel_id TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_events_family_time ON events (family_name, detected_at DESC);
        CREATE INDEX IF NOT EXISTS idx_events_guild_time ON events (guild_name, detected_at DESC);
        CREATE INDEX IF NOT EXISTS idx_snapshots_guild_time ON snapshots (guild_name, scanned_at DESC);
        """)
        self.conn.commit()

    def add_watched_guild(self, guild_name: str, priority: int = 0, interval_min: Optional[int] = None):
        interval_min = interval_min or (PRIORITY_SCAN_INTERVAL_MIN if priority else DEFAULT_SCAN_INTERVAL_MIN)
        now = utc_now_text()
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO watched_guilds (guild_name, enabled, priority, scan_interval_min, next_scan_after, created_at)
            VALUES (?, 1, ?, ?, ?, ?)
            ON CONFLICT(guild_name) DO UPDATE SET
                enabled = 1,
                priority = excluded.priority,
                scan_interval_min = excluded.scan_interval_min
        """, (guild_name, priority, interval_min, now, now))
        self.conn.commit()

    def remove_watched_guild(self, guild_name: str) -> int:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM watched_guilds WHERE guild_name = ?", (guild_name,))
        self.conn.commit()
        return cur.rowcount

    def get_priority_guilds(self, interval_min: int = 15):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, scan_interval_min
            FROM watched_guilds
            WHERE enabled = 1
              AND scan_interval_min = ?
            ORDER BY guild_name COLLATE NOCASE
        """, (interval_min,))
        return cur.fetchall()

    def bulk_update_scan_interval(self, guild_names: list[str], interval_min: int):
        cur = self.conn.cursor()
        updated = []
        not_found = []

        for guild_name in guild_names:
            name = guild_name.strip()
            if not name:
                continue
            cur.execute("""
                UPDATE watched_guilds
                SET scan_interval_min = ?
                WHERE enabled = 1
                  AND lower(guild_name) = lower(?)
            """, (interval_min, name))

            if cur.rowcount > 0:
                updated.append(name)
            else:
                not_found.append(name)

        self.conn.commit()
        return updated, not_found

    def bulk_update_by_current_interval(self, from_interval: int, to_interval: int):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name
            FROM watched_guilds
            WHERE enabled = 1
              AND scan_interval_min = ?
            ORDER BY guild_name COLLATE NOCASE
        """, (from_interval,))
        rows = cur.fetchall()
        names = [r["guild_name"] for r in rows]

        cur.execute("""
            UPDATE watched_guilds
            SET scan_interval_min = ?
            WHERE enabled = 1
              AND scan_interval_min = ?
        """, (to_interval, from_interval))
        self.conn.commit()
        return names

    def add_watched_family(self, family_name: str):
        name = normalize_name(family_name)
        cur = self.conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO watched_families (family_name, created_at)
            VALUES (?, ?)
        """, (name, utc_now_text()))
        self.conn.commit()
        return cur.rowcount > 0

    def remove_watched_family(self, family_name: str):
        name = normalize_name(family_name)
        cur = self.conn.cursor()
        cur.execute("""
            DELETE FROM watched_families
            WHERE lower(family_name) = lower(?)
        """, (name,))
        self.conn.commit()
        return cur.rowcount > 0

    def list_watched_families(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT family_name, created_at
            FROM watched_families
            ORDER BY family_name COLLATE NOCASE
        """)
        return cur.fetchall()

    def get_watched_family_names(self):
        cur = self.conn.cursor()
        cur.execute("SELECT family_name FROM watched_families")
        return {r["family_name"] for r in cur.fetchall()}

    def list_watched_guilds(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, priority, scan_interval_min, last_success_at, last_error
            FROM watched_guilds
            WHERE enabled = 1
            ORDER BY priority DESC, guild_name COLLATE NOCASE
        """)
        return cur.fetchall()

    def get_due_guilds(self, limit: int):
        now = utc_now_text()
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, priority, scan_interval_min
            FROM watched_guilds
            WHERE enabled = 1 AND (next_scan_after IS NULL OR next_scan_after <= ?)
            ORDER BY priority DESC, next_scan_after ASC, guild_name COLLATE NOCASE
            LIMIT ?
        """, (now, limit))
        return cur.fetchall()

    def mark_scan_success(self, guild_name: str, interval_min: int):
        next_at = utc_now() + timedelta(minutes=effective_interval_minutes(interval_min))
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE watched_guilds
            SET last_success_at = ?, last_error = NULL, next_scan_after = ?
            WHERE guild_name = ?
        """, (utc_now_text(), next_at.isoformat(timespec="seconds"), guild_name))
        self.conn.commit()

    def mark_scan_failure(self, guild_name: str, error_text: str):
        next_at = utc_now() + timedelta(minutes=FAIL_BACKOFF_MINUTES)
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE watched_guilds
            SET last_error = ?, next_scan_after = ?
            WHERE lower(guild_name) = lower(?)
        """, (error_text[:300], next_at.isoformat(timespec="seconds"), guild_name))
        self.conn.commit()


    def get_latest_snapshot_members(self, guild_name: str) -> Set[str]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT sm.family_name
            FROM snapshots s
            JOIN snapshot_members sm ON sm.snapshot_id = s.id
            WHERE s.guild_name = ?
            ORDER BY s.id DESC
        """, (guild_name,))
        return {r["family_name"] for r in cur.fetchall()}

    def create_snapshot(self, guild_name: str, members: Set[str], source_url: str):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO snapshots (guild_name, scanned_at, member_count, source_url)
            VALUES (?, ?, ?, ?)
        """, (guild_name, utc_now_text(), len(members), source_url))
        snapshot_id = cur.lastrowid
        cur.executemany("""
            INSERT INTO snapshot_members (snapshot_id, family_name)
            VALUES (?, ?)
        """, [(snapshot_id, m) for m in sorted(members)])
        self.conn.commit()

    def get_latest_event_type(self, guild_name: str, family_name: str) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT event_type
            FROM events
            WHERE lower(guild_name) = lower(?)
              AND lower(family_name) = lower(?)
            ORDER BY id DESC
            LIMIT 1
        """, (guild_name, family_name))
        row = cur.fetchone()
        return row["event_type"] if row else None

    def has_recent_event(self, guild_name: str, family_name: str, event_type: str, hours: int) -> bool:
        cutoff = (utc_now() - timedelta(hours=hours)).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute("""
            SELECT 1
            FROM events
            WHERE lower(guild_name) = lower(?)
              AND lower(family_name) = lower(?)
              AND event_type = ?
              AND detected_at >= ?
            LIMIT 1
        """, (guild_name, family_name, event_type, cutoff))
        return cur.fetchone() is not None

    def insert_events(self, guild_name: str, joined: list[str], left: list[str], source_url: str):
        cur = self.conn.cursor()
        now = utc_now_text()
        cur.executemany("""
            INSERT INTO events (family_name, guild_name, event_type, detected_at, source_url)
            VALUES (?, ?, 'join', ?, ?)
        """, [(x, guild_name, now, source_url) for x in joined])
        cur.executemany("""
            INSERT INTO events (family_name, guild_name, event_type, detected_at, source_url)
            VALUES (?, ?, 'leave', ?, ?)
        """, [(x, guild_name, now, source_url) for x in left])
        self.conn.commit()

    def set_notify_channel(self, discord_guild_id: int, channel_id: int):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO guild_settings (discord_guild_id, notify_channel_id)
            VALUES (?, ?)
            ON CONFLICT(discord_guild_id) DO UPDATE SET notify_channel_id = excluded.notify_channel_id
        """, (str(discord_guild_id), str(channel_id)))
        self.conn.commit()

    def get_notify_channel(self, discord_guild_id: int) -> Optional[int]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT notify_channel_id
            FROM guild_settings
            WHERE discord_guild_id = ?
        """, (str(discord_guild_id),))
        row = cur.fetchone()
        return int(row["notify_channel_id"]) if row and row["notify_channel_id"] else None

    def get_recent_leave_events(self, guild_name: str, within_minutes: int, limit: int = 50):
        cutoff = (utc_now() - timedelta(minutes=within_minutes)).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute("""
            SELECT family_name, detected_at
            FROM events
            WHERE lower(guild_name) = lower(?) AND event_type = 'leave' AND detected_at >= ?
            ORDER BY id DESC
            LIMIT ?
        """, (guild_name, cutoff, limit))
        return cur.fetchall()

    def insert_rename_suspicion(self, old_name: str, new_name: str, guild_name: str, confidence: str, score: int, reason: str):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO rename_suspicions (old_family_name, new_family_name, guild_name, confidence, score, reason, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (old_name, new_name, guild_name, confidence, score, reason, utc_now_text()))
        self.conn.commit()

    def set_guild_priority(self, guild_name: str, priority: int, interval_min: int) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE watched_guilds
            SET priority = ?, scan_interval_min = ?
            WHERE lower(guild_name) = lower(?)
        """, (priority, interval_min, guild_name))
        self.conn.commit()
        return cur.rowcount

    def get_unscanned_guilds(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name
            FROM watched_guilds
            WHERE enabled = 1 AND last_success_at IS NULL
            ORDER BY guild_name COLLATE NOCASE
        """)
        return [r["guild_name"] for r in cur.fetchall()]

    def get_family_rename_suspicions(self, family_name: str, limit: int = 5):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT old_family_name, new_family_name, confidence, score, detected_at
            FROM rename_suspicions
            WHERE lower(old_family_name) = lower(?) OR lower(new_family_name) = lower(?)
            ORDER BY id DESC
            LIMIT ?
        """, (family_name, family_name, limit))
        return cur.fetchall()

    def get_family_recent_events(self, family_name: str, limit: int = 50):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, event_type, detected_at
            FROM events
            WHERE lower(family_name) = lower(?)
            ORDER BY id DESC
            LIMIT ?
        """, (family_name, limit))
        return cur.fetchall()

    def get_family_recent_guilds(self, family_name: str, limit: int = 10):
        rows = self.get_family_recent_events(family_name, 200)
        out = []
        seen = set()
        for r in rows:
            guild_name = r["guild_name"]
            if guild_name not in seen:
                seen.add(guild_name)
                out.append((guild_name, r["detected_at"], r["event_type"]))
            if len(out) >= limit:
                break
        return out

    def find_family_current_guilds_from_snapshots(self, family_name: str, limit: int = 10):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT s.guild_name, s.scanned_at, s.member_count
            FROM snapshot_members sm
            JOIN snapshots s ON s.id = sm.snapshot_id
            JOIN (
                SELECT guild_name, MAX(id) AS max_id
                FROM snapshots
                GROUP BY guild_name
            ) latest ON latest.max_id = s.id
            WHERE lower(sm.family_name) = lower(?)
            ORDER BY s.scanned_at DESC
            LIMIT ?
        """, (family_name, limit))
        return cur.fetchall()

    def get_family_current_guild_guess(self, family_name: str) -> Optional[str]:
        rows = self.get_family_recent_events(family_name, 30)
        if not rows:
            return None
        latest = rows[0]
        if latest["event_type"] == "join":
            return latest["guild_name"]
        return None

    def get_guild_recent_events(self, guild_name: str, limit: int = 30):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT family_name, event_type, detected_at
            FROM events
            WHERE lower(guild_name) = lower(?)
            ORDER BY id DESC
            LIMIT ?
        """, (guild_name, limit))
        return cur.fetchall()

    def get_current_guild_member_count(self, guild_name: str) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT member_count
            FROM snapshots
            WHERE lower(guild_name) = lower(?)
            ORDER BY id DESC
            LIMIT 1
        """, (guild_name,))
        row = cur.fetchone()
        return int(row["member_count"]) if row else 0


    def get_watched_guild_info(self, guild_name: str):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, priority, scan_interval_min, last_success_at, last_error, next_scan_after
            FROM watched_guilds
            WHERE enabled = 1 AND lower(guild_name) = lower(?)
            LIMIT 1
        """, (guild_name,))
        return cur.fetchone()

    def set_scan_interval(self, guild_name: str, interval_min: int) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE watched_guilds
            SET scan_interval_min = ?
            WHERE enabled = 1 AND lower(guild_name) = lower(?)
        """, (interval_min, guild_name))
        self.conn.commit()
        return cur.rowcount

    def get_failed_guilds(self, limit: int = 30):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, last_error, last_success_at, next_scan_after
            FROM watched_guilds
            WHERE enabled = 1 AND last_error IS NOT NULL
            ORDER BY guild_name COLLATE NOCASE
            LIMIT ?
        """, (limit,))
        return cur.fetchall()

    def get_recent_events_all(self, hours: int = 2, limit: int = 50):
        cutoff = (utc_now() - timedelta(hours=hours)).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, family_name, event_type, detected_at
            FROM events
            WHERE detected_at >= ?
            ORDER BY id DESC
            LIMIT ?
        """, (cutoff, limit))
        return cur.fetchall()

    def get_recent_events_for_guild(self, guild_name: str, days: int = 7, limit: int = 50):
        cutoff = (utc_now() - timedelta(days=days)).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, family_name, event_type, detected_at
            FROM events
            WHERE lower(guild_name) = lower(?) AND detected_at >= ?
            ORDER BY id DESC
            LIMIT ?
        """, (guild_name, cutoff, limit))
        return cur.fetchall()

    def get_event_rank_by_guild(self, days: int = 7, limit: int = 10):
        cutoff = (utc_now() - timedelta(days=days)).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute("""
            SELECT guild_name, COUNT(*) AS cnt
            FROM events
            WHERE detected_at >= ?
            GROUP BY guild_name
            ORDER BY cnt DESC
            LIMIT ?
        """, (cutoff, limit))
        return cur.fetchall()

    def get_activity_rank_by_family(self, days: int = 7, limit: int = 10):
        cutoff = (utc_now() - timedelta(days=days)).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute("""
            SELECT family_name, COUNT(*) AS cnt
            FROM events
            WHERE detected_at >= ?
            GROUP BY family_name
            ORDER BY cnt DESC
            LIMIT ?
        """, (cutoff, limit))
        return cur.fetchall()

    def get_snapshot_members_all(self, guild_name: str):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT sm.family_name
            FROM snapshots s
            JOIN snapshot_members sm ON sm.snapshot_id = s.id
            WHERE lower(s.guild_name) = lower(?)
              AND s.id = (
                SELECT id
                FROM snapshots
                WHERE lower(guild_name) = lower(?)
                ORDER BY id DESC
                LIMIT 1
              )
            ORDER BY sm.family_name COLLATE NOCASE
        """, (guild_name, guild_name))
        return [r["family_name"] for r in cur.fetchall()]

    def get_snapshot_member_sample(self, guild_name: str, limit: int = 25):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT sm.family_name
            FROM snapshots s
            JOIN snapshot_members sm ON sm.snapshot_id = s.id
            WHERE lower(s.guild_name) = lower(?)
              AND s.id = (
                SELECT id
                FROM snapshots
                WHERE lower(guild_name) = lower(?)
                ORDER BY id DESC
                LIMIT 1
              )
            ORDER BY sm.family_name COLLATE NOCASE
            LIMIT ?
        """, (guild_name, guild_name, limit))
        return [r["family_name"] for r in cur.fetchall()]

    def upsert_pending_event(self, guild_name: str, family_name: str, event_type: str, source_url: str):
        now = utc_now()
        confirm_after = now + timedelta(minutes=PENDING_CONFIRM_MINUTES)
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO pending_events (
                guild_name, family_name, event_type, first_detected_at,
                confirm_after, last_seen_at, source_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_name, family_name, event_type) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                source_url = excluded.source_url
        """, (
            guild_name, family_name, event_type,
            now.isoformat(timespec="seconds"),
            confirm_after.isoformat(timespec="seconds"),
            now.isoformat(timespec="seconds"),
            source_url,
        ))
        self.conn.commit()

    def delete_pending_event(self, guild_name: str, family_name: str, event_type: str):
        cur = self.conn.cursor()
        cur.execute("""
            DELETE FROM pending_events
            WHERE lower(guild_name) = lower(?)
              AND lower(family_name) = lower(?)
              AND event_type = ?
        """, (guild_name, family_name, event_type))
        self.conn.commit()

    def get_due_pending_events(self, limit: int = 20):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, guild_name, family_name, event_type, first_detected_at,
                   confirm_after, last_seen_at, source_url
            FROM pending_events
            WHERE confirm_after <= ?
            ORDER BY confirm_after ASC
            LIMIT ?
        """, (utc_now_text(), limit))
        return cur.fetchall()

    def delete_pending_by_id(self, pending_id: int):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM pending_events WHERE id = ?", (pending_id,))
        self.conn.commit()

    def cleanup_expired_pending_events(self):
        cutoff = (utc_now() - timedelta(minutes=PENDING_EXPIRE_MINUTES)).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute("""
            DELETE FROM pending_events
            WHERE first_detected_at <= ?
        """, (cutoff,))
        self.conn.commit()

    def list_pending_events(self, limit: int = 30):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, guild_name, family_name, event_type, first_detected_at,
                   confirm_after, last_seen_at
            FROM pending_events
            ORDER BY confirm_after ASC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()

db = Database(DB_PATH)


class HttpClient:
    def __init__(self):
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        )
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def close(self):
        await self.session.close()

    async def get_text(self, url: str) -> str:
        async with self.sem:
            last_error = None
            for _ in range(REQUEST_RETRY_COUNT):
                try:
                    async with self.session.get(url) as resp:
                        resp.raise_for_status()
                        return await resp.text()
                except Exception as e:
                    last_error = e
                    await asyncio.sleep(1.0)
            raise last_error


http_client: Optional[HttpClient] = None


def make_embed(title: str, description: Optional[str] = None, color: int = 0x5865F2) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color)
    embed.set_footer(text="BDO KR Watch Rename Guard Bot")
    return embed


def get_channel_name(interaction: discord.Interaction) -> str:
    return getattr(interaction.channel, "name", "") or ""


def is_track_channel(interaction: discord.Interaction) -> bool:
    return get_channel_name(interaction) == TRACK_CHANNEL_NAME


def is_admin_channel(interaction: discord.Interaction) -> bool:
    return get_channel_name(interaction) == ADMIN_CHANNEL_NAME


async def require_track_channel(interaction: discord.Interaction) -> bool:
    if is_track_channel(interaction):
        return True
    await interaction.response.send_message(
        f"❌ 이 명령어는 `#{TRACK_CHANNEL_NAME}` 채널에서만 사용할 수 있습니다.",
        ephemeral=True
    )
    return False


async def require_admin_channel(interaction: discord.Interaction) -> bool:
    if is_admin_channel(interaction):
        return True
    await interaction.response.send_message(
        f"❌ 이 명령어는 `#{ADMIN_CHANNEL_NAME}` 채널에서만 사용할 수 있습니다.",
        ephemeral=True
    )
    return False


def get_channel_name(interaction: discord.Interaction) -> str:
    return getattr(interaction.channel, "name", "") or ""


def is_track_channel(interaction: discord.Interaction) -> bool:
    return get_channel_name(interaction) == TRACK_CHANNEL_NAME


def is_admin_channel(interaction: discord.Interaction) -> bool:
    return get_channel_name(interaction) == ADMIN_CHANNEL_NAME


async def require_track_channel(interaction: discord.Interaction) -> bool:
    if is_track_channel(interaction):
        return True
    await interaction.response.send_message(
        f"❌ 이 명령어는 `#{TRACK_CHANNEL_NAME}` 채널에서만 사용할 수 있습니다.",
        ephemeral=True
    )
    return False


async def require_admin_channel(interaction: discord.Interaction) -> bool:
    if is_admin_channel(interaction):
        return True
    await interaction.response.send_message(
        f"❌ 이 명령어는 `#{ADMIN_CHANNEL_NAME}` 채널에서만 사용할 수 있습니다.",
        ephemeral=True
    )
    return False


def extract_members_from_guild_html(html: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")

    # 차단 페이지면 실패
    block_keywords = ("Incapsula", "Request unsuccessful", "incident ID", "NOINDEX, NOFOLLOW")
    if any(k in html for k in block_keywords):
        return set()

    members = []
    seen = set()

    def add_name(value: str):
        value = normalize_name(value)
        value = re.sub(r"\s+대장$", "", value).strip()

        if not value:
            return
        if value in {"가문명", "구성원", "길드원", "대장", "비공개"}:
            return
        if value.isdigit():
            return
        if not re.fullmatch(r"[0-9A-Za-z가-힣_]{2,20}", value):
            return

        if value not in seen:
            seen.add(value)
            members.append(value)

    # 공홈 실제 구조:
    # <div class="box_list_area">
    #   <ul class="adventure_list_table">
    #     <div class="guild_name"><a href="/Adventure/Profile?...">가문명</a>
    # 이 링크만 뽑는다.
    selectors = [
        ".box_list_area .adventure_list_table .guild_name a",
        ".box_list_area a[href*='/Adventure/Profile']",
        ".box_list_area a[href*='Adventure/Profile']",
    ]

    for sel in selectors:
        for a in soup.select(sel):
            add_name(a.get_text(" ", strip=True))

    # 보강: 정확한 구조가 약간 바뀌었을 때도 Adventure/Profile 링크만 사용
    if not members:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "Adventure/Profile" in href:
                add_name(a.get_text(" ", strip=True))

    return set(members)



def anti_block_delay_sync():
    delay = random.uniform(ANTI_BLOCK_MIN_DELAY_SECONDS, ANTI_BLOCK_MAX_DELAY_SECONDS)
    time.sleep(delay)


def is_night_slowdown_now() -> bool:
    if not NIGHT_SLOWDOWN_ENABLED:
        return False
    hour = datetime.now(timezone(timedelta(hours=9))).hour
    if NIGHT_SLOWDOWN_START_HOUR <= NIGHT_SLOWDOWN_END_HOUR:
        return NIGHT_SLOWDOWN_START_HOUR <= hour < NIGHT_SLOWDOWN_END_HOUR
    return hour >= NIGHT_SLOWDOWN_START_HOUR or hour < NIGHT_SLOWDOWN_END_HOUR


def effective_interval_minutes(base_minutes: int) -> int:
    if is_night_slowdown_now():
        return max(1, int(base_minutes * NIGHT_SLOWDOWN_MULTIPLIER))
    return base_minutes


_playwright = None
_browser = None
_page = None
_browser_lock = asyncio.Lock()


async def await restart_browser():
    global _playwright, _browser, _page
    try:
        if _page is not None:
            await _page.close()
    except Exception:
        pass
    try:
        if _browser is not None:
            await _browser.close()
    except Exception:
        pass
    try:
        if _playwright is not None:
            await _playwright.stop()
    except Exception:
        pass
    _page = None
    _browser = None
    _playwright = None


async def get_browser_page():
    global _playwright, _browser, _page

    if _page is not None:
        return _page

    _playwright = await aasync_playwright().start()

    launch_args = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-software-rasterizer",
    ]

    _browser = await _playwright.chromium.launch(
        headless=True,
        args=launch_args,
    )

    context = await _browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1400, "height": 1000},
        locale="ko-KR",
    )

    _page = await context.new_page()
    await _page.set_viewport_size({"width": 1400, "height": 1000})

    return _page


async async def fetch_guild_html_browser(guild_name: str) -> tuple[str, str]:
    url = build_guild_url(guild_name)

    async with _browser_lock:
        anti_block_delay_sync()

        try:
            page = await get_browser_page()

            await await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=BROWSER_PAGE_TIMEOUT * 1000,
            )

            try:
                await await page.wait_for_selector(
                    ".box_list_area",
                    timeout=BROWSER_WAIT_SECONDS * 1000,
                )
            except PlaywrightTimeoutError:
                pass

            await asyncio.sleep(BROWSER_COOLDOWN_SECONDS)
            html = await await page.content()

        except Exception:
            await await restart_browser()

            page = await get_browser_page()

            await await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=BROWSER_PAGE_TIMEOUT * 1000,
            )

            await asyncio.sleep(max(BROWSER_COOLDOWN_SECONDS, 0.5))
            html = await await page.content()

        return html, url


async def fetch_guild_members(guild_name: str) -> GuildScanResult:
    html, url = await asyncio.to_thread(fetch_guild_html_browser_sync, guild_name)
    members = extract_members_from_guild_html(html)

    return GuildScanResult(
        guild_name=guild_name,
        members=members,
        source_url=url,
        fetched_at=utc_now_text(),
    )


def score_rename_candidate(old_name: str, new_name: str, leave_detected_at: str):
    score = 25
    reasons = ["같은 길드"]

    try:
        mins = (utc_now() - parse_iso(leave_detected_at)).total_seconds() / 60
        if mins <= 5:
            score += 20
            reasons.append("5분 안")
        elif mins <= 15:
            score += 12
            reasons.append("15분 안")
        elif mins <= RENAME_WINDOW_MINUTES:
            score += 5
            reasons.append("30분 안")
    except Exception:
        pass

    if score >= 40:
        confidence = "중간"
    else:
        confidence = "낮음"
    return score, confidence, ", ".join(reasons)


async def detect_rename_suspicions(guild_name: str, joined: list[str]):
    if not joined:
        return []

    recent_leaves = db.get_recent_leave_events(guild_name, RENAME_WINDOW_MINUTES, 30)
    if not recent_leaves:
        return []

    found = []
    used_new = set()

    for leave in recent_leaves:
        old_name = leave["family_name"]
        best = None

        for new_name in joined:
            if new_name == old_name or new_name in used_new:
                continue

            score, confidence, reason = score_rename_candidate(
                old_name, new_name, leave["detected_at"]
            )
            if score < 40:
                continue
            if best is None or score > best["score"]:
                best = {
                    "old_name": old_name,
                    "new_name": new_name,
                    "confidence": confidence,
                    "score": score,
                    "reason": reason,
                }

        if best:
            used_new.add(best["new_name"])
            db.insert_rename_suspicion(
                best["old_name"], best["new_name"], guild_name,
                best["confidence"], best["score"], best["reason"]
            )
            found.append(best)

    return found


async def scan_one_guild(guild_name: str, interval_min: int):
    previous = db.get_latest_snapshot_members(guild_name)
    is_first_scan = len(previous) == 0

    result = await fetch_guild_members(guild_name)

    if len(result.members) < MIN_VALID_MEMBER_COUNT:
        raise RuntimeError("구성원 0명으로 읽힘 - 저장 안 함 / 전원 탈퇴 오기록 방지")

    raw_joined = sorted(result.members - previous)
    raw_left = sorted(previous - result.members)

    if previous:
        changed_count = len(raw_joined) + len(raw_left)
        ratio = changed_count / max(len(previous), 1)
        if ratio >= MAX_CHANGE_RATIO:
            raise RuntimeError(
                f"이상 변화율 감지: 변경 {changed_count}명 / 기존 {len(previous)}명 / 비율 {ratio:.0%} - 저장 중단"
            )

    # 최신 스냅샷은 저장하되, 가입/탈퇴 이벤트는 5분 후 재검증해서 확정한다.
    db.create_snapshot(guild_name, result.members, result.source_url)

    effective_joined = []
    effective_left = []

    if not is_first_scan:
        # 다시 보이면 탈퇴 후보 취소
        for family_name in result.members:
            db.delete_pending_event(guild_name, family_name, "leave")

        # 다시 사라지면 가입 후보 취소
        for family_name in previous - result.members:
            db.delete_pending_event(guild_name, family_name, "join")

        # 새로 보이는 사람은 가입 후보로 등록
        for family_name in raw_joined:
            if db.get_latest_event_type(guild_name, family_name) != "join":
                db.upsert_pending_event(guild_name, family_name, "join", result.source_url)

        # 안 보이는 사람은 탈퇴 후보로 등록
        for family_name in raw_left:
            if db.get_latest_event_type(guild_name, family_name) != "leave":
                db.upsert_pending_event(guild_name, family_name, "leave", result.source_url)

    rename_suspicions = [] if is_first_scan else await detect_rename_suspicions(guild_name, effective_joined)
    db.mark_scan_success(guild_name, interval_min)

    return {
        "guild_name": guild_name,
        "member_count": len(result.members),
        "joined": effective_joined,
        "left": effective_left,
        "rename_suspicions": rename_suspicions,
        "pending_joined": [] if is_first_scan else raw_joined,
        "pending_left": [] if is_first_scan else raw_left,
        "fetched_at": result.fetched_at,
        "first_scan": is_first_scan,
    }


intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)
SCAN_PAUSED = False


def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            raise app_commands.CheckFailure("서버 안에서만 사용할 수 있습니다.")
        perms = interaction.user.guild_permissions
        return perms.administrator or perms.manage_guild
    return app_commands.check(predicate)


@bot.event
async def on_ready():
    global http_client
    if http_client is None:
        http_client = HttpClient()
    log.info("봇 로그인 완료: %s", bot.user)
    synced = await bot.tree.sync()
    log.info("슬래시 명령어 동기화 완료: %s개", len(synced))
    if not scheduler_loop.is_running():
        scheduler_loop.start()


async def notify_watched_family_changes(summary: dict):
    watched = db.get_watched_family_names()
    if not watched:
        return

    joined = [x for x in summary.get("joined", []) if x in watched]
    left = [x for x in summary.get("left", []) if x in watched]

    if not joined and not left:
        return

    for discord_guild in bot.guilds:
        channel_id = db.get_notify_channel(discord_guild.id)
        if not channel_id:
            continue

        channel = discord_guild.get_channel(channel_id)
        if channel is None:
            continue

        embed = make_embed(
            title=f"🔔 관심 가문 변동 · {summary['guild_name']}",
            description=f"현재 인원: **{summary['member_count']}명**\n감지 시각: {kst_display(summary['fetched_at'])}",
            color=0xE91E63,
        )

        if joined:
            embed.add_field(
                name=f"가입 {len(joined)}명",
                value=limit_text([f"🟢 `{x}`" for x in joined], 1000),
                inline=False
            )

        if left:
            embed.add_field(
                name=f"탈퇴 {len(left)}명",
                value=limit_text([f"🔴 `{x}`" for x in left], 1000),
                inline=False
            )

        await channel.send(embed=embed)


async def notify_changes(summary: dict):
    for discord_guild in bot.guilds:
        channel_id = db.get_notify_channel(discord_guild.id)
        if not channel_id:
            continue
        channel = discord_guild.get_channel(channel_id)
        if channel is None:
            continue

        joined = summary["joined"]
        left = summary["left"]
        rename_suspicions = summary.get("rename_suspicions") or []

        color = 0x9B59B6 if rename_suspicions else 0x2ECC71 if joined and not left else 0xE74C3C if left and not joined else 0xF39C12

        embed = make_embed(
            title=f"📌 길드 변화 · {summary['guild_name']}",
            description=f"현재 인원: **{summary['member_count']}명**\n감지 시각: {kst_display(summary['fetched_at'])}",
            color=color,
        )

        embed.add_field(
            name=f"가입 {len(joined)}명",
            value=limit_text([f"🟢 `{x}`" for x in joined[:15]]) if joined else "-",
            inline=False
        )
        embed.add_field(
            name=f"탈퇴 {len(left)}명",
            value=limit_text([f"🔴 `{x}`" for x in left[:15]]) if left else "-",
            inline=False
        )

        if rename_suspicions:
            rename_lines = []
            for x in rename_suspicions[:10]:
                old_name = x.get("old_name") or x.get("old_family_name") or "?"
                new_name = x.get("new_name") or x.get("new_family_name") or "?"
                confidence = x.get("confidence", "?")
                rename_lines.append(f"🟣 `{old_name}` → `{new_name}` | 추정도 {confidence}")

            embed.add_field(
                name=f"가문명 변경 추정 {len(rename_suspicions)}건",
                value=limit_text(rename_lines, 1200),
                inline=False
            )

        await channel.send(embed=embed)



def is_rename_alert_allowed_now():
    if not RENAME_ALERT_WEEKDAY_ONLY:
        return True

    try:
        now = datetime.now(KST)
    except NameError:
        from datetime import timezone, timedelta
        kst = timezone(timedelta(hours=9))
        now = datetime.now(kst)

    # Wednesday=2
    if now.weekday() != RENAME_ALERT_ALLOWED_WEEKDAY:
        return False

    # Allow only within configured hour range from midnight
    if now.hour >= RENAME_ALERT_WINDOW_HOURS:
        return False

    return True


def simple_name_similarity(a: str, b: str) -> float:
    a = normalize_name(a).lower()
    b = normalize_name(b).lower()
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio()


async def detect_rename_suspicions_for_pairs(guild_name: str, joined: list[str], left: list[str]):
    suspicions = []

    # 수요일 점검 반영 시간 외에는 가문명 변경추정 비활성
    if not is_rename_alert_allowed_now():
        return suspicions

    if not joined or not left:
        return suspicions

    for old_name in left:
        best = None
        best_score = 0.0

        for new_name in joined:
            score = simple_name_similarity(old_name, new_name)
            if score > best_score:
                best_score = score
                best = new_name

        if best and best_score >= 0.45:
            confidence = "높음" if best_score >= 0.75 else "중간" if best_score >= 0.60 else "낮음"

            try:
                if hasattr(db, "insert_rename_suspicion"):
                    db.insert_rename_suspicion(
                        guild_name=guild_name,
                        old_family_name=old_name,
                        new_family_name=best,
                        confidence=confidence,
                    )
            except Exception:
                pass

            suspicions.append({
                "old_family_name": old_name,
                "new_family_name": best,
                "old_name": old_name,
                "new_name": best,
                "confidence": confidence,
                "score": round(best_score, 3),
            })

    return suspicions


async def notify_confirmed_change(guild_name: str, member_count: int, joined: list[str], left: list[str], fetched_at: str):
    summary = {
        "guild_name": guild_name,
        "member_count": member_count,
        "joined": joined,
        "left": left,
        "rename_suspicions": [],
        "fetched_at": fetched_at,
        "first_scan": False,
    }
    await notify_changes(summary)


async def confirm_pending_events():
    db.cleanup_expired_pending_events()
    rows = db.get_due_pending_events(20)
    if not rows:
        return

    grouped = {}
    for r in rows:
        grouped.setdefault(r["guild_name"], []).append(r)

    for guild_name, pendings in grouped.items():
        info = db.get_watched_guild_info(guild_name) if hasattr(db, "get_watched_guild_info") else None
        interval_min = int(info["scan_interval_min"]) if info else DEFAULT_SCAN_INTERVAL_MIN

        try:
            result = await fetch_guild_members(guild_name)
            if len(result.members) < MIN_VALID_MEMBER_COUNT:
                raise RuntimeError("pending 확인 중 구성원 0명")

            db.create_snapshot(guild_name, result.members, result.source_url)
            db.mark_scan_success(guild_name, interval_min)

            confirmed_joined = []
            confirmed_left = []

            for p in pendings:
                pid = int(p["id"])
                family_name = p["family_name"]
                event_type = p["event_type"]

                if event_type == "join":
                    if family_name in result.members and db.get_latest_event_type(guild_name, family_name) != "join":
                        confirmed_joined.append(family_name)
                    db.delete_pending_by_id(pid)

                elif event_type == "leave":
                    if family_name not in result.members and db.get_latest_event_type(guild_name, family_name) != "leave":
                        confirmed_left.append(family_name)
                    db.delete_pending_by_id(pid)

            if confirmed_joined or confirmed_left:
                db.insert_events(guild_name, confirmed_joined, confirmed_left, result.source_url)

                rename_suspicions = []
                try:
                    rename_suspicions = await detect_rename_suspicions_for_pairs(
                        guild_name=guild_name,
                        joined=confirmed_joined,
                        left=confirmed_left,
                    )
                except Exception as e:
                    log.exception("가문명 변경 추정 실패 | 길드=%s | %s", guild_name, e)

                summary = {
                    "guild_name": guild_name,
                    "member_count": len(result.members),
                    "joined": confirmed_joined,
                    "left": confirmed_left,
                    "rename_suspicions": rename_suspicions,
                    "fetched_at": result.fetched_at,
                    "first_scan": False,
                }
                await notify_changes(summary)

        except Exception as e:
            db.mark_scan_failure(guild_name, f"pending 확인 실패: {e}")
            log.exception("pending 확인 실패 | 길드=%s | %s", guild_name, e)

    try:
        await notify_watched_family_changes(summary)
    except Exception as e:
        log.exception("관심가문 알림 실패 | %s", e)


@tasks.loop(seconds=SCAN_LOOP_SECONDS)
async def scheduler_loop():
    try:
        await confirm_pending_events()
    except Exception as e:
        log.exception("pending 확인 루프 실패 | %s", e)

    if SCAN_PAUSED:
        return

    rows = db.get_due_guilds(MAX_CONCURRENT_REQUESTS)
    if not rows:
        return

    jobs = [scan_one_guild(r["guild_name"], int(r["scan_interval_min"])) for r in rows]
    results = await asyncio.gather(*jobs, return_exceptions=True)

    for row, res in zip(rows, results):
        guild_name = row["guild_name"]
        if isinstance(res, Exception):
            db.mark_scan_failure(guild_name, str(res))
            log.exception("스캔 실패 | 길드=%s | %s", guild_name, res)
            continue
        log.info("스캔 완료 | 길드=%s 인원=%s 가입=%s 탈퇴=%s", guild_name, res["member_count"], len(res["joined"]), len(res["left"]))
        if res["joined"] or res["left"] or res["rename_suspicions"]:
            await notify_changes(res)


@scheduler_loop.before_loop
async def before_scheduler():
    await bot.wait_until_ready()


@bot.tree.command(name="채널설정", description="알림 채널을 정합니다.")
@admin_only()
@app_commands.describe(channel="알림 채널")
async def set_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    if not await require_admin_channel(interaction):
        return

    db.set_notify_channel(interaction.guild.id, channel.id)
    await interaction.response.send_message(f"알림 채널 설정 완료: {channel.mention}", ephemeral=True)


@bot.tree.command(name="길드추가", description="감시할 길드를 추가합니다.")
@admin_only()
@app_commands.describe(guild_name="길드명", priority="중요 길드면 체크", interval_min="스캔 간격(분)")
async def add_guild(interaction: discord.Interaction, guild_name: str, priority: bool = False, interval_min: Optional[int] = None):
    if not await require_admin_channel(interaction):
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    guild_name = guild_name.strip()
    chosen = interval_min or (PRIORITY_SCAN_INTERVAL_MIN if priority else DEFAULT_SCAN_INTERVAL_MIN)
    db.add_watched_guild(guild_name, 1 if priority else 0, chosen)

    last_error = None
    for attempt in range(FIRST_SCAN_RETRY_COUNT + 1):
        try:
            res = await scan_one_guild(guild_name, chosen)
            embed = make_embed("✅ 길드 추가 + 브라우저 스캔 완료", color=0x2ECC71)
            embed.add_field(name="길드명", value=f"`{guild_name}`", inline=True)
            embed.add_field(name="현재 인원", value=f"**{res['member_count']}명**", inline=True)
            embed.add_field(name="스캔 간격", value=f"{chosen}분", inline=True)
            embed.add_field(name="차단방지", value=f"{ANTI_BLOCK_MIN_DELAY_SECONDS:.0f}~{ANTI_BLOCK_MAX_DELAY_SECONDS:.0f}초 랜덤 대기 적용", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        except Exception as e:
            last_error = e
            if attempt < FIRST_SCAN_RETRY_COUNT:
                await asyncio.sleep(FIRST_SCAN_RETRY_DELAY_SECONDS)

    embed = make_embed("⚠️ 길드 추가 완료 / 브라우저 스캔 실패", color=0xE67E22)
    embed.add_field(name="길드명", value=f"`{guild_name}`", inline=True)
    embed.add_field(name="스캔 간격", value=f"{chosen}분", inline=True)
    embed.add_field(name="실패 이유", value=str(last_error)[:500], inline=False)
    embed.add_field(name="설명", value=f"길드는 등록됐습니다. 실패 길드는 {FAIL_BACKOFF_MINUTES}분 백오프 후 다시 시도합니다.", inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="길드제거", description="감시 길드를 뺍니다.")
@admin_only()
@app_commands.describe(guild_name="길드명")
async def remove_guild(interaction: discord.Interaction, guild_name: str):
    if not await require_admin_channel(interaction):
        return

    deleted = db.remove_watched_guild(guild_name.strip())
    if deleted:
        await interaction.response.send_message(f"제거 완료: `{guild_name}`", ephemeral=True)
    else:
        await interaction.response.send_message("등록된 길드가 없습니다.", ephemeral=True)


@bot.tree.command(name="중요길드설정", description="등록된 길드를 중요 길드로 바꾸고 10분 스캔으로 설정합니다.")
@admin_only()
@app_commands.describe(guild_name="길드명")
async def set_priority_guild(interaction: discord.Interaction, guild_name: str):
    if not await require_admin_channel(interaction):
        return

    guild_name = guild_name.strip()
    updated = db.set_guild_priority(guild_name, 1, PRIORITY_SCAN_INTERVAL_MIN)
    if updated:
        await interaction.response.send_message(f"✅ `{guild_name}` 중요 길드 설정 완료 / 스캔 간격 {PRIORITY_SCAN_INTERVAL_MIN}분", ephemeral=True)
    else:
        await interaction.response.send_message("등록된 길드가 없습니다. 먼저 `/길드추가` 를 해주세요.", ephemeral=True)


@bot.tree.command(name="일반길드설정", description="등록된 길드를 일반 길드로 바꾸고 15분 스캔으로 설정합니다.")
@admin_only()
@app_commands.describe(guild_name="길드명")
async def set_normal_guild(interaction: discord.Interaction, guild_name: str):
    if not await require_admin_channel(interaction):
        return

    guild_name = guild_name.strip()
    updated = db.set_guild_priority(guild_name, 0, DEFAULT_SCAN_INTERVAL_MIN)
    if updated:
        await interaction.response.send_message(f"✅ `{guild_name}` 일반 길드 설정 완료 / 스캔 간격 {DEFAULT_SCAN_INTERVAL_MIN}분", ephemeral=True)
    else:
        await interaction.response.send_message("등록된 길드가 없습니다. 먼저 `/길드추가` 를 해주세요.", ephemeral=True)


@bot.tree.command(name="길드목록", description="등록된 감시 길드 목록을 봅니다.")
async def list_guilds(interaction: discord.Interaction):
    if not await require_track_channel(interaction):
        return

    rows = db.list_watched_guilds()
    embed = make_embed("📋 감시 길드 목록", color=0x95A5A6)

    if not rows:
        embed.add_field(name="결과", value="등록된 길드 없음", inline=False)
    else:
        lines = []
        for r in rows:
            star = "⭐" if r["priority"] else "•"
            last = kst_date_only(r["last_success_at"]) if r["last_success_at"] else "미스캔"
            lines.append(f"{star} `{r['guild_name']}` | {r['scan_interval_min']}분 | {last}")

        chunks = chunk_lines_for_embed(lines, max_chars=900, max_chunks=20)
        embed.description = f"총 **{len(rows)}개** / 길면 나눠서 표시합니다."

        for i, chunk in enumerate(chunks, start=1):
            embed.add_field(
                name=f"목록 {i}/{len(chunks)}",
                value=chunk or "-",
                inline=False
            )

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="즉시스캔", description="지금 바로 스캔합니다.")
@admin_only()
@app_commands.describe(limit="최대 길드 수")
async def manual_scan(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 10] = 5):
    if not await require_admin_channel(interaction):
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    rows = db.list_watched_guilds()[:limit]
    if not rows:
        await interaction.followup.send("등록된 길드가 없습니다.", ephemeral=True)
        return

    jobs = [scan_one_guild(r["guild_name"], int(r["scan_interval_min"])) for r in rows]
    results = await asyncio.gather(*jobs, return_exceptions=True)

    embeds = []
    for row, res in zip(rows, results):
        if isinstance(res, Exception):
            db.mark_scan_failure(row["guild_name"], str(res))
            embeds.append(make_embed("스캔 실패", f"{row['guild_name']} | {res}", color=0xE74C3C))
            continue

        embed = make_embed(f"즉시 스캔 · {res['guild_name']}", f"현재 인원: **{res['member_count']}명**", color=0x95A5A6)
        embed.add_field(name="가입", value="\n".join(f"🟢 `{x}`" for x in res["joined"][:15]) or "-", inline=False)
        embed.add_field(name="탈퇴", value="\n".join(f"🔴 `{x}`" for x in res["left"][:15]) or "-", inline=False)
        embeds.append(embed)

    await interaction.followup.send(embeds=embeds[:10], ephemeral=True)


@bot.tree.command(name="가문추적", description="가문 기록과 현재 스냅샷 기준 소속 길드를 봅니다.")
@app_commands.describe(family_name="가문명")
async def family_track(interaction: discord.Interaction, family_name: str):
    if not await require_track_channel(interaction):
        return

    family_name = family_name.strip()

    current_guild = db.get_family_current_guild_guess(family_name)
    snapshot_rows = db.find_family_current_guilds_from_snapshots(family_name, 10)
    recent_guilds = db.get_family_recent_guilds(family_name, 10)
    rename_rows = db.get_family_rename_suspicions(family_name, 5)

    embed = make_embed("👤 가문 추적 결과", color=0x9B59B6)
    embed.add_field(name="가문명", value=f"`{family_name}`", inline=True)

    # 이벤트 기반 현재 길드가 있으면 우선 표시.
    # 이벤트가 없으면 최신 스냅샷에서 현재 소속 길드를 찾아 표시.
    if current_guild:
        current_text = f"`{current_guild}`"
    elif snapshot_rows:
        current_text = "\n".join([
            f"• `{r['guild_name']}` | 최근스캔 {kst_date_only(r['scanned_at'])}"
            for r in snapshot_rows[:5]
        ])
    else:
        current_text = "기록 없음"

    embed.add_field(name="현재 길드", value=current_text, inline=False)

    prev_lines = []
    for guild_name, detected_at, event_type in recent_guilds:
        emoji = "🟢" if event_type == "join" else "🔴"
        prev_lines.append(f"{emoji} `{guild_name}` | {kst_date_only(detected_at)}")

    embed.add_field(
        name="이동/변동 기록",
        value=limit_text(prev_lines) if prev_lines else "변동 기록 없음",
        inline=False
    )

    rename_lines = []
    for r in rename_rows:
        rename_lines.append(f"[{r['confidence']}] `{r['old_family_name']}` → `{r['new_family_name']}`")

    embed.add_field(
        name="가문명 변경 추정",
        value=limit_text(rename_lines) if rename_lines else "기록 없음",
        inline=False
    )

    embed.add_field(
        name="설명",
        value="현재 길드는 최신 스냅샷 기준입니다. 이동/변동 기록은 가입·탈퇴 이벤트가 쌓인 뒤 표시됩니다.",
        inline=False
    )

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="길드연혁", description="길드 연혁을 봅니다.")
@app_commands.describe(guild_name="길드명")
async def guild_history(interaction: discord.Interaction, guild_name: str):
    # 길드연혁은 반드시 추적 채널에서만 사용
    if not await require_track_channel(interaction):
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    guild_name = guild_name.strip()
    member_count = db.get_current_guild_member_count(guild_name)
    rows = db.get_guild_recent_events(guild_name, MAX_HISTORY_LINES)

    embed = make_embed("📜 길드 연혁", color=0xF39C12)
    embed.add_field(name="길드명", value=f"`{guild_name}`", inline=True)
    embed.add_field(name="현재 인원", value=f"**{member_count}명**", inline=True)

    lines = []
    for r in rows:
        emoji = "🟢" if r["event_type"] == "join" else "🔴"
        lines.append(f"{emoji} `{r['family_name']}` | {kst_date_only(r['detected_at'])}")

    embed.add_field(
        name=f"최근 가입 / 탈퇴 {len(rows)}개",
        value=limit_text(lines) if lines else "기록 없음",
        inline=False
    )

    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="파싱테스트", description="저장된 debug HTML 기준으로 파싱 인원을 확인합니다.")
@app_commands.describe(guild_name="길드명")
async def parse_test(interaction: discord.Interaction, guild_name: str):
    if not await require_admin_channel(interaction):
        return

    safe_name = re.sub(r"[^0-9A-Za-z가-힣_-]+", "_", guild_name.strip())
    path = Path("debug_pages") / f"debug_{safe_name}.html"

    if not path.exists():
        await interaction.response.send_message(f"디버그 파일 없음: `{path}`", ephemeral=True)
        return

    html = path.read_text(encoding="utf-8", errors="ignore")
    members = extract_members_from_guild_html(html)
    sample = sorted(list(members))[:20]

    embed = make_embed("🔎 파싱 테스트", color=0x3498DB)
    embed.add_field(name="길드명", value=f"`{guild_name}`", inline=True)
    embed.add_field(name="파싱 인원", value=f"**{len(members)}명**", inline=True)
    embed.add_field(name="샘플", value=limit_text([f"• `{x}`" for x in sample]) if sample else "없음", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)



@bot.tree.command(name="길드정보", description="길드 현재 구성원 전체를 봅니다.")
@app_commands.describe(guild_name="길드명")
async def guild_info(interaction: discord.Interaction, guild_name: str):
    # 길드정보는 일반 추적 채널에서 사용
    if not await require_track_channel(interaction):
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    guild_name = guild_name.strip()

    # 조회 시 최신화: 5분 이상 오래된 경우 해당 길드만 즉시 스캔
    if 'refresh_guild_if_stale' in globals():
        refreshed, res, err = await refresh_guild_if_stale(guild_name)
    else:
        refreshed, res, err = False, None, None

    info = db.get_watched_guild_info(guild_name) if hasattr(db, "get_watched_guild_info") else None
    member_count = db.get_current_guild_member_count(guild_name)
    members = db.get_snapshot_members_all(guild_name)

    if not info and member_count == 0 and not members:
        await interaction.followup.send("등록된 길드 정보가 없습니다.", ephemeral=True)
        return

    embed = make_embed("🏰 길드 정보", color=0x3498DB)
    embed.add_field(name="길드명", value=f"`{guild_name}`", inline=True)
    embed.add_field(name="현재 인원", value=f"**{member_count or len(members)}명**", inline=True)

    if info:
        embed.add_field(name="스캔 간격", value=f"{info['scan_interval_min']}분", inline=True)
        embed.add_field(name="마지막 성공", value=kst_display(info["last_success_at"]), inline=False)

    embed.add_field(
        name="최신화",
        value="방금 스캔" if refreshed else ("실패: " + err[:80] if err else "최근 스캔 사용"),
        inline=False
    )

    # 최근변화는 제외하고 현재 구성원만 표시
    add_member_fields_to_embed(embed, members, per_field=20, max_fields=20)

    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="강제스캔", description="특정 길드만 즉시 스캔합니다.")
@admin_only()
@app_commands.describe(guild_name="길드명")
async def force_scan(interaction: discord.Interaction, guild_name: str):
    if not await require_admin_channel(interaction):
        return

    await interaction.response.defer(ephemeral=True, thinking=True)
    info = db.get_watched_guild_info(guild_name.strip())
    if not info:
        await interaction.followup.send("등록된 길드가 없습니다.", ephemeral=True)
        return

    try:
        res = await scan_one_guild(info["guild_name"], int(info["scan_interval_min"]))
        embed = make_embed(f"🔄 강제 스캔 · {res['guild_name']}", color=0x1ABC9C)
        embed.add_field(name="현재 인원", value=f"**{res['member_count']}명**", inline=True)
        embed.add_field(name="가입", value=limit_text([f"🟢 `{x}`" for x in res["joined"]]) if res["joined"] else "-", inline=False)
        embed.add_field(name="탈퇴", value=limit_text([f"🔴 `{x}`" for x in res["left"]]) if res["left"] else "-", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        db.mark_scan_failure(info["guild_name"], str(e))
        await interaction.followup.send(f"스캔 실패: `{info['guild_name']}` | {e}", ephemeral=True)


@bot.tree.command(name="미스캔목록", description="아직 스캔 성공 기록이 없는 길드 목록을 봅니다.")
@admin_only()
async def unscanned_list(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    guilds = db.get_unscanned_guilds()
    if not guilds:
        await interaction.response.send_message("✅ 미스캔 길드 없음", ephemeral=True)
        return

    embed = make_embed("⚠️ 미스캔 길드", color=0xE67E22)
    embed.add_field(name=f"{len(guilds)}개", value=limit_text([f"• `{g}`" for g in guilds[:50]], 3500), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="스캔주기변경", description="특정 길드의 스캔 주기를 변경합니다.")
@admin_only()
@app_commands.describe(guild_name="길드명", interval_min="분 단위")
async def change_scan_interval(interaction: discord.Interaction, guild_name: str, interval_min: app_commands.Range[int, 5, 240]):
    if not await require_admin_channel(interaction):
        return

    updated = db.set_scan_interval(guild_name.strip(), int(interval_min))
    if updated:
        await interaction.response.send_message(f"✅ `{guild_name}` 스캔 주기 {interval_min}분으로 변경", ephemeral=True)
    else:
        await interaction.response.send_message("등록된 길드가 없습니다.", ephemeral=True)


@bot.tree.command(name="중요길드목록", description="중요 길드 목록을 봅니다.")
@admin_only()
async def priority_guild_list(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    rows = [r for r in db.list_watched_guilds() if r["priority"]]
    embed = make_embed("⭐ 중요 길드 목록", color=0xF1C40F)
    embed.add_field(name=f"{len(rows)}개", value=limit_text([f"• `{r['guild_name']}` | {r['scan_interval_min']}분" for r in rows], 3500) if rows else "없음", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="일반길드목록", description="일반 길드 목록을 봅니다.")
@admin_only()
async def normal_guild_list(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    rows = [r for r in db.list_watched_guilds() if not r["priority"]]
    embed = make_embed("📋 일반 길드 목록", color=0x95A5A6)
    embed.add_field(name=f"{len(rows)}개", value=limit_text([f"• `{r['guild_name']}` | {r['scan_interval_min']}분" for r in rows[:80]], 3500) if rows else "없음", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="가문현재", description="가문의 현재 소속 길드만 빠르게 봅니다.")
@app_commands.describe(family_name="가문명")
async def family_current(interaction: discord.Interaction, family_name: str):
    if not await require_track_channel(interaction):
        return

    rows = db.find_family_current_guilds_from_snapshots(family_name.strip(), 10)
    embed = make_embed("👤 가문 현재 길드", color=0x9B59B6)
    embed.add_field(name="가문명", value=f"`{family_name.strip()}`", inline=True)
    if rows:
        embed.add_field(name="현재 스냅샷 기준", value=limit_text([f"• `{r['guild_name']}` | {kst_date_only(r['scanned_at'])}" for r in rows]), inline=False)
    else:
        embed.add_field(name="현재 스냅샷 기준", value="기록 없음", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="최근변화", description="최근 몇 시간 동안의 전체 가입/탈퇴를 봅니다.")
@admin_only()
@app_commands.describe(hours="조회 시간")
async def recent_changes(interaction: discord.Interaction, hours: app_commands.Range[int, 1, 72] = 2):
    if not await require_admin_channel(interaction):
        return

    rows = db.get_recent_events_all(int(hours), 60)
    embed = make_embed(f"🕒 최근 변화 · {hours}시간", color=0x3498DB)
    lines = []
    for r in rows:
        emoji = "🟢" if r["event_type"] == "join" else "🔴"
        lines.append(f"{emoji} `{r['guild_name']}` · `{r['family_name']}` | {kst_display(r['detected_at'], short=True) if 'short' in kst_display.__code__.co_varnames else kst_date_only(r['detected_at'])}")
    embed.add_field(name=f"{len(rows)}건", value=limit_text(lines, 3500) if lines else "기록 없음", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="길드변화", description="특정 길드의 최근 변화를 봅니다.")
@admin_only()
@app_commands.describe(guild_name="길드명", days="조회 일수")
async def guild_changes(interaction: discord.Interaction, guild_name: str, days: app_commands.Range[int, 1, 30] = 7):
    if not await require_admin_channel(interaction):
        return

    rows = db.get_recent_events_for_guild(guild_name.strip(), int(days), 60)
    embed = make_embed(f"📜 길드 변화 · {guild_name}", color=0xF39C12)
    lines = []
    for r in rows:
        emoji = "🟢" if r["event_type"] == "join" else "🔴"
        lines.append(f"{emoji} `{r['family_name']}` | {kst_date_only(r['detected_at'])}")
    embed.add_field(name=f"최근 {days}일 / {len(rows)}건", value=limit_text(lines, 3500) if lines else "기록 없음", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="디버그목록", description="최근 디버그 HTML 파일 목록을 봅니다.")
@admin_only()
async def debug_list(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    p = Path("debug_pages")
    files = sorted(p.glob("*.html"), key=lambda x: x.stat().st_mtime, reverse=True)[:15] if p.exists() else []
    if not files:
        await interaction.response.send_message("디버그 파일 없음", ephemeral=True)
        return

    embed = make_embed("🧪 디버그 파일 목록", color=0xE67E22)
    embed.add_field(name="최근 파일", value=limit_text([f"• `{f.name}`" for f in files], 3500), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="디버그로그", description="디버그 오류 로그를 봅니다.")
@admin_only()
async def debug_log(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    p = Path("debug_pages") / "error_log.txt"
    if not p.exists():
        await interaction.response.send_message("디버그 로그 없음", ephemeral=True)
        return

    text = p.read_text(encoding="utf-8", errors="ignore")[-1800:]
    await interaction.response.send_message(f"```{text}```", ephemeral=True)


@bot.tree.command(name="스캔정지", description="자동 스캔을 일시 정지합니다.")
@admin_only()
async def pause_scan(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    global SCAN_PAUSED
    SCAN_PAUSED = True
    await interaction.response.send_message("⏸️ 자동 스캔 정지됨", ephemeral=True)


@bot.tree.command(name="스캔재개", description="자동 스캔을 다시 시작합니다.")
@admin_only()
async def resume_scan(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    global SCAN_PAUSED
    SCAN_PAUSED = False
    await interaction.response.send_message("▶️ 자동 스캔 재개됨", ephemeral=True)


@bot.tree.command(name="설정확인", description="현재 운영 설정값을 봅니다.")
@admin_only()
async def config_view(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    embed = make_embed("⚙️ 설정 확인", color=0x34495E)
    embed.add_field(name="스캔 주기", value=f"일반 {DEFAULT_SCAN_INTERVAL_MIN}분 / 중요 {PRIORITY_SCAN_INTERVAL_MIN}분", inline=False)
    embed.add_field(name="차단방지 대기", value=f"{ANTI_BLOCK_MIN_DELAY_SECONDS}~{ANTI_BLOCK_MAX_DELAY_SECONDS}초", inline=False)
    embed.add_field(name="실패 백오프", value=f"{FAIL_BACKOFF_MINUTES}분", inline=True)
    embed.add_field(name="중복 방지", value=f"{DUPLICATE_EVENT_COOLDOWN_HOURS}시간", inline=True)
    embed.add_field(name="자동 스캔", value="정지" if SCAN_PAUSED else "실행중", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="길드랭킹", description="최근 변동 많은 길드 순위를 봅니다.")
@admin_only()
@app_commands.describe(days="조회 일수")
async def guild_ranking(interaction: discord.Interaction, days: app_commands.Range[int, 1, 30] = 7):
    if not await require_admin_channel(interaction):
        return

    rows = db.get_event_rank_by_guild(int(days), 15)
    embed = make_embed(f"🏆 변동 많은 길드 TOP {len(rows)}", color=0xF1C40F)
    embed.add_field(name=f"최근 {days}일", value=limit_text([f"{i+1}. `{r['guild_name']}` · {r['cnt']}건" for i, r in enumerate(rows)]) if rows else "기록 없음", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="활동량", description="최근 이동/변동 많은 가문 순위를 봅니다.")
@admin_only()
@app_commands.describe(days="조회 일수")
async def activity_ranking(interaction: discord.Interaction, days: app_commands.Range[int, 1, 30] = 7):
    if not await require_admin_channel(interaction):
        return

    rows = db.get_activity_rank_by_family(int(days), 15)
    embed = make_embed(f"📈 가문 활동량 TOP {len(rows)}", color=0x9B59B6)
    embed.add_field(name=f"최근 {days}일", value=limit_text([f"{i+1}. `{r['family_name']}` · {r['cnt']}건" for i, r in enumerate(rows)]) if rows else "기록 없음", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="가문명변경추정", description="최근 가문명 변경 추정 기록을 봅니다.")
@admin_only()
@app_commands.describe(family_name="가문명")
async def rename_suspicion_check(interaction: discord.Interaction, family_name: str):
    if not await require_admin_channel(interaction):
        return

    rows = db.get_family_rename_suspicions(family_name.strip(), 10)

    embed = make_embed("🟣 가문명 변경 추정", color=0x9B59B6)
    embed.add_field(name="검색 가문명", value=f"`{family_name.strip()}`", inline=True)

    if rows:
        lines = []
        for r in rows:
            old_name = r["old_family_name"] if "old_family_name" in r.keys() else r.get("old_name", "?")
            new_name = r["new_family_name"] if "new_family_name" in r.keys() else r.get("new_name", "?")
            confidence = r["confidence"] if "confidence" in r.keys() else "?"
            lines.append(f"🟣 `{old_name}` → `{new_name}` | 추정도 {confidence}")
        embed.add_field(name=f"{len(rows)}건", value=limit_text(lines, 1200), inline=False)
    else:
        embed.add_field(name="결과", value="기록 없음", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="pending목록", description="5분 재검증 대기 중인 가입/탈퇴 후보를 봅니다.")
@admin_only()
async def pending_list(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    rows = db.list_pending_events(50)
    embed = make_embed("⏳ Pending 이벤트 목록", color=0xE67E22)

    if not rows:
        embed.add_field(name="결과", value="대기 중인 이벤트 없음", inline=False)
    else:
        lines = []
        for r in rows:
            emoji = "🟢" if r["event_type"] == "join" else "🔴"
            lines.append(
                f"{emoji} `{r['guild_name']}` · `{r['family_name']}` | 확인예정 {kst_display(r['confirm_after'])}"
            )
        embed.add_field(name=f"{len(rows)}건", value=limit_text(lines, 3500), inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="우선스캔목록", description="특정 스캔주기 길드 목록을 봅니다. 기본값은 15분입니다.")
@admin_only()
@app_commands.describe(interval="확인할 스캔주기. 기본 15분")
async def priority_scan_list(interaction: discord.Interaction, interval: app_commands.Range[int, 5, 240] = 15):
    if not await require_admin_channel(interaction):
        return

    rows = db.get_priority_guilds(int(interval))

    embed = make_embed(f"⚡ {interval}분 스캔 길드 목록", color=0xF1C40F)

    if not rows:
        embed.add_field(name="결과", value=f"{interval}분 스캔 길드 없음", inline=False)
    else:
        lines = [f"• `{r['guild_name']}` | {r['scan_interval_min']}분" for r in rows]
        chunks = chunk_lines_for_embed(lines, max_chars=900, max_chunks=8)

        for i, chunk in enumerate(chunks, start=1):
            embed.add_field(
                name=f"목록 {i}/{len(chunks)}" if len(chunks) > 1 else f"총 {len(rows)}개",
                value=chunk or "-",
                inline=False
            )

        if len(chunks) > 1:
            embed.description = f"총 **{len(rows)}개** / 디스코드 제한 때문에 나눠서 표시합니다."

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="우선스캔변경", description="여러 길드 스캔주기를 쉼표 목록으로 한번에 변경합니다.")
@admin_only()
@app_commands.describe(
    interval="변경할 분 단위",
    guilds="쉼표(,)로 구분. 예: 길드A,길드B,길드C"
)
async def bulk_change_scan_interval(
    interaction: discord.Interaction,
    interval: app_commands.Range[int, 10, 240],
    guilds: str
):
    if not await require_admin_channel(interaction):
        return

    guild_list = [g.strip() for g in guilds.split(",") if g.strip()]
    if not guild_list:
        await interaction.response.send_message("길드 목록이 비어있습니다.", ephemeral=True)
        return

    updated, not_found = db.bulk_update_scan_interval(guild_list, int(interval))

    embed = make_embed("⚙️ 스캔주기 일괄 변경 완료", color=0x2ECC71)
    embed.add_field(name="변경 주기", value=f"{interval}분", inline=True)
    embed.add_field(name="변경 성공", value=f"{len(updated)}개", inline=True)
    embed.add_field(name="실패/미등록", value=f"{len(not_found)}개", inline=True)

    if updated:
        embed.add_field(
            name="적용 길드",
            value=limit_text([f"• `{g}`" for g in updated], 2500),
            inline=False
        )

    if not_found:
        embed.add_field(
            name="미등록 또는 실패",
            value=limit_text([f"• `{g}`" for g in not_found], 1000),
            inline=False
        )

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="스캔주기일괄변경", description="현재 특정 주기인 모든 길드를 다른 주기로 한번에 변경합니다.")
@admin_only()
@app_commands.describe(
    from_interval="현재 스캔주기",
    to_interval="변경할 스캔주기"
)
async def bulk_change_by_interval(
    interaction: discord.Interaction,
    from_interval: app_commands.Range[int, 5, 240],
    to_interval: app_commands.Range[int, 10, 240]
):
    if not await require_admin_channel(interaction):
        return

    changed = db.bulk_update_by_current_interval(int(from_interval), int(to_interval))

    embed = make_embed("⚙️ 스캔주기 전체 변경 완료", color=0x3498DB)
    embed.add_field(name="변경", value=f"{from_interval}분 → {to_interval}분", inline=True)
    embed.add_field(name="대상", value=f"{len(changed)}개", inline=True)

    if changed:
        embed.add_field(
            name="변경된 길드",
            value=limit_text([f"• `{g}`" for g in changed], 3500),
            inline=False
        )
    else:
        embed.add_field(name="결과", value=f"{from_interval}분 스캔 길드가 없습니다.", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="관심가문추가", description="가입/탈퇴 변동을 따로 알림 받을 가문을 등록합니다.")
@admin_only()
@app_commands.describe(family_name="가문명")
async def watch_family_add(interaction: discord.Interaction, family_name: str):
    if not await require_admin_channel(interaction):
        return

    ok = db.add_watched_family(family_name)
    await interaction.response.send_message(
        f"✅ 관심 가문 등록: `{normalize_name(family_name)}`" if ok else f"이미 등록됨: `{normalize_name(family_name)}`",
        ephemeral=True
    )


@bot.tree.command(name="관심가문제거", description="관심 가문 알림 등록을 제거합니다.")
@admin_only()
@app_commands.describe(family_name="가문명")
async def watch_family_remove(interaction: discord.Interaction, family_name: str):
    if not await require_admin_channel(interaction):
        return

    ok = db.remove_watched_family(family_name)
    await interaction.response.send_message(
        f"✅ 관심 가문 제거: `{normalize_name(family_name)}`" if ok else f"등록되지 않은 가문: `{normalize_name(family_name)}`",
        ephemeral=True
    )


@bot.tree.command(name="관심가문목록", description="관심 가문 목록을 봅니다.")
@admin_only()
async def watch_family_list(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    rows = db.list_watched_families()
    embed = make_embed("🔔 관심 가문 목록", color=0xE91E63)

    if not rows:
        embed.add_field(name="결과", value="등록된 관심 가문 없음", inline=False)
    else:
        lines = [f"• `{r['family_name']}` | {kst_date_only(r['created_at'])}" for r in rows]
        chunks = chunk_lines_for_embed(lines, max_chars=900, max_chunks=10)
        for i, chunk in enumerate(chunks, start=1):
            embed.add_field(name=f"목록 {i}/{len(chunks)}", value=chunk or "-", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="가문명변경테스트", description="두 가문명이 변경 추정으로 잡히는지 테스트합니다.")
@admin_only()
@app_commands.describe(old_name="이전 가문명", new_name="새 가문명")
async def rename_test(interaction: discord.Interaction, old_name: str, new_name: str):
    if not await require_admin_channel(interaction):
        return

    score = simple_name_similarity(old_name, new_name)
    confidence = "높음" if score >= 0.75 else "중간" if score >= 0.60 else "낮음" if score >= 0.45 else "추정 안함"

    embed = make_embed("🟣 가문명 변경 추정 테스트", color=0x9B59B6)
    embed.add_field(name="이전", value=f"`{old_name}`", inline=True)
    embed.add_field(name="새 이름", value=f"`{new_name}`", inline=True)
    embed.add_field(name="점수", value=f"{score:.3f}", inline=True)
    embed.add_field(name="판정", value=confidence, inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="상태", description="봇 상태를 봅니다.")
async def status(interaction: discord.Interaction):
    if not await require_admin_channel(interaction):
        return

    rows = db.list_watched_guilds()
    unscanned = db.get_unscanned_guilds()

    embed = make_embed("✅ 봇 상태", color=0x1ABC9C)
    embed.add_field(name="감시 길드 수", value=str(len(rows)), inline=True)
    embed.add_field(name="미스캔", value=str(len(unscanned)), inline=True)
    embed.add_field(name="스캔 루프", value=f"{SCAN_LOOP_SECONDS}초", inline=True)
    embed.add_field(name="동시 요청", value=str(MAX_CONCURRENT_REQUESTS), inline=True)
    embed.add_field(name="브라우저 모드", value="켜짐", inline=True)
    embed.add_field(name="헤드리스", value="켜짐" if BROWSER_HEADLESS else "꺼짐", inline=True)
    embed.add_field(name="차단방지", value=f"{ANTI_BLOCK_MIN_DELAY_SECONDS:.0f}~{ANTI_BLOCK_MAX_DELAY_SECONDS:.0f}초 랜덤대기", inline=True)
    embed.add_field(name="야간감속", value="켜짐" if is_night_slowdown_now() else "대기중", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)