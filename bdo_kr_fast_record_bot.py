import asyncio
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Set
from urllib.parse import quote

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from bs4 import BeautifulSoup
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

SCAN_LOOP_SECONDS = int(os.getenv("SCAN_LOOP_SECONDS", "60"))
DEFAULT_SCAN_INTERVAL_MIN = int(os.getenv("DEFAULT_SCAN_INTERVAL_MIN", "15"))
PRIORITY_SCAN_INTERVAL_MIN = int(os.getenv("PRIORITY_SCAN_INTERVAL_MIN", "10"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "2"))
REQUEST_RETRY_COUNT = int(os.getenv("REQUEST_RETRY_COUNT", "2"))
RENAME_WINDOW_MINUTES = int(os.getenv("RENAME_WINDOW_MINUTES", "30"))

GUILD_PROFILE_URL = "https://www.kr.playblackdesert.com/ko-KR/Adventure/Guild/GuildProfile?guildName={guild_name}&region=KR"

if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN 이 비어 있습니다. .env 파일에 넣으세요.")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_text() -> str:
    return utc_now().isoformat(timespec="seconds")


def parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text)


def kst_display(text: Optional[str]) -> str:
    if not text:
        return "-"
    try:
        dt = parse_iso(text).astimezone(timezone(timedelta(hours=9)))
        return dt.strftime("%Y-%m-%d %H:%M:%S KST")
    except Exception:
        return text


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip()


def build_guild_url(guild_name: str) -> str:
    return GUILD_PROFILE_URL.format(guild_name=quote(guild_name))


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
        next_at = utc_now() + timedelta(minutes=interval_min)
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE watched_guilds
            SET last_success_at = ?, last_error = NULL, next_scan_after = ?
            WHERE guild_name = ?
        """, (utc_now_text(), next_at.isoformat(timespec="seconds"), guild_name))
        self.conn.commit()

    def mark_scan_failure(self, guild_name: str, error_text: str):
        next_at = utc_now() + timedelta(minutes=5)
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE watched_guilds
            SET last_error = ?, next_scan_after = ?
            WHERE guild_name = ?
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

    def get_family_recent_guilds(self, family_name: str, limit: int = 4):
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
    embed.set_footer(text="BDO KR Fast Record Bot")
    return embed


def extract_members_from_guild_html(html: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    if "구성원" not in text or "가문명" not in text:
        return set()

    lines = [normalize_name(x) for x in text.splitlines() if normalize_name(x)]
    start = None
    end = None

    for i, line in enumerate(lines):
        if line == "구성원":
            start = i
            break
    if start is None:
        return set()

    for j in range(start + 1, len(lines)):
        if any(x in lines[j] for x in ("Facebook", "Youtube", "Instagram", "Twitch", "서비스 이용약관")):
            end = j
            break
    if end is None:
        end = len(lines)

    block = lines[start:end]
    blacklist = {
        "구성원", "가문명", "길드 프로필", "길드생성일", "대장", "인원", "명", "비공개",
        "점령현황 없음", "가문", "검은사막", "검은사막+", "Facebook", "Youtube", "Instagram", "Twitch"
    }

    members = set()
    for line in block:
        if not line or line in blacklist:
            continue
        if any(x in line for x in ("길드생성일", "점령현황", "Copyright", "Pearl Abyss", "로그인", "게임 시작")):
            continue
        line = re.sub(r"\s+대장$", "", line).strip()
        if not re.fullmatch(r"[0-9A-Za-z가-힣_]{2,16}", line):
            continue
        members.add(line)

    return members


async def fetch_guild_members(guild_name: str) -> GuildScanResult:
    url = build_guild_url(guild_name)
    html = await http_client.get_text(url)
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
    result = await fetch_guild_members(guild_name)

    db.create_snapshot(guild_name, result.members, result.source_url)

    joined = sorted(result.members - previous)
    left = sorted(previous - result.members)

    if joined or left:
        db.insert_events(guild_name, joined, left, result.source_url)

    rename_suspicions = await detect_rename_suspicions(guild_name, joined)
    db.mark_scan_success(guild_name, interval_min)

    return {
        "guild_name": guild_name,
        "member_count": len(result.members),
        "joined": joined,
        "left": left,
        "rename_suspicions": rename_suspicions,
        "fetched_at": result.fetched_at,
    }


intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


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


async def notify_changes(summary: dict):
    for discord_guild in bot.guilds:
        channel_id = db.get_notify_channel(discord_guild.id)
        if not channel_id:
            continue
        channel = discord_guild.get_channel(channel_id)
        if channel is None:
            continue

        embed = make_embed(
            title=f"길드 변화 · {summary['guild_name']}",
            description=f"현재 인원: **{summary['member_count']}명**",
            color=0x2ECC71 if summary["joined"] else 0xE67E22,
        )
        embed.add_field(name="가입", value="\n".join(f"🟢 `{x}`" for x in summary["joined"][:20]) or "-", inline=False)
        embed.add_field(name="탈퇴", value="\n".join(f"🔴 `{x}`" for x in summary["left"][:20]) or "-", inline=False)
        await channel.send(embed=embed)


@tasks.loop(seconds=SCAN_LOOP_SECONDS)
async def scheduler_loop():
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
        if False:
            await notify_changes(res)


@scheduler_loop.before_loop
async def before_scheduler():
    await bot.wait_until_ready()


@bot.tree.command(name="채널설정", description="알림 채널을 정합니다.")
@admin_only()
@app_commands.describe(channel="알림 채널")
async def set_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    db.set_notify_channel(interaction.guild.id, channel.id)
    await interaction.response.send_message(f"알림 채널 설정 완료: {channel.mention}", ephemeral=True)


@bot.tree.command(name="길드추가", description="감시할 길드를 추가합니다.")
@admin_only()
@app_commands.describe(guild_name="길드명", priority="중요 길드면 체크", interval_min="스캔 간격(분)")
async def add_guild(interaction: discord.Interaction, guild_name: str, priority: bool = False, interval_min: Optional[int] = None):
    db.add_watched_guild(guild_name.strip(), 1 if priority else 0, interval_min)
    chosen = interval_min or (PRIORITY_SCAN_INTERVAL_MIN if priority else DEFAULT_SCAN_INTERVAL_MIN)
    await scan_one_guild(guild_name.strip(), chosen)
    await interaction.response.send_message(f"추가 완료: `{guild_name}` / 간격 {chosen}분", ephemeral=True)


@bot.tree.command(name="길드제거", description="감시 길드를 뺍니다.")
@admin_only()
@app_commands.describe(guild_name="길드명")
async def remove_guild(interaction: discord.Interaction, guild_name: str):
    deleted = db.remove_watched_guild(guild_name.strip())
    if deleted:
        await interaction.response.send_message(f"제거 완료: `{guild_name}`", ephemeral=True)
    else:
        await interaction.response.send_message("등록된 길드가 없습니다.", ephemeral=True)


@bot.tree.command(name="길드목록", description="감시 중인 길드 목록을 봅니다.")
async def list_guilds(interaction: discord.Interaction):
    rows = db.list_watched_guilds()
    if not rows:
        await interaction.response.send_message("등록된 길드가 없습니다.", ephemeral=True)
        return

    embed = make_embed("감시 길드 목록", color=0x3498DB)
    lines = []
    for r in rows[:30]:
        level = "중요" if r["priority"] else "일반"
        lines.append(f"• {r['guild_name']} | {level} | {r['scan_interval_min']}분 | 성공: {kst_display(r['last_success_at'])}")
    embed.add_field(name="목록", value="\n".join(lines), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="즉시스캔", description="지금 바로 스캔합니다.")
@admin_only()
@app_commands.describe(limit="최대 길드 수")
async def manual_scan(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 10] = 5):
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


@bot.tree.command(name="가문추적", description="가문 기록을 바로 봅니다.")
@app_commands.describe(family_name="가문명")
async def family_track(interaction: discord.Interaction, family_name: str):
    family_name = family_name.strip()
    current_guild = db.get_family_current_guild_guess(family_name)
    recent_guilds = db.get_family_recent_guilds(family_name, 4)
    rename_rows = db.get_family_rename_suspicions(family_name, 5)

    embed = make_embed("가문 추적 결과", color=0x9B59B6)
    embed.add_field(name="가문명", value=family_name, inline=True)
    embed.add_field(name="현재 길드(기록 기준)", value=current_guild or "기록 없음", inline=True)

    prev_lines = []
    for guild_name, detected_at, event_type in recent_guilds:
        emoji = "🟢" if event_type == "join" else "🔴"
        prev_lines.append(f"{emoji} {guild_name} | {kst_display(detected_at)}")
    embed.add_field(
        name="이전 길드 4개 (기록 기준)",
        value="\n".join(prev_lines) if prev_lines else "기록 없음",
        inline=False
    )

    rename_lines = []
    for r in rename_rows:
        rename_lines.append(f"[{r['confidence']}] {r['old_family_name']} → {r['new_family_name']}")
    embed.add_field(
        name="가문명 변경 추정",
        value="\n".join(rename_lines) if rename_lines else "기록 없음",
        inline=False
    )

    embed.add_field(
        name="설명",
        value="이 가문추적은 공홈 검색이 아니라 이 봇이 모은 기록으로 바로 보여줍니다.",
        inline=False
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="길드연혁", description="길드 연혁을 봅니다.")
@app_commands.describe(guild_name="길드명")
async def guild_history(interaction: discord.Interaction, guild_name: str):
    await interaction.response.defer(ephemeral=True, thinking=True)

    member_count = db.get_current_guild_member_count(guild_name.strip())
    rows = db.get_guild_recent_events(guild_name.strip(), 30)

    embed = make_embed("길드 연혁", color=0xF39C12)
    embed.add_field(name="길드명", value=guild_name, inline=True)
    embed.add_field(name="현재 인원", value=str(member_count), inline=True)

    lines = []
    for r in rows[:25]:
        emoji = "🟢" if r["event_type"] == "join" else "🔴"
        lines.append(f"{emoji} `{r['family_name']}` | {kst_display(r['detected_at'])}")
    embed.add_field(
        name="최근 가입 / 탈퇴 30개",
        value="\n".join(lines) if lines else "기록 없음",
        inline=False
    )

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="상태", description="봇 상태를 봅니다.")
async def status(interaction: discord.Interaction):
    rows = db.list_watched_guilds()
    embed = make_embed("봇 상태", color=0x1ABC9C)
    embed.add_field(name="감시 길드 수", value=str(len(rows)), inline=True)
    embed.add_field(name="스캔 루프", value=f"{SCAN_LOOP_SECONDS}초", inline=True)
    embed.add_field(name="동시 요청", value=str(MAX_CONCURRENT_REQUESTS), inline=True)
    embed.add_field(name="가문추적 방식", value="기록 즉시조회", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)



@bot.tree.command(name="미스캔", description="아직 스캔 안 된 길드 목록")
async def not_scanned(interaction: discord.Interaction):
    rows = db.list_watched_guilds()

    not_scanned = []
    for r in rows:
        if not r["last_success_at"]:
            not_scanned.append(r["guild_name"])

    if not not_scanned:
        await interaction.response.send_message("모든 길드가 최소 1회 스캔 완료됨", ephemeral=True)
        return

    text = "\n".join(f"• {g}" for g in not_scanned[:30])

    embed = make_embed("미스캔 길드", color=0xE74C3C)
    embed.add_field(name="아직 스캔 안됨", value=text, inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
