import csv
import re
import sys
from pathlib import Path


# Real server-log top-level format:
# 2026-04-07 16:18:26 message...
LOG_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<time>\d{2}:\d{2}:\d{2})\s+(?P<message>.*)$"
)

# Real useful line families seen in your logs
RE_STEAM_NET = re.compile(
    r"steamid:(?P<steamid>\d+)@(?P<ip>\d{1,3}(?:\.\d{1,3}){3})(?::(?P<port>\d+))?\s+'(?P<steam_name>[^']+)'"
)

RE_PLAYER_ACCOUNT = re.compile(
    r"Player\s+(?P<steamid>\d+)\s+Account\s+(?P<account_id>\d+)\s+TotalGold\s*="
)

RE_HERO_ACCOUNT = re.compile(
    r"(?P<hero_name>[a-zA-Z0-9_]+)\s*-\s*(?P<account_id>\d+)\s*-\s*Predicted Rank\s*="
)

RE_TEAM_PLAYER_ACCOUNT = re.compile(
    r"Team\s+(?P<team_num>\d+)\s+Player\s+(?P<team_slot>\d+)\s+m_unAccountID\s*=\s*(?P<steamid>\d+)"
)

# Optional bot-ish connect lines if present
RE_BOT_CONNECT = re.compile(
    r'"(?P<userid>\d+):[^:]+:(?P<steam_name>[^<]+)<(?P<slot>\d+)><><>" connected, address "(?P<ip>[^"]+)"'
)


def norm_hero(name: str) -> str:
    name = (name or "").strip().lower()
    if name.startswith("npc_dota_hero_"):
        name = name[len("npc_dota_hero_"):]
    return name


def parse_message(message: str):
    """
    Return (event_type, data_dict) for the real server-log formats.
    """
    msg = message.strip()

    m = RE_STEAM_NET.search(msg)
    if m:
        return "steam_net_identity", m.groupdict()

    m = RE_PLAYER_ACCOUNT.search(msg)
    if m:
        return "player_account_summary", m.groupdict()

    m = RE_HERO_ACCOUNT.search(msg)
    if m:
        gd = m.groupdict()
        gd["hero_name"] = norm_hero(gd["hero_name"])
        return "hero_account_map", gd

    m = RE_TEAM_PLAYER_ACCOUNT.search(msg)
    if m:
        return "team_player_account", m.groupdict()

    m = RE_BOT_CONNECT.search(msg)
    if m:
        return "bot_connect", m.groupdict()

    return "generic", {}


def parse_file(log_path: Path):
    all_rows = []

    # Intermediate join tables
    steam_identity_by_steamid = {}   # steamid -> {steamid, steam_name, ip, port}
    steamid_to_account = {}          # steamid -> account_id
    account_to_hero = {}             # account_id -> hero_name

    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line.strip():
                continue

            m = LOG_RE.match(line)
            if not m:
                all_rows.append({
                    "date": "",
                    "time": "",
                    "timestamp": "",
                    "logger": "server",
                    "event_type": "unparsed",
                    "steamid": "",
                    "account_id": "",
                    "steam_name": "",
                    "ip": "",
                    "port": "",
                    "hero_name": "",
                    "team": "",
                    "is_bot": "",
                    "message": line.strip(),
                })
                continue

            date_str = m.group("date")
            time_str = m.group("time")
            timestamp = f"{date_str} {time_str}"
            message = m.group("message")

            event_type, data = parse_message(message)

            row = {
                "date": date_str,
                "time": time_str,
                "timestamp": timestamp,
                "logger": "server",
                "event_type": event_type,
                "steamid": data.get("steamid", ""),
                "account_id": data.get("account_id", ""),
                "steam_name": data.get("steam_name", ""),
                "ip": data.get("ip", ""),
                "port": data.get("port", ""),
                "hero_name": norm_hero(data.get("hero_name", "")),
                "team": data.get("team", ""),
                "is_bot": "true" if event_type == "bot_connect" else "false",
                "message": message,
            }
            all_rows.append(row)

            # Build join tables
            if event_type == "steam_net_identity":
                steamid = data.get("steamid", "")
                if steamid:
                    existing = steam_identity_by_steamid.get(steamid, {})
                    merged = {
                        "steamid": steamid,
                        "steam_name": data.get("steam_name", "") or existing.get("steam_name", ""),
                        "ip": data.get("ip", "") or existing.get("ip", ""),
                        "port": data.get("port", "") or existing.get("port", ""),
                    }
                    steam_identity_by_steamid[steamid] = merged

            elif event_type == "player_account_summary":
                steamid = data.get("steamid", "")
                account_id = data.get("account_id", "")
                if steamid and account_id:
                    steamid_to_account[steamid] = account_id

            elif event_type == "hero_account_map":
                account_id = data.get("account_id", "")
                hero_name = norm_hero(data.get("hero_name", ""))
                if account_id and hero_name:
                    account_to_hero[account_id] = hero_name

    # Final joined identity rows
    identity_rows = []
    for steamid, ident in steam_identity_by_steamid.items():
        account_id = steamid_to_account.get(steamid, "")
        hero_name = account_to_hero.get(account_id, "")

        identity_rows.append({
            "date": "",
            "time": "",
            "timestamp": "",
            "logger": "server",
            "event_type": "joined_identity",
            "steamid": steamid,
            "account_id": account_id,
            "steam_name": ident.get("steam_name", ""),
            "ip": ident.get("ip", ""),
            "port": ident.get("port", ""),
            "hero_name": hero_name,
            "team": "",
            "is_bot": "false",
            "message": "",
        })

    identity_rows = dedupe_identity_rows(identity_rows)
    return identity_rows, all_rows


def dedupe_identity_rows(rows):
    """
    Keep one best row per hero_name.
    """
    best = {}
    for row in rows:
        hero = norm_hero(row.get("hero_name", ""))
        if not hero:
            continue

        score = 0
        if row.get("steamid"):
            score += 2
        if row.get("account_id"):
            score += 1
        if row.get("steam_name"):
            score += 1
        if row.get("ip"):
            score += 1

        existing = best.get(hero)
        if existing is None or score > existing[0]:
            row["hero_name"] = hero
            best[hero] = (score, row)

    return [v[1] for v in best.values()]


def write_csv(path: Path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def extract_player_identity_from_server_log(log_path: Path):
    identity_rows, _ = parse_file(log_path)
    return identity_rows


def find_latest_server_log(raw_dir: Path) -> Path | None:
    candidates = sorted(raw_dir.glob("server*.log")) + sorted(raw_dir.glob("server*.txt"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def main():
    script_dir = Path(__file__).parent
    raw_dir = script_dir / "raw_data"
    processed_dir = script_dir / "processed"

    raw_dir.mkdir(exist_ok=True)
    processed_dir.mkdir(exist_ok=True)

    if len(sys.argv) >= 2:
        log_files = [Path(sys.argv[1])]
    else:
        log_files = sorted(raw_dir.glob("server*.txt")) + sorted(raw_dir.glob("server*.log"))
        if not log_files:
            print(f"No server log files found in {raw_dir}")
            sys.exit(0)

    for log_path in log_files:
        if not log_path.exists():
            print(f"File not found: {log_path}")
            continue

        prefix = log_path.stem
        identity_rows, all_rows = parse_file(log_path)

        print("IDENTITY ROWS FOUND:", len(identity_rows))
        for row in identity_rows[:20]:
            print(row)

        write_csv(
            processed_dir / f"{prefix}_player_identity.csv",
            [
                "date", "time", "timestamp", "logger", "event_type",
                "steamid", "account_id", "steam_name", "ip", "port",
                "hero_name", "team", "is_bot", "message"
            ],
            identity_rows,
        )

        write_csv(
            processed_dir / f"{prefix}_events.csv",
            [
                "date", "time", "timestamp", "logger", "event_type",
                "steamid", "account_id", "steam_name", "ip", "port",
                "hero_name", "team", "is_bot", "message"
            ],
            all_rows,
        )

        print(f"[{log_path.name}] → {len(identity_rows)} identity rows, {len(all_rows)} total rows")

    print(f"\nCSVs written to: {processed_dir}")


if __name__ == "__main__":
    main()