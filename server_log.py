import csv
from datetime import date
import re
import sys
from pathlib import Path


LOG_RE = re.compile(
    r'^L\s+(?P<date>\d{2}/\d{2}/\d{4})\s+-\s+(?P<time>\d{2}:\d{2}:\d{2}):\s+(?P<message>.*)$'
)

# Adjust these patterns to your exact server log format
PATTERNS = [
    # Example:
    # Player connected: name=Minh, steamid=7656119..., ip=138.25.4.57, hero=npc_dota_hero_nevermore, team=RADIANT
        (
            "identity_bot",
            re.compile(
                r'"(?P<userid>\d+):[^:]+:(?P<name>[^<]+)<(?P<slot>\d+)><><>" connected, address "(?P<ip>[^"]+)"'
            )
        ),


    # Example:
    # steamid=7656119... name=Minh ip=138.25.4.57 hero=npc_dota_hero_nevermore
    (
        "player_identity_alt",
        re.compile(
            r"^steamid=(?P<steamid>\d+)\s+"
            r"name=(?P<steam_name>.*?)\s+"
            r"ip=(?P<ip>\d{1,3}(?:\.\d{1,3}){3})\s+"
            r"hero=(?P<hero_name>npc_dota_hero_[a-zA-Z0-9_]+)"
            r"(?:\s+team=(?P<team>[A-Z]+))?$"
        )
    ),

    # Example:
    # Bot connected: name=Bot_1, ip=127.0.0.1, hero=npc_dota_hero_razor, team=DIRE
    (
        "bot_identity",
        re.compile(
            r"^Bot connected:\s+name=(?P<steam_name>.*?),\s+"
            r"ip=(?P<ip>\d{1,3}(?:\.\d{1,3}){3}),\s+"
            r"hero=(?P<hero_name>npc_dota_hero_[a-zA-Z0-9_]+),\s+"
            r"team=(?P<team>[A-Z]+)$"
        )
    ),
]

def norm_hero(name: str) -> str:
    name = (name or "").strip().lower()
    if name.startswith("npc_dota_hero_"):
        name = name[len("npc_dota_hero_"):]
    return name

def load_identity_map_from_server_log(raw_dir: Path):
    server_log_path = find_latest_server_log(raw_dir)
    if server_log_path is None:
        print(f"No server log found in {raw_dir}; continuing without player identity enrichment.")
        return {}

    print(f"Using server log: {server_log_path.name}")

    identity_rows = extract_player_identity_from_server_log(server_log_path)

    identity_map = {}
    for row in identity_rows:
        hero = norm_hero(row.get("hero_name"))
        if not hero:
            continue
        identity_map[hero] = row

    print("IDENTITY MAP SIZE:", len(identity_map))
    for hero, info in identity_map.items():
        print("IDENTITY:", hero, info)

    return identity_map

def attach_identity_to_combat_rows(rows, identity_map):
    for row in rows:
        actor = norm_hero(row.get("actor", ""))
        target = norm_hero(row.get("target", ""))

        actor_info = identity_map.get(actor, {})
        target_info = identity_map.get(target, {})

        row["actor_steamid"] = actor_info.get("steamid", "")
        row["actor_steam_name"] = actor_info.get("steam_name", "")
        row["actor_ip"] = actor_info.get("ip", "")
        row["actor_is_bot"] = actor_info.get("is_bot", "")

        row["target_steamid"] = target_info.get("steamid", "")
        row["target_steam_name"] = target_info.get("steam_name", "")
        row["target_ip"] = target_info.get("ip", "")
        row["target_is_bot"] = target_info.get("is_bot", "")

    return rows


def attach_identity_to_economy_rows(rows, identity_map):
    for row in rows:
        actor = norm_hero(row.get("actor", ""))
        actor_info = identity_map.get(actor, {})

        row["actor_steamid"] = actor_info.get("steamid", "")
        row["actor_steam_name"] = actor_info.get("steam_name", "")
        row["actor_ip"] = actor_info.get("ip", "")
        row["actor_is_bot"] = actor_info.get("is_bot", "")

    return rows

def strip_npc_hero_prefix(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"^npc_dota_hero_", "", name)


def parse_message(message: str):
    stripped = message.strip()
    for event_type, pattern in PATTERNS:
        m = pattern.match(stripped)
        if m:
            return event_type, m.groupdict()
    return "generic", {}


def parse_file(log_path: Path):
    identity_rows = []
    all_rows = []

    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line.strip():
                continue

            m = LOG_RE.match(line)
            if not m:
                if any(k in line.lower() for k in ["steam", "hero", "ip", "client", "player"]):
                    print("TOP-LEVEL UNPARSED LINE:", line)
                all_rows.append({
                    "timestamp": "",
                    "logger": "",
                    "event_type": "unparsed",
                    "message": line.strip(),
                })
                continue

            date_str = m.group("date")
            time_str = m.group("time")
            timestamp = f"{date_str} {time_str}"
            logger = "server"
            message = m.group("message")

            event_type, data = parse_message(message)
            if event_type == "generic" and any(k in message.lower() for k in ["steam", "hero", "ip", "client", "player"]):
                print("UNMATCHED SERVER LINE:", message)

            row = {
                "date": m.group("date"),
                "time": m.group("time"),
                "timestamp": f"{m.group('date')} {m.group('time')}",
                "logger": "server",
                "event_type": event_type,
                "steamid": data.get("steamid", ""),
                "steam_name": data.get("steam_name", ""),
                "ip": data.get("ip", ""),
                "hero_name": strip_npc_hero_prefix(data.get("hero_name", "")),
                "team": data.get("team", ""),
                "is_bot": "true" if event_type in {"bot_identity", "identity_bot"} else "false",
                "message": message,
}

            all_rows.append(row)

            if event_type in {"player_identity", "player_identity_alt", "bot_identity", "identity_bot"}:
                identity_rows.append(row)

    return identity_rows, all_rows


def dedupe_identity_rows(rows):
    """
    Keep one best row per hero_name.
    Prefer rows with steamid and ip populated.
    """
    best = {}

    for row in rows:
        hero = row.get("hero_name", "")
        if not hero:
            continue

        score = 0
        if row.get("steamid"):
            score += 2
        if row.get("ip"):
            score += 1
        if row.get("steam_name"):
            score += 1

        existing = best.get(hero)
        if existing is None:
            best[hero] = (score, row)
        else:
            if score > existing[0]:
                best[hero] = (score, row)

    return [item[1] for item in best.values()]


def write_csv(path: Path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def extract_player_identity_from_server_log(log_path: Path):
    identity_rows, _ = parse_file(log_path)
    return dedupe_identity_rows(identity_rows)


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
        identity_rows = dedupe_identity_rows(identity_rows)
        print("IDENTITY ROWS FOUND:", len(identity_rows))
        for row in identity_rows[:20]:
            print(row)

        write_csv(
            processed_dir / f"{prefix}_player_identity.csv",
            ["date","time","timestamp", "logger", "event_type", "steamid", "steam_name", "ip", "hero_name", "team", "is_bot", "message"],
            identity_rows,
        )

        write_csv(
            processed_dir / f"{prefix}_events.csv",
            ["date","time","timestamp", "logger", "event_type", "steamid", "steam_name", "ip", "hero_name", "team", "is_bot", "message"],
            all_rows,
        )

        print(f"[{log_path.name}] → {len(identity_rows)} identity rows, {len(all_rows)} total rows")

    print(f"\nCSVs written to: {processed_dir}")


if __name__ == "__main__":
    main()