"""
combatlog.py  —  Dota 2 combat log parser
Reads UTF-16 encoded combat log files from raw_data/ and writes CSVs to processed/.

Produces 3 focused CSVs per log file:
  <stem>_combat.csv     — hits, kills, heals, damage events
  <stem>_economy.csv    — gold & XP gains/losses, item purchases & uses
  <stem>_abilities.csv  — ability casts, modifier gains/losses, game state changes
  <stem>_blocks.csv     — structured DOTA_COMBATLOG_* block events
"""

import csv
import re
import sys
from pathlib import Path
import datetime
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Regexes
# ---------------------------------------------------------------------------

TIME_RE = re.compile(r"^\[(?P<ts>\d{2}:\d{2}:\d{2}\.\d{3})\]\s+(?P<msg>.+)$")
BLOCK_HEADER_RE = re.compile(
    r"^(?P<block_type>DOTA_COMBATLOG_[A-Z_]+)\s+\((?P<block_id>\d+)\):\s+type:\s+(?P<declared_type>\S+)$"
)
KEY_VALUE_RE = re.compile(r"^(?P<key>[A-Za-z0-9_]+):\s*(?P<value>.*)$")

# Line event patterns
RE_GAME_STATE   = re.compile(r"^game state is now (?P<state>\d+)$")
RE_GOLD_GAIN    = re.compile(r"^(?P<actor>\S+) receives (?P<value>\d+) gold$")
RE_GOLD_LOSE    = re.compile(r"^(?P<actor>\S+) looses (?P<value>\d+) gold$")
RE_XP_GAIN      = re.compile(r"^(?P<actor>\S+) gains (?P<value>\d+) XP$")
RE_BUY_ITEM     = re.compile(r"^(?P<actor>\S+) buys item (?P<item>\S+)$")
RE_USE_ITEM     = re.compile(r"^(?P<actor>\S+) uses (?P<item>\S+)$")
RE_CAST         = re.compile(
    r"^(?P<actor>\S+) casts ability (?P<ability>\S+) \(lvl (?P<level>\d+)\) on (?P<target>\S+)$"
)
RE_MOD_GAIN     = re.compile(
    r"^(?P<actor>\S+) receives (?P<modifier>\S+) buff/debuff from (?P<source>\S+)$"
)
RE_MOD_LOSE     = re.compile(r"^(?P<actor>\S+) loses (?P<modifier>\S+) buff/debuff$")
RE_HIT          = re.compile(
    r"^(?P<actor>\S+) hits (?P<target>\S+) with (?P<source>\S+) "
    r"for (?P<value>\d+) damage(?:\s+\((?P<hp_before>\d+)->(?P<hp_after>\d+)\))?$"
)
RE_KILL         = re.compile(r"^(?P<target>\S+) is killed by (?P<actor>\S+)$")
RE_HEAL         = re.compile(
    r"^(?P<actor>\S+)'s (?P<source>\S+) heals (?P<target>\S+) "
    r"for (?P<value>\d+) health(?:\s+\((?P<hp_before>\d+)->(?P<hp_after>\d+)\))?$"
)

# Game state meanings (Dota 2 standard)
GAME_STATES = {
    "2": "hero_selection", "3": "strategy_time", "4": "game_in_progress",
    "8": "pre_game", "9": "game_loading", "10": "post_game", "12": "wait_for_players",
}


TEAM_NUM_MAP = {"2": "RADIANT", "3": "DIRE"}

def strip_npc_prefix(name: str) -> str:
    for prefix in ("npc_dota_hero_", "npc_dota_", "npc_"):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name

def infer_team_from_name(name: str) -> str:
    if not name:
        return ""
    s = name.lower()

    if "goodguys" in s:
        return "RADIANT"
    if "badguys" in s:
        return "DIRE"

    return ""

def normalize_team_num(value: str) -> str:
    return TEAM_NUM_MAP.get(str(value).strip(), "")

def build_team_map(lines: list[str], block_rows: list[dict]) -> dict[str, str]:
    team_map: dict[str, str] = {}

    def remember(unit: str, team: str):
        unit = strip_npc_prefix(unit)
        if unit and team and unit not in team_map:
            team_map[unit] = team

    # Pass 1: direct goodguys/badguys clues in line events
    for line in lines:
        m = TIME_RE.match(line)
        if not m:
            continue
        msg = m.group("msg")

        for pattern in (
            RE_HIT, RE_KILL, RE_HEAL, RE_GOLD_GAIN, RE_GOLD_LOSE,
            RE_XP_GAIN, RE_BUY_ITEM, RE_USE_ITEM, RE_CAST,
            RE_MOD_GAIN, RE_MOD_LOSE
        ):
            mm = pattern.match(msg)
            if not mm:
                continue

            for key in ("actor", "target", "source"):
                raw = mm.groupdict().get(key)
                if raw:
                    team = infer_team_from_name(raw)
                    if team:
                        remember(raw, team)

        # Pass 1b: tower/building modifier source clues
        mm = RE_MOD_GAIN.match(msg)
        if mm:
            actor = mm.group("actor")
            source = mm.group("source")
            source_team = infer_team_from_name(source)
            if source_team:
                remember(actor, source_team)
                remember(source, source_team)

    # Pass 2: explicit numeric team ids from structured block rows
    for row in block_rows:
        attacker_name = row.get("attacker_name", "")
        target_name = row.get("target_name", "")
        attacker_team = normalize_team_num(row.get("attacker_team", ""))
        target_team = normalize_team_num(row.get("target_team", ""))

        if attacker_name and attacker_team and not attacker_name.isdigit():
            remember(attacker_name, attacker_team)
        if target_name and target_team and not target_name.isdigit():
            remember(target_name, target_team)

    return team_map

def get_unit_team(name: str, team_map: dict[str, str]) -> str:
    if not name:
        return ""

    clean = strip_npc_prefix(name)

    if clean in team_map:
        return team_map[clean]

    inferred = infer_team_from_name(clean)
    if inferred:
        return inferred

    return ""

def hhmmss_to_seconds(ts: str) -> float:
    hh, mm, rest = ts.split(":")
    return int(hh) * 3600 + int(mm) * 60 + float(rest)

def wallclock_to_dt(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")

def normalize_latency_intervals(latency_intervals, team_alias):
    intervals_by_team = {"RADIANT": [], "DIRE": []}

    for row in latency_intervals or []:
        raw_team = (row.get("team") or "").strip()
        mapped_team = team_alias.get(raw_team, raw_team).upper()

        if mapped_team not in intervals_by_team:
            continue

        start_dt = wallclock_to_dt(row["start_time"])
        end_dt = wallclock_to_dt(row["end_time"]) if row.get("end_time") else None

        intervals_by_team[mapped_team].append({
            "start_dt": start_dt,
            "end_dt": end_dt,
            "latency_ms": row.get("latency_ms", "")
        })

    for t in intervals_by_team:
        intervals_by_team[t].sort(key=lambda x: x["start_dt"])

    return intervals_by_team

def build_wallclock_for_event(ts, anchor):
    return anchor + timedelta(seconds=hhmmss_to_seconds(ts))

def lookup_latency(intervals_by_team, team, event_dt):
    if not team:
        return ""

    for interval in intervals_by_team.get(team, []):
        start = interval["start_dt"]
        end = interval["end_dt"]

        if end is None:
            if event_dt >= start:
                return interval["latency_ms"]
        else:
            if start <= event_dt < end:
                return interval["latency_ms"]

    return ""

def enrich_combat_rows(rows, team_map, intervals_by_team, anchor):
    for row in rows:
        ts = row.get("timestamp", "")

        actor_team = get_unit_team(row.get("actor"), team_map)
        target_team = get_unit_team(row.get("target"), team_map)

        dt = build_wallclock_for_event(ts, anchor)

        row["actor_team"] = actor_team
        row["target_team"] = target_team
        row["actor_latency_ms"] = lookup_latency(intervals_by_team, actor_team, dt)
        row["target_latency_ms"] = lookup_latency(intervals_by_team, target_team, dt)
        row["event_wallclock"] = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    return rows

def enrich_economy_rows(rows, team_map, intervals_by_team, anchor):
    for row in rows:
        ts = row.get("timestamp", "")

        actor_team = get_unit_team(row.get("actor"), team_map)
        dt = build_wallclock_for_event(ts, anchor)

        row["actor_team"] = actor_team
        row["actor_latency_ms"] = lookup_latency(intervals_by_team, actor_team, dt)
        row["event_wallclock"] = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    return rows


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_lines(lines: list[str]):
    combat_rows   = []
    economy_rows  = []
    ability_rows  = []

    for line in lines:
        m = TIME_RE.match(line)
        if not m:
            continue
        ts  = m.group("ts")
        msg = m.group("msg")

        # --- Hits (damage) ---
        mm = RE_HIT.match(msg)
        if mm:
            combat_rows.append({
                "timestamp":  ts,
                "event":      "hit",
                "actor":      strip_npc_prefix(mm.group("actor")),
                "target":     strip_npc_prefix(mm.group("target")),
                "source":     mm.group("source"),
                "value":      mm.group("value"),
                "unit":       "damage",
                "hp_before":  mm.group("hp_before") or "",
                "hp_after":   mm.group("hp_after") or "",
            })
            continue

        # --- Kills ---
        mm = RE_KILL.match(msg)
        if mm:
            combat_rows.append({
                "timestamp":  ts,
                "event":      "kill",
                "actor":      strip_npc_prefix(mm.group("actor")),
                "target":     strip_npc_prefix(mm.group("target")),
                "source":     "",
                "value":      "",
                "unit":       "",
                "hp_before":  "",
                "hp_after":   "",
            })
            continue

        # --- Heals (missing from original parser) ---
        mm = RE_HEAL.match(msg)
        if mm:
            combat_rows.append({
                "timestamp":  ts,
                "event":      "heal",
                "actor":      strip_npc_prefix(mm.group("actor")),
                "target":     strip_npc_prefix(mm.group("target")),
                "source":     mm.group("source"),
                "value":      mm.group("value"),
                "unit":       "health",
                "hp_before":  mm.group("hp_before") or "",
                "hp_after":   mm.group("hp_after") or "",
            })
            continue

        # --- Gold gain ---
        mm = RE_GOLD_GAIN.match(msg)
        if mm:
            economy_rows.append({
                "timestamp": ts,
                "event":     "gold_gain",
                "actor":     strip_npc_prefix(mm.group("actor")),
                "item":      "",
                "value":     mm.group("value"),
                "unit":      "gold",
            })
            continue

        # --- Gold loss ---
        mm = RE_GOLD_LOSE.match(msg)
        if mm:
            economy_rows.append({
                "timestamp": ts,
                "event":     "gold_loss",
                "actor":     strip_npc_prefix(mm.group("actor")),
                "item":      "",
                "value":     mm.group("value"),
                "unit":      "gold",
            })
            continue

        # --- XP gain ---
        mm = RE_XP_GAIN.match(msg)
        if mm:
            economy_rows.append({
                "timestamp": ts,
                "event":     "xp_gain",
                "actor":     strip_npc_prefix(mm.group("actor")),
                "item":      "",
                "value":     mm.group("value"),
                "unit":      "xp",
            })
            continue

        # --- Buy item ---
        mm = RE_BUY_ITEM.match(msg)
        if mm:
            item = mm.group("item").replace("item_", "")
            economy_rows.append({
                "timestamp": ts,
                "event":     "buy_item",
                "actor":     strip_npc_prefix(mm.group("actor")),
                "item":      item,
                "value":     "",
                "unit":      "",
            })
            continue

        # --- Use item ---
        mm = RE_USE_ITEM.match(msg)
        if mm:
            item = mm.group("item").replace("item_", "")
            economy_rows.append({
                "timestamp": ts,
                "event":     "use_item",
                "actor":     strip_npc_prefix(mm.group("actor")),
                "item":      item,
                "value":     "",
                "unit":      "",
            })
            continue

        # --- Ability cast ---
        mm = RE_CAST.match(msg)
        if mm:
            ability_rows.append({
                "timestamp": ts,
                "event":     "cast_ability",
                "actor":     strip_npc_prefix(mm.group("actor")),
                "target":    strip_npc_prefix(mm.group("target")),
                "ability":   mm.group("ability"),
                "modifier":  "",
                "source":    "",
                "value":     mm.group("level"),
                "unit":      "level",
                "state":     "",
                "state_name":"",
            })
            continue

        # --- Modifier gain ---
        mm = RE_MOD_GAIN.match(msg)
        if mm:
            ability_rows.append({
                "timestamp": ts,
                "event":     "modifier_gain",
                "actor":     strip_npc_prefix(mm.group("actor")),
                "target":    "",
                "ability":   "",
                "modifier":  mm.group("modifier"),
                "source":    strip_npc_prefix(mm.group("source")),
                "value":     "",
                "unit":      "",
                "state":     "",
                "state_name":"",
            })
            continue

        # --- Modifier loss ---
        mm = RE_MOD_LOSE.match(msg)
        if mm:
            ability_rows.append({
                "timestamp": ts,
                "event":     "modifier_loss",
                "actor":     strip_npc_prefix(mm.group("actor")),
                "target":    "",
                "ability":   "",
                "modifier":  mm.group("modifier"),
                "source":    "",
                "value":     "",
                "unit":      "",
                "state":     "",
                "state_name":"",
            })
            continue

        # --- Game state ---
        mm = RE_GAME_STATE.match(msg)
        if mm:
            state = mm.group("state")
            ability_rows.append({
                "timestamp": ts,
                "event":     "game_state",
                "actor":     "",
                "target":    "",
                "ability":   "",
                "modifier":  "",
                "source":    "",
                "value":     state,
                "unit":      "",
                "state":     state,
                "state_name": GAME_STATES.get(state, "unknown"),
            })
            continue

    return combat_rows, economy_rows, ability_rows


def parse_blocks(lines: list[str]) -> list[dict]:
    block_rows = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        hm = BLOCK_HEADER_RE.match(line)
        if not hm:
            i += 1
            continue

        block_type    = hm.group("block_type")
        block_id      = hm.group("block_id")
        declared_type = hm.group("declared_type")

        # Collect key-value lines until blank or next block/timestamped line
        i += 1
        data: dict = {}
        assist_players: list = []
        while i < len(lines):
            nxt = lines[i].rstrip()
            stripped = nxt.strip()
            if not stripped:
                i += 1
                break
            if BLOCK_HEADER_RE.match(stripped) or TIME_RE.match(stripped):
                break
            km = KEY_VALUE_RE.match(stripped)
            if km:
                key = km.group("key")
                val = km.group("value").strip()
                if key == "assist_players":
                    assist_players.append(val)
                else:
                    data[key] = val
            i += 1

        block_rows.append({
            "block_type":            block_type,
            "block_id":              block_id,
            "declared_type":         declared_type,
            "timestamp_sec":         data.get("timestamp", ""),
            "attacker_name":         data.get("attacker_name", ""),
            "target_name":           data.get("target_name", ""),
            "source_name":           (data.get("sourcename")
                                      or data.get("inflictor_name")
                                      or data.get("damage_source_name", "")),
            "value":                 data.get("value", ""),
            "health":                data.get("health", ""),
            "ability_level":         data.get("ability_level", ""),
            "gold_reason":           data.get("gold_reason", ""),
            "xp_reason":             data.get("xp_reason", ""),
            "modifier_type":         data.get("modifier_type", ""),
            "modifier_duration":     data.get("modifier_duration", ""),
            "modifier_elapsed":      data.get("modifier_elapsed_duration", ""),
            "stack_count":           data.get("stack_count", ""),
            "slow_duration":         data.get("slow_duration", ""),
            "attacker_team":         data.get("attacker_team", ""),
            "target_team":           data.get("target_team", ""),
            "is_attacker_hero":      data.get("is_attacker_hero", ""),
            "is_target_hero":        data.get("is_target_hero", ""),
            "is_attacker_illusion":  data.get("is_attacker_illusion", ""),
            "is_target_illusion":    data.get("is_target_illusion", ""),
            "is_visible_radiant":    data.get("is_visible_radiant", ""),
            "is_visible_dire":       data.get("is_visible_dire", ""),
            "assist_players":        ",".join(assist_players),
        })

    return block_rows

def parse_file(path: Path,
               latency_intervals=None,
               experiment_anchor_dt=None,
               team_alias=None):
    raw = path.read_bytes()
    if raw[:2] in (b'\xff\xfe', b'\xfe\xff'):
        text = raw.decode("utf-16")
    else:
        text = raw.decode("utf-8", errors="replace")

    lines = text.splitlines()
    combat_rows, economy_rows, ability_rows = parse_lines(lines)
    block_rows = parse_blocks(lines)

    if latency_intervals and experiment_anchor_dt and team_alias:
        team_map = build_team_map(lines, block_rows)
        intervals_by_team = normalize_latency_intervals(latency_intervals, team_alias)

        combat_rows = enrich_combat_rows(
            combat_rows, team_map, intervals_by_team, experiment_anchor_dt
        )
        economy_rows = enrich_economy_rows(
            economy_rows, team_map, intervals_by_team, experiment_anchor_dt
        )

    return combat_rows, economy_rows, ability_rows, block_rows


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(path: Path, fieldnames: list, rows: list):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main — raw_data/ → processed/ folder structure
# ---------------------------------------------------------------------------

def main():
    script_dir = Path(__file__).parent
    raw_dir = script_dir / "raw_data"
    processed_dir = script_dir / "processed"
    raw_dir.mkdir(exist_ok=True)
    processed_dir.mkdir(exist_ok=True)

    if len(sys.argv) >= 2:
        log_files = [Path(sys.argv[1])]
    else:
        log_files = sorted(raw_dir.glob("*.txt"))
        if not log_files:
            print(f"No .txt files found in {raw_dir}")
            return

    for log_path in log_files:
        if not log_path.exists():
            print(f"File not found: {log_path}")
            continue

        stem = log_path.stem
        print(f"Parsing {log_path.name} ...")

        combat_rows, economy_rows, ability_rows, block_rows = parse_file(log_path)

        write_csv(
            processed_dir / f"{stem}_combat.csv",
            [
                "timestamp", "event_wallclock", "event",
                "actor", "actor_team", "actor_latency_ms",
                "target", "target_team", "target_latency_ms",
                "source", "value", "unit", "hp_before", "hp_after"
            ],
            combat_rows,
        )

        write_csv(
            processed_dir / f"{stem}_economy.csv",
            [
                "timestamp", "event_wallclock", "event",
                "actor", "actor_team", "actor_latency_ms",
                "item", "value", "unit"
            ],
            economy_rows,
        )

        write_csv(
            processed_dir / f"{stem}_abilities.csv",
            [
                "timestamp", "event", "actor", "target", "ability",
                "modifier", "source", "value", "unit", "state", "state_name"
            ],
            ability_rows,
        )

        write_csv(
            processed_dir / f"{stem}_blocks.csv",
            [
                "block_type", "block_id", "declared_type", "timestamp_sec",
                "attacker_name", "target_name", "source_name", "value", "health",
                "ability_level", "gold_reason", "xp_reason",
                "modifier_type", "modifier_duration", "modifier_elapsed", "stack_count",
                "slow_duration", "attacker_team", "target_team",
                "is_attacker_hero", "is_target_hero",
                "is_attacker_illusion", "is_target_illusion",
                "is_visible_radiant", "is_visible_dire", "assist_players"
            ],
            block_rows,
        )

        print(
            f"  → {len(combat_rows)} combat events, "
            f"{len(economy_rows)} economy events, "
            f"{len(ability_rows)} ability events, "
            f"{len(block_rows)} block events"
        )


if __name__ == "__main__":
    main()
