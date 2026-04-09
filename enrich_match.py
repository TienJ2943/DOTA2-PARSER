from pathlib import Path
from datetime import datetime
import re
from server_log import extract_player_identity_from_server_log, find_latest_server_log

from client_log import (
    parse_file as parse_client_log,
    build_latency_intervals,
)
from combatlog import parse_file as parse_combatlog, write_csv

def load_identity_map_from_server_log(raw_dir: Path):
    server_log_path = find_latest_server_log(raw_dir)
    if server_log_path is None:
        print(f"No server log found in {raw_dir}; continuing without player identity enrichment.")
        return {}

    print(f"Using server log: {server_log_path.name}")

    identity_rows = extract_player_identity_from_server_log(server_log_path)

    # hero_name -> identity
    identity_map = {}
    for row in identity_rows:
        hero = (row.get("hero_name") or "").strip()
        if not hero:
            continue
        identity_map[hero] = row

    return identity_map


def attach_identity_to_combat_rows(rows, identity_map):
    for row in rows:
        actor = row.get("actor", "")
        target = row.get("target", "")

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
        actor = row.get("actor", "")
        actor_info = identity_map.get(actor, {})

        row["actor_steamid"] = actor_info.get("steamid", "")
        row["actor_steam_name"] = actor_info.get("steam_name", "")
        row["actor_ip"] = actor_info.get("ip", "")
        row["actor_is_bot"] = actor_info.get("is_bot", "")

    return rows


def find_latest_tcp_log(raw_dir: Path) -> Path | None:
    tcp_logs = sorted(raw_dir.glob("tcp_client_log*.txt"))
    if not tcp_logs:
        return None
    return max(tcp_logs, key=lambda p: p.stat().st_mtime)


def find_combatlogs(raw_dir: Path) -> list[Path]:
    return sorted(
        p for p in raw_dir.glob("*.txt")
        if p.name.startswith("combatlog")
    )


def extract_experiment_anchor_from_tcp_log(tcp_log_path: Path) -> datetime | None:
    """
    Find the first EXPERIMENT START timestamp directly from the tcp client log.
    """
    log_re = re.compile(
        r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]\s+\[(?P<logger>[^\]]+)\]\s+(?P<message>.*)$"
    )

    with tcp_log_path.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            m = log_re.match(line)
            if not m:
                continue

            message = m.group("message")
            if message.startswith("EXPERIMENT START |"):
                return datetime.strptime(m.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f")

    return None


def main():
    script_dir = Path(__file__).parent
    raw_dir = script_dir / "raw_data"
    processed_dir = script_dir / "processed"

    raw_dir.mkdir(exist_ok=True)
    processed_dir.mkdir(exist_ok=True)

    team_alias = {
        "A": "RADIANT",
        "B": "DIRE",
    }

    identity_map = load_identity_map_from_server_log(raw_dir)

    tcp_log_path = find_latest_tcp_log(raw_dir)
    if tcp_log_path is None:
        print(f"No tcp_client_log*.txt found in {raw_dir}")
        return

    combatlog_files = find_combatlogs(raw_dir)
    if not combatlog_files:
        print(f"No combatlog*.txt found in {raw_dir}")
        return

    print(f"Using tcp log: {tcp_log_path.name}")

    # Build latency intervals directly in memory
    tcp_events, tcp_latency_rows, tcp_server_rows = parse_client_log(tcp_log_path)
    latency_intervals = build_latency_intervals(tcp_latency_rows)

    tcp_stem = tcp_log_path.stem
    write_csv(
        processed_dir / f"{tcp_stem}_events.csv",
        ["timestamp", "logger", "category", "event_type", "command_text",
        "mode", "cycle", "team", "server_ip", "server_port",
        "latency_ms", "duration_sec", "status", "response", "message"],
        tcp_events,
    )

    write_csv(
        processed_dir / f"{tcp_stem}_latency_timeline.csv",
        ["timestamp", "logger", "event_type", "mode", "cycle", "team",
        "latency_ms", "duration_sec", "status", "latency_values", "teams", "message"],
        tcp_latency_rows,
    )

    write_csv(
        processed_dir / f"{tcp_stem}_server_actions.csv",
        ["timestamp", "logger", "event_type", "server_ip", "server_port",
        "team", "command", "latency_ms", "response", "message"],
        tcp_server_rows,
    )

    write_csv(
        processed_dir / f"{tcp_stem}_latency_intervals.csv",
        ["team", "latency_ms", "start_time", "end_time", "duration_sec", "status", "mode", "cycle"],
        latency_intervals,
    )

    # Dynamically extract experiment start time from tcp log
    experiment_anchor_dt = extract_experiment_anchor_from_tcp_log(tcp_log_path)
    if experiment_anchor_dt is None:
        print(f"Could not find EXPERIMENT START in {tcp_log_path.name}")
        return

    print(f"Experiment anchor: {experiment_anchor_dt}")

    for combatlog_path in combatlog_files:
        print(f"Processing combat log: {combatlog_path.name}")

        combat_rows, economy_rows, ability_rows, block_rows = parse_combatlog(
            combatlog_path,
            latency_intervals=latency_intervals,
            experiment_anchor_dt=experiment_anchor_dt,
            team_alias=team_alias,
        )

        combat_rows = attach_identity_to_combat_rows(combat_rows, identity_map)
        economy_rows = attach_identity_to_economy_rows(economy_rows, identity_map)

        stem = combatlog_path.stem

        write_csv(
            processed_dir / f"{stem}_combat.csv",
            [
                "timestamp", "event_wallclock", "event",
                "actor", "actor_team", "actor_latency_ms",
                "actor_steamid", "actor_steam_name", "actor_ip", "actor_is_bot",
                "target", "target_team", "target_latency_ms",
                "target_steamid", "target_steam_name", "target_ip", "target_is_bot",
                "source", "value", "unit", "hp_before", "hp_after"
            ],
            combat_rows,
        )

        write_csv(
            processed_dir / f"{stem}_economy.csv",
            [
                "timestamp", "event_wallclock", "event",
                "actor", "actor_team", "actor_latency_ms",
                "actor_steamid", "actor_steam_name", "actor_ip", "actor_is_bot",
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

        print(f"Done: {stem}")

    print(f"\nAll enriched CSVs written to: {processed_dir}")


if __name__ == "__main__":
    main()