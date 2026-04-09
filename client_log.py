import csv
import re
import sys
from pathlib import Path

LOG_RE = re.compile(
    r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]\s+"
    r"\[(?P<logger>[^\]]+)\]\s+"
    r"(?P<message>.*)$"
)

PATTERNS = [
    ("log_file_name", re.compile(r"^Log file name: (?P<log_file_name>.+)$")),
    ("log_file_path", re.compile(r"^Log file path: (?P<log_file_path>.+)$")),
    ("controller_start", re.compile(
        r"^Controller starting: client_name=(?P<client_name>[^,]+), "
        r"default_server_port=(?P<default_server_port>\d+), "
        r"connect_timeout=(?P<connect_timeout>[0-9.]+), "
        r"targets=(?P<targets>\[.*\])$"
    )),
    ("user_command", re.compile(r"^User command: (?P<command_text>.+)$")),
    ("stop_requested", re.compile(r"^Stop requested by user\.$")),
    ("quit_requested", re.compile(r"^Quit requested by user\.$")),
    ("process_exiting", re.compile(r"^Process exiting\.$")),
    ("schedule_not_running", re.compile(r"^Schedule is not running\.$")),

    ("connecting_to_servers", re.compile(r"^Connecting to servers:$")),
    ("connect_success", re.compile(
        r"^✅\s+(?P<server_ip>\d{1,3}(?:\.\d{1,3}){3}):(?P<server_port>\d+)\s+"
        r"\(#\s*(?P<server_number>\d+),\s+Team\s+(?P<team>[AB])\)$"
    )),
    ("connect_failed", re.compile(
        r"^❌\s+(?P<server_ip>\d{1,3}(?:\.\d{1,3}){3}):(?P<server_port>\d+)\s+\((?P<status>.+)\)$"
    )),

    ("connected_servers_header", re.compile(r"^Connected servers by team:$")),
    ("team_count", re.compile(r"^Team\s+(?P<team>[AB])\s+\((?P<count>\d+)/(?P<capacity>\d+)\):$")),
    ("team_member", re.compile(
        r"^(?P<server_number>\d+)\)\s+(?P<server_ip>\d{1,3}(?:\.\d{1,3}){3}):(?P<server_port>\d+)$"
    )),
    ("moved_server", re.compile(r"^Moved server #(?P<server_number>\d+) to Team (?P<team>[AB])\.$")),

    ("experiment_setup", re.compile(
        r"^Starting experiment setup: mode=(?P<mode>\w+), duration=(?P<duration_sec>[0-9.]+), "
        r"latency_values=(?P<latency_values>\[.*\]), teams=(?P<teams>\[.*\])$"
    )),
    ("experiment_setup_actions", re.compile(r"^Starting experiment setup: (?P<actions>.+)$")),
    ("experiment_start", re.compile(
        r"^EXPERIMENT START \| mode=(?P<mode>\w+) \| duration_sec=(?P<duration_sec>[0-9.]+) "
        r"\| latency_values=(?P<latency_values>\[.*\]) \| teams=(?P<teams>\[.*\])$"
    )),
    ("experiment_cycle_start", re.compile(
        r"^EXPERIMENT CYCLE START \| cycle=(?P<cycle>\d+) \| mode=(?P<mode>\w+)$"
    )),
    ("experiment_cycle_end", re.compile(
        r"^EXPERIMENT CYCLE END \| cycle=(?P<cycle>\d+) \| mode=(?P<mode>\w+)$"
    )),
    ("experiment_end", re.compile(r"^EXPERIMENT END \| mode=(?P<mode>\w+)$")),
    ("experiment_mode_started", re.compile(r"^Experiment mode started: (?P<mode>\w+)\.$")),
    ("experiment_stopped", re.compile(r"^Experiment stopped\.$")),
    ("cleanup_resources", re.compile(r"^Cleaning up experiment resources\.\.\.$")),

    ("team_order", re.compile(r"^Team order per latency value: (?P<team_order>\[.*\])$")),
    ("team_alternation", re.compile(r"^Team A and Team B will alternate on each latency value\.$")),
    ("duration_per_latency", re.compile(r"^Duration per latency value: (?P<duration_sec>[0-9.]+) sec$")),
    ("latency_sequence", re.compile(r"^Latency sequence: (?P<latency_values>\[.*\])$")),
    ("stop_hint", re.compile(r"^Type 'stop' to stop experiment\.$")),

    ("inactive_team_stop", re.compile(
        r"^Active team is (?P<team>[AB])\. Sending STOP to inactive team\(s\): (?P<inactive_teams>\[.*\])$"
    )),

    ("latency_step_start", re.compile(
        r"^LATENCY STEP START \| team=(?P<team>[AB]) \| latency_ms=(?P<latency_ms>\d+) \| duration_sec=(?P<duration_sec>[0-9.]+)$"
    )),
    ("latency_step_end", re.compile(
        r"^LATENCY STEP END \| team=(?P<team>[AB]) \| latency_ms=(?P<latency_ms>\d+) \| status=(?P<status>\w+)$"
    )),

    ("server_set_latency", re.compile(
        r"^\((?P<server_ip>\d{1,3}(?:\.\d{1,3}){3}):(?P<server_port>\d+)\)\s+\[Team\s+(?P<team>[AB])\]\s+"
        r"SET_LATENCY\s+(?P<latency_ms>\d+)\s+->\s+(?P<response>.+)$"
    )),
    ("server_stop_inactive", re.compile(
        r"^\((?P<server_ip>\d{1,3}(?:\.\d{1,3}){3}):(?P<server_port>\d+)\)\s+STOP\s+\(inactive team\)\s+->\s+(?P<response>.+)$"
    )),
    ("server_generic_response", re.compile(
        r"^\[(?P<server_logger>tcp_server):(?P<server_ip>\d{1,3}(?:\.\d{1,3}){3}):(?P<server_port>\d+)\]\s+(?P<response>.+)$"
    )),
]

CATEGORY_MAP = {
    "log_file_name": "session",
    "log_file_path": "session",
    "controller_start": "session",
    "user_command": "command",
    "stop_requested": "command",
    "quit_requested": "command",
    "process_exiting": "session",
    "schedule_not_running": "command",
    "connecting_to_servers": "connection",
    "connect_success": "connection",
    "connect_failed": "connection",
    "connected_servers_header": "team_state",
    "team_count": "team_state",
    "team_member": "team_state",
    "moved_server": "team_state",
    "experiment_setup": "experiment",
    "experiment_setup_actions": "experiment",
    "experiment_start": "experiment",
    "experiment_cycle_start": "experiment",
    "experiment_cycle_end": "experiment",
    "experiment_end": "experiment",
    "experiment_mode_started": "experiment",
    "experiment_stopped": "experiment",
    "cleanup_resources": "experiment",
    "team_order": "experiment",
    "team_alternation": "experiment",
    "duration_per_latency": "experiment",
    "latency_sequence": "experiment",
    "stop_hint": "experiment",
    "inactive_team_stop": "latency",
    "latency_step_start": "latency",
    "latency_step_end": "latency",
    "server_set_latency": "server_response",
    "server_stop_inactive": "server_response",
    "server_generic_response": "server_response",
}


def parse_message(message: str):
    stripped = message.strip()
    for event_type, pattern in PATTERNS:
        m = pattern.match(stripped)
        if m:
            return event_type, CATEGORY_MAP.get(event_type, "other"), m.groupdict()
    return "generic", "other", {}


def parse_file(log_path: Path):
    events = []
    latency_rows = []
    server_rows = []

    current_mode = ""
    current_cycle = ""
    current_latency_values = ""
    current_teams = ""

    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line.strip():
                continue

            m = LOG_RE.match(line)
            if not m:
                events.append({
                    "timestamp": "",
                    "logger": "",
                    "category": "other",
                    "event_type": "unparsed",
                    "message": line.strip(),
                })
                continue

            ts = m.group("timestamp")
            logger = m.group("logger")
            message = m.group("message")

            event_type, category, data = parse_message(message)

            # keep context for cleaner latency rows
            if event_type in {"experiment_setup", "experiment_start"}:
                current_mode = data.get("mode", current_mode)
                current_latency_values = data.get("latency_values", current_latency_values)
                current_teams = data.get("teams", current_teams)
            elif event_type == "experiment_cycle_start":
                current_cycle = data.get("cycle", current_cycle)

            # general events
            if category in {"session", "command", "connection", "team_state", "experiment", "other"}:
                events.append({
                    "timestamp": ts,
                    "logger": logger,
                    "category": category,
                    "event_type": event_type,
                    "command_text": data.get("command_text", ""),
                    "mode": data.get("mode", current_mode),
                    "cycle": data.get("cycle", current_cycle),
                    "team": data.get("team", ""),
                    "server_ip": data.get("server_ip", ""),
                    "server_port": data.get("server_port", ""),
                    "latency_ms": data.get("latency_ms", ""),
                    "duration_sec": data.get("duration_sec", ""),
                    "status": data.get("status", ""),
                    "response": data.get("response", ""),
                    "message": message,
                })

            # latency timeline
            if event_type in {"latency_step_start", "latency_step_end"}:
                latency_rows.append({
                    "timestamp": ts,
                    "logger": logger,
                    "event_type": event_type,
                    "mode": current_mode,
                    "cycle": current_cycle,
                    "team": data.get("team", ""),
                    "latency_ms": data.get("latency_ms", ""),
                    "duration_sec": data.get("duration_sec", ""),
                    "status": data.get("status", ""),
                    "latency_values": current_latency_values,
                    "teams": current_teams,
                    "message": message,
                })

            # server actions
            if event_type in {"server_set_latency", "server_stop_inactive", "server_generic_response"}:
                command = ""
                if event_type == "server_set_latency":
                    command = "SET_LATENCY"
                elif event_type == "server_stop_inactive":
                    command = "STOP"
                else:
                    response_upper = data.get("response", "").upper()
                    if "OBS" in response_upper:
                        command = "OBS"
                    elif "APP" in response_upper:
                        command = "APP"
                    elif "LATENCY INJECTOR" in response_upper:
                        command = "LATENCY_INJECTOR"
                    elif "BYE" in response_upper:
                        command = "QUIT"
                    else:
                        command = "SERVER_RESPONSE"

                server_rows.append({
                    "timestamp": ts,
                    "logger": logger,
                    "event_type": event_type,
                    "server_ip": data.get("server_ip", ""),
                    "server_port": data.get("server_port", ""),
                    "team": data.get("team", ""),
                    "command": command,
                    "latency_ms": data.get("latency_ms", ""),
                    "response": data.get("response", ""),
                    "message": message,
                })

    return events, latency_rows, server_rows


def write_csv(path: Path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_latency_intervals(latency_rows):
    intervals = []
    open_steps = {}

    for row in latency_rows:
        team = row.get("team", "")
        latency_ms = row.get("latency_ms", "")
        event_type = row.get("event_type", "")
        timestamp = row.get("timestamp", "")
        mode = row.get("mode", "")
        cycle = row.get("cycle", "")
        duration_sec = row.get("duration_sec", "")
        status = row.get("status", "")

        key = (team, latency_ms, cycle, mode)

        if event_type == "latency_step_start":
            open_steps[key] = {
                "team": team,
                "latency_ms": latency_ms,
                "start_time": timestamp,
                "end_time": "",
                "duration_sec": duration_sec,
                "status": "",
                "mode": mode,
                "cycle": cycle,
            }

        elif event_type == "latency_step_end":
            if key in open_steps:
                step = open_steps.pop(key)
                step["end_time"] = timestamp
                step["status"] = status
                intervals.append(step)

    # anything left open becomes an incomplete interval
    for step in open_steps.values():
        step["status"] = "open"
        intervals.append(step)

    return intervals

def extract_latency_intervals_from_log(log_path: Path):
    _, latency_rows, _ = parse_file(log_path)
    return build_latency_intervals(latency_rows)

def main():
    # Folder structure relative to this script
    script_dir = Path(__file__).parent
    raw_dir       = script_dir / "raw_data"
    processed_dir = script_dir / "processed"

    # Create folders if they don't exist yet
    raw_dir.mkdir(exist_ok=True)
    processed_dir.mkdir(exist_ok=True)

    # If a specific file is passed, use it — otherwise process all .txt in raw_data/
    if len(sys.argv) >= 2:
        log_files = [Path(sys.argv[1])]
    else:
        log_files = sorted(raw_dir.glob("tcp_client_log_*.txt")) + sorted(raw_dir.glob("tcp_client_log_*.log"))
        if not log_files:
            print(f"No .txt or .log files found in {raw_dir}")
            sys.exit(0)

    for log_path in log_files:
        if not log_path.exists():
            print(f"File not found: {log_path}")
            continue

        prefix = log_path.stem
        events, latency_rows, server_rows = parse_file(log_path)

        write_csv(
            processed_dir / f"{prefix}_events.csv",
            ["timestamp", "logger", "category", "event_type", "command_text",
             "mode", "cycle", "team", "server_ip", "server_port",
             "latency_ms", "duration_sec", "status", "response", "message"],
            events,
        )
        write_csv(
            processed_dir / f"{prefix}_latency_timeline.csv",
            ["timestamp", "logger", "event_type", "mode", "cycle", "team",
             "latency_ms", "duration_sec", "status", "latency_values", "teams", "message"],
            latency_rows,
        )
        write_csv(
            processed_dir / f"{prefix}_server_actions.csv",
            ["timestamp", "logger", "event_type", "server_ip", "server_port",
             "team", "command", "latency_ms", "response", "message"],
            server_rows,
        )

        latency_intervals = build_latency_intervals(latency_rows)

        write_csv(
        processed_dir / f"{prefix}_latency_intervals.csv",
        ["team", "latency_ms", "start_time", "end_time", "duration_sec", "status", "mode", "cycle"],
        latency_intervals,
        )

        print(f"[{log_path.name}] → {len(events)} events, {len(latency_rows)} latency rows, {len(server_rows)} server rows")

    print(f"\nCSVs written to: {processed_dir}")


if __name__ == "__main__":
    main()
