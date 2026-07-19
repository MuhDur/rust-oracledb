#!/usr/bin/env python3
"""Persist and enforce the Live nightly advisory-to-blocking state machine.

The Live lane is intentionally advisory while its known blocker is being
repaired. Three consecutive green observations re-arm it automatically. Once
re-armed, blocking is sticky: a later regression fails the lane instead of
silently demoting it again. Infrastructure skips are distinct from product-test
reds and reset a pending green streak.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = "driver-live-gate-state/v1"
OBSERVATIONS = ("green", "red", "infra_skip")
MODES = ("advisory", "blocking")
STATE_KEYS = {
    "schema",
    "mode",
    "green_streak",
    "last_observation",
    "updated_at",
}


class StateError(ValueError):
    """The persisted state is malformed or unsafe to consume."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise StateError("state must be a JSON object")
    if set(value) != STATE_KEYS:
        missing = sorted(STATE_KEYS - set(value))
        extra = sorted(set(value) - STATE_KEYS)
        raise StateError(f"state keys mismatch: missing={missing} extra={extra}")
    if value["schema"] != SCHEMA:
        raise StateError(f"unsupported schema: {value['schema']!r}")
    if value["mode"] not in MODES:
        raise StateError(f"invalid mode: {value['mode']!r}")
    streak = value["green_streak"]
    if not isinstance(streak, int) or isinstance(streak, bool) or not 0 <= streak <= 3:
        raise StateError("green_streak must be an integer from 0 through 3")
    if value["last_observation"] not in OBSERVATIONS:
        raise StateError(f"invalid observation: {value['last_observation']!r}")
    timestamp = value["updated_at"]
    if not isinstance(timestamp, str) or not timestamp.endswith("Z"):
        raise StateError("updated_at must be an RFC3339 UTC timestamp ending in Z")
    try:
        datetime.fromisoformat(timestamp.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise StateError("updated_at is not a valid RFC3339 timestamp") from exc
    if value["mode"] == "advisory" and streak >= 3:
        raise StateError("an advisory state cannot retain a three-green streak")
    return dict(value)


def initial_state(updated_at: str) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "mode": "advisory",
        "green_streak": 0,
        "last_observation": "infra_skip",
        "updated_at": updated_at,
    }


def transition(
    previous: dict[str, Any] | None, observation: str, updated_at: str
) -> dict[str, Any]:
    if observation not in OBSERVATIONS:
        raise StateError(f"invalid observation: {observation!r}")
    state = initial_state(updated_at) if previous is None else validate_state(previous)
    mode = state["mode"]
    streak = min(3, state["green_streak"] + 1) if observation == "green" else 0
    if mode == "advisory" and streak == 3:
        mode = "blocking"
    return validate_state(
        {
            "schema": SCHEMA,
            "mode": mode,
            "green_streak": streak,
            "last_observation": observation,
            "updated_at": updated_at,
        }
    )


def should_block(state: dict[str, Any]) -> bool:
    checked = validate_state(state)
    return checked["mode"] == "blocking" and checked["last_observation"] != "green"


def _load(path: Path) -> dict[str, Any]:
    try:
        return validate_state(json.loads(path.read_text()))
    except (OSError, json.JSONDecodeError, StateError) as exc:
        raise StateError(f"cannot consume {path}: {exc}") from exc


def _self_test() -> None:
    stamp = "2026-07-18T00:00:00Z"
    one = transition(None, "green", stamp)
    assert one["mode"] == "advisory" and one["green_streak"] == 1
    two = transition(one, "green", stamp)
    assert two["mode"] == "advisory" and two["green_streak"] == 2
    reset_red = transition(two, "red", stamp)
    assert reset_red["mode"] == "advisory" and reset_red["green_streak"] == 0
    reset_infra = transition(two, "infra_skip", stamp)
    assert reset_infra["mode"] == "advisory" and reset_infra["green_streak"] == 0
    three = transition(two, "green", stamp)
    assert three["mode"] == "blocking" and three["green_streak"] == 3
    assert not should_block(three)
    regression = transition(three, "red", stamp)
    assert regression["mode"] == "blocking" and should_block(regression)
    recovered = transition(regression, "green", stamp)
    assert recovered["mode"] == "blocking" and not should_block(recovered)
    try:
        validate_state({**three, "green_streak": 4})
    except StateError:
        pass
    else:
        raise AssertionError("out-of-range streak was accepted")
    print("live-gate-state: 8 deterministic transition fixtures passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("self-test")

    transition_parser = subparsers.add_parser("transition")
    transition_parser.add_argument("--previous", type=Path)
    transition_parser.add_argument("--allow-missing", action="store_true")
    transition_parser.add_argument("--observation", choices=OBSERVATIONS, required=True)
    transition_parser.add_argument("--updated-at", default=None)
    transition_parser.add_argument("--output", type=Path, required=True)

    enforce_parser = subparsers.add_parser("enforce")
    enforce_parser.add_argument("--state", type=Path, required=True)

    args = parser.parse_args()
    try:
        if args.command == "self-test":
            _self_test()
            return 0
        if args.command == "transition":
            previous = None
            if args.previous is not None:
                if args.previous.exists():
                    previous = _load(args.previous)
                elif not args.allow_missing:
                    raise StateError(f"previous state is missing: {args.previous}")
            updated_at = args.updated_at or _utc_now()
            current = transition(previous, args.observation, updated_at)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n")
            print(json.dumps(current, separators=(",", ":"), sort_keys=True))
            return 0
        state = _load(args.state)
        if should_block(state):
            print(
                f"live-gate-state: BLOCKING {state['last_observation']} after re-arm",
                file=sys.stderr,
            )
            return 1
        print(
            f"live-gate-state: {state['mode']} observation={state['last_observation']} "
            f"green_streak={state['green_streak']}"
        )
        return 0
    except StateError as exc:
        print(f"live-gate-state: invalid state: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
