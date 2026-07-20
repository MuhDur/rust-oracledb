#!/usr/bin/env python3
"""Validate and query the cargo-fuzz target manifest."""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "targets.toml"
FUZZ_TARGETS = ROOT / "fuzz_targets"
CARGO_TOML = ROOT / "Cargo.toml"

REQUIRED_FIELDS = {
    "name",
    "owner",
    "parser_entry",
    "risk_tier",
    "corpus_dir",
    "dictionary",
    "max_input_bytes",
    "timeout_seconds",
    "rss_limit_mb",
    "malloc_limit_mb",
    "lane",
    "iterations",
    "max_total_time_seconds",
}


def load_toml(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def load_manifest() -> dict:
    return load_toml(MANIFEST)


def targets(manifest: dict) -> list[dict]:
    return manifest.get("targets", [])


def target_files() -> set[str]:
    return {path.stem for path in FUZZ_TARGETS.glob("*.rs")}


def cargo_bins() -> dict[str, str]:
    cargo = load_toml(CARGO_TOML)
    bins = {}
    for bin_config in cargo.get("bin", []):
        name = bin_config.get("name")
        path = bin_config.get("path")
        if name and path:
            bins[name] = Path(path).stem
    return bins


def validate() -> int:
    manifest = load_manifest()
    manifest_targets = targets(manifest)
    errors: list[str] = []

    tier_budgets = manifest.get("tier_budgets", {})
    valid_tiers = set(tier_budgets)
    if not valid_tiers:
        errors.append("targets.toml has no [tier_budgets] table")

    names: list[str] = []
    for index, target in enumerate(manifest_targets, start=1):
        missing = sorted(REQUIRED_FIELDS.difference(target))
        if missing:
            errors.append(f"target #{index} missing fields: {', '.join(missing)}")
            continue
        name = target["name"]
        names.append(name)
        if target["risk_tier"] not in valid_tiers:
            errors.append(f"{name}: unknown risk_tier {target['risk_tier']!r}")
        for field in (
            "max_input_bytes",
            "timeout_seconds",
            "rss_limit_mb",
            "malloc_limit_mb",
            "iterations",
            "max_total_time_seconds",
        ):
            value = target[field]
            if not isinstance(value, int) or value <= 0:
                errors.append(f"{name}: {field} must be a positive integer")
        if target["dictionary"]:
            # Fail closed here, not as libFuzzer's cryptic runtime
            # "ParseDictionaryFile: file does not exist or is empty" exit.
            dict_path = ROOT / target["dictionary"]
            if not dict_path.is_file() or dict_path.stat().st_size == 0:
                errors.append(
                    f"{name}: dictionary {target['dictionary']!r} missing or "
                    f"empty (resolved against the fuzz dir: {dict_path})"
                )

    duplicate_names = sorted({name for name in names if names.count(name) > 1})
    if duplicate_names:
        errors.append(f"duplicate targets: {', '.join(duplicate_names)}")

    manifest_names = set(names)
    file_names = target_files()
    if manifest_names != file_names:
        missing_from_manifest = sorted(file_names - manifest_names)
        missing_files = sorted(manifest_names - file_names)
        if missing_from_manifest:
            errors.append(
                "fuzz_targets/*.rs missing from targets.toml: "
                + ", ".join(missing_from_manifest)
            )
        if missing_files:
            errors.append(
                "targets.toml entries missing fuzz target files: "
                + ", ".join(missing_files)
            )

    bins = cargo_bins()
    bin_names = set(bins)
    if manifest_names != bin_names:
        missing_from_cargo = sorted(manifest_names - bin_names)
        extra_cargo_bins = sorted(bin_names - manifest_names)
        if missing_from_cargo:
            errors.append(
                "targets.toml entries missing Cargo [[bin]] entries: "
                + ", ".join(missing_from_cargo)
            )
        if extra_cargo_bins:
            errors.append(
                "Cargo [[bin]] entries missing from targets.toml: "
                + ", ".join(extra_cargo_bins)
            )
    for name, stem in bins.items():
        if name != stem:
            errors.append(f"{name}: Cargo [[bin]] path stem {stem!r} does not match name")

    if errors:
        for error in errors:
            print(f"manifest drift: {error}", file=sys.stderr)
        return 1

    print(f"targets.toml ok: {len(manifest_targets)} targets")
    return 0


def iter_selected(manifest: dict, tier: str | None) -> list[dict]:
    selected = targets(manifest)
    if tier:
        selected = [target for target in selected if target["risk_tier"] == tier]
    return selected


def print_list(tier: str | None) -> int:
    manifest = load_manifest()
    for target in iter_selected(manifest, tier):
        print(target["name"])
    return 0


def print_flags(name: str) -> int:
    manifest = load_manifest()
    by_name = {target["name"]: target for target in targets(manifest)}
    target = by_name.get(name)
    if target is None:
        print(f"unknown target: {name}", file=sys.stderr)
        return 1

    flags = [
        f"-runs={target['iterations']}",
        f"-max_total_time={target['max_total_time_seconds']}",
        f"-max_len={target['max_input_bytes']}",
        f"-timeout={target['timeout_seconds']}",
        f"-rss_limit_mb={target['rss_limit_mb']}",
        f"-malloc_limit_mb={target['malloc_limit_mb']}",
    ]
    if target["dictionary"]:
        # The manifest path is relative to the fuzz dir, but callers invoke
        # `cargo fuzz run` from the crate dir (and locally from anywhere), and
        # libFuzzer resolves -dict= against its own cwd — emit an absolute
        # path so the flag is correct from any invocation directory.
        flags.append(f"-dict={(ROOT / target['dictionary']).resolve()}")
    print(" ".join(flags))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="validate manifest, fuzz target files, and Cargo bins")
    list_parser = sub.add_parser("list", help="list manifest target names")
    list_parser.add_argument("--tier", choices=("required", "canary", "soak", "release"))
    flags_parser = sub.add_parser("flags", help="print libFuzzer flags for one target")
    flags_parser.add_argument("target")

    args = parser.parse_args()
    if args.command == "check":
        return validate()
    if args.command == "list":
        return print_list(args.tier)
    if args.command == "flags":
        return print_flags(args.target)
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
