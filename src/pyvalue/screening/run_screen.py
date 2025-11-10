"""
Apply YAML-defined screening criteria to stored metric values.
Author: Emre Tezel
"""

from __future__ import annotations

import argparse
from typing import List

from pyvalue.ingestion import Session
from pyvalue.screening import apply_screen, load_screening_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a screening definition against the database.")
    parser.add_argument("config", help="Path to screening YAML file.")
    parser.add_argument("screen_name", help="Name of the screen to run.")
    return parser.parse_args()


def format_result(result: dict) -> List[str]:
    lines = [f"{result['stock'].symbol} ({result['stock'].name or 'N/A'})"]
    for detail in result["filters"]:
        lines.append(
            f"  - {detail['metric']} {detail['operator']} {detail['threshold']}: "
            f"{detail['value']} (as of {detail['data_from_date']})"
        )
    return lines


def main():
    args = parse_args()
    config = load_screening_config(args.config)
    screen = config.get(args.screen_name)

    with Session() as session:
        results = apply_screen(session, screen)

    if not results:
        print("No stocks matched the provided screening criteria.")
        return

    for result in results:
        for line in format_result(result):
            print(line)


if __name__ == "__main__":
    main()
