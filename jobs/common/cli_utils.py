import argparse
from datetime import datetime


def add_partition_date_arg(parser: argparse.ArgumentParser, required: bool = False) -> None:
    parser.add_argument(
        "--partition-date",
        required=required,
        default=None if required else datetime.today().strftime("%Y-%m-%d"),
        help="Target partition date in YYYY-MM-DD format" + ("" if required else " (default: today)"),
    )
