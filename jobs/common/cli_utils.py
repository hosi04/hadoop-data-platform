import argparse


def add_partition_date_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--partition-date",
        required=True,
        help="Target partition date in YYYY-MM-DD format",
    )
