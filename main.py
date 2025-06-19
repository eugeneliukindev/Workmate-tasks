import argparse
import csv
import operator
import re
from collections.abc import Iterable
from os import PathLike
from typing import Self

from tabulate import tabulate

FILTER_OPS = {
    ">": operator.gt,
    "<": operator.lt,
    "=": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
    "!=": operator.ne,
}

AGG_OPS = {
    "avg": lambda x: sum(x) / len(x),
    "min": min,
    "max": max,
    "sum": sum,
    "count": len,
}


class CSVProcessor:
    def __init__(self, filename: str | PathLike[str]):
        self._filename = filename
        self._headers: list[str] = []
        self._source_data: list[list[str]] = []
        self._processed_data: list[list[str]] | int = []
        self._parse_csv_file()

    @property
    def filename(self) -> str | PathLike[str]:
        return self._filename

    @property
    def headers(self) -> list[str]:
        return self._headers

    @property
    def source_data(self) -> list[list[str]]:
        return self._source_data

    @property
    def processed_data(self) -> list[list[str]] | int:
        return self._processed_data

    def _parse_csv_file(self) -> None:
        with open(self._filename) as f:
            reader = csv.reader(f)
            self._headers = next(reader)
            self._source_data = [row for row in reader]
            self._processed_data = self._source_data.copy()

    def _validate_column(self, column: str) -> None:
        if column not in self._headers:
            raise ValueError(f"Column {column!r} not found in CSV headers")

    def _validate_process_data_iterable(self) -> None:
        if not isinstance(self._processed_data, Iterable):
            raise ValueError("Processed data must be an iterable, maybe u use aggregation func?")

    def filter(self, column: str, op: str, value: str) -> Self:
        self._validate_process_data_iterable()
        self._validate_column(column)
        col_index = self._headers.index(column)
        op_func = FILTER_OPS.get(op)
        if op_func is None:
            raise ValueError(f"Operation {op!r} not supported")
        try:
            numeric_value = float(value)
            self._processed_data = [
                row
                for row in self._processed_data  # type: ignore[union-attr]
                if op_func(float(row[col_index]), numeric_value)
            ]
        except ValueError:
            self._processed_data = [row for row in self._processed_data if op_func(row[col_index], value)]  # type: ignore[union-attr]

        return self

    @staticmethod
    def _convert_to_number(value: str) -> int | float:
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                raise ValueError(f"Value {value!r} cannot be converted to a number")

    def aggregate(self, column: str, agg_operation: str) -> Self:
        self._validate_column(column)
        self._validate_process_data_iterable()
        agg_func = AGG_OPS.get(agg_operation)
        if agg_func is None:
            raise ValueError(f"Invalid aggregation operation {agg_operation!r}")

        col_index = self._headers.index(column)
        numbers = []

        for row in self._processed_data:  # type: ignore[union-attr]
            try:
                number = self._convert_to_number(value=row[col_index])
                numbers.append(number)
            except ValueError:
                raise ValueError(f"Non-numeric value {row[col_index]!r} found in column {column!r}")

        self._processed_data = agg_func(numbers) if numbers else []  # type: ignore[operator]
        return self


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CSV Processor: filtering and aggregation",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("file", help="CSV file to process")

    filter_group = parser.add_argument_group("Filtering options")
    filter_group.add_argument(
        "--filter",
        action="append",
        type=str,
        metavar="CONDITION",
        help=f"Filter condition (e.g. 'price>=149'). Operators: {', '.join(FILTER_OPS.keys())}",
    )

    agg_group = parser.add_argument_group("Aggregation options")
    agg_group.add_argument(
        "--agg",
        type=str,
        metavar="AGGREGATION",
        help=f"Aggregation operation (e.g. 'price=max'). Operations: {', '.join(AGG_OPS.keys())}",
    )

    return parser.parse_args()


def _parse_filter_condition(condition: str) -> tuple[str, str, str]:
    match = re.match(r"^([a-zA-Z_]+)(>=|<=|!=|>|<|=)([^><=!]+)$", condition)
    if not match:
        raise ValueError(f"Invalid filter condition format: {condition!r}\n")
    return match.groups()  # type: ignore[return-value]


def _parse_aggregation(agg_str: str) -> tuple[str, str]:
    agg_parts = agg_str.split("=")
    if len(agg_parts) != 2:
        raise ValueError(f"Invalid aggregation format: {agg_str!r}")
    return agg_parts[0], agg_parts[1]


def apply_filters(processor: CSVProcessor, conditions: list[str]) -> str:
    for condition in conditions:
        column, op, value = _parse_filter_condition(condition)
        processor.filter(column, op, value)
    return tabulate(processor.processed_data, headers=processor.headers, tablefmt="grid")  # type: ignore[arg-type]


def apply_aggregation(processor: CSVProcessor, condition: str) -> str:
    column, agg_op = _parse_aggregation(condition)
    processor.aggregate(column, agg_op)

    if not processor.processed_data:
        return "No data to aggregate"
    return tabulate([[agg_op], [processor.processed_data]], tablefmt="grid")


def main() -> None:
    args = parse_args()
    processor = CSVProcessor(args.file)
    msg = ""

    if args.filter:
        msg = apply_filters(processor=processor, conditions=args.filter)

    if args.agg:
        msg = apply_aggregation(processor=processor, condition=args.agg)

    print(msg)


if __name__ == "__main__":
    main()
