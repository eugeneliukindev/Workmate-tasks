import csv
import re
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from typing import Final
from unittest.mock import patch

import pytest

from main import CSVProcessor, parse_args
from tests.conftest import NO_RESULT, ExpectationType

MOCK_PRODUCTS_PATH: Final[Path] = Path(__file__).parent / "mock_data" / "products.csv"


@pytest.fixture(scope="session")
def sample_csv_file() -> list[list[str]]:
    with open(MOCK_PRODUCTS_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        return list(reader)


@pytest.fixture(scope="function")
def processor() -> CSVProcessor:
    return CSVProcessor(filename=MOCK_PRODUCTS_PATH)


class TestCsvProcessor:
    def test_parse_csv_file(self, sample_csv_file: list[list[str]], processor: CSVProcessor) -> None:
        assert processor.headers == ["name", "brand", "price", "rating"]
        assert sample_csv_file[1:] == processor.processed_data

    @pytest.mark.parametrize(
        "column, op, value, expected_result, expectation",
        [
            (
                "price",
                ">",
                "300",
                [["phone4", "brand1", "400", "4.8"], ["phone5", "brand2", "500", "4.2"]],
                does_not_raise(),
            ),
            ("price", "<", "200", [["phone1", "brand1", "100", "4.5"]], does_not_raise()),
            (
                "price",
                ">=",
                "400",
                [["phone4", "brand1", "400", "4.8"], ["phone5", "brand2", "500", "4.2"]],
                does_not_raise(),
            ),
            ("price", "<=", "100", [["phone1", "brand1", "100", "4.5"]], does_not_raise()),
            ("price", "=", "300", [["phone3", "brand3", "300", "3.5"]], does_not_raise()),
            (
                "price",
                "!=",
                "300",
                [
                    ["phone1", "brand1", "100", "4.5"],
                    ["phone2", "brand2", "200", "4.0"],
                    ["phone4", "brand1", "400", "4.8"],
                    ["phone5", "brand2", "500", "4.2"],
                ],
                does_not_raise(),
            ),
            (
                "brand",
                "=",
                "brand1",
                [["phone1", "brand1", "100", "4.5"], ["phone4", "brand1", "400", "4.8"]],
                does_not_raise(),
            ),
            (
                "brand",
                "!=",
                "brand1",
                [
                    ["phone2", "brand2", "200", "4.0"],
                    ["phone3", "brand3", "300", "3.5"],
                    ["phone5", "brand2", "500", "4.2"],
                ],
                does_not_raise(),
            ),
            (
                "rating",
                ">",
                "4.0",
                [
                    ["phone1", "brand1", "100", "4.5"],
                    ["phone4", "brand1", "400", "4.8"],
                    ["phone5", "brand2", "500", "4.2"],
                ],
                does_not_raise(),
            ),
            ("rating", "<", "4.0", [["phone3", "brand3", "300", "3.5"]], does_not_raise()),
            # Exceptions
            (
                "nonexistent",
                ">",
                "100",
                NO_RESULT,
                pytest.raises(ValueError, match="Column 'nonexistent' not found in CSV headers"),
            ),
            (
                "price",
                "??",
                "100",
                NO_RESULT,
                pytest.raises(ValueError, match=re.escape("Operation '??' not supported")),
            ),
        ],
    )
    def test_filter(
        self,
        column: str,
        op: str,
        value: str,
        expectation: ExpectationType,
        expected_result: list[list[str]],
        sample_csv_file: list[list[str]],
        processor: CSVProcessor,
    ) -> None:
        with expectation:
            processor.filter(column=column, op=op, value=value)
            assert processor.processed_data == expected_result

    @pytest.mark.parametrize(
        "columns, operators, values, expected_result, expectation",
        [
            (
                ["price", "brand"],
                [">=", "="],
                ["200", "brand2"],
                [["phone2", "brand2", "200", "4.0"], ["phone5", "brand2", "500", "4.2"]],
                does_not_raise(),
            ),
            (
                ["brand", "rating"],
                ["=", ">"],
                ["brand1", "4.0"],
                [["phone1", "brand1", "100", "4.5"], ["phone4", "brand1", "400", "4.8"]],
                does_not_raise(),
            ),
            (
                ["price", "price"],
                ["<", ">"],
                ["500", "100"],
                [
                    ["phone2", "brand2", "200", "4.0"],
                    ["phone3", "brand3", "300", "3.5"],
                    ["phone4", "brand1", "400", "4.8"],
                ],
                does_not_raise(),
            ),
            # Exceptions
            (
                ["nonexistent", "price"],
                [">", "<"],
                ["100", "300"],
                NO_RESULT,
                pytest.raises(ValueError, match="Column 'nonexistent' not found in CSV headers"),
            ),
        ],
    )
    def test_filters(
        self,
        columns: list[str],
        operators: list[str],
        values: list[str],
        expected_result: list[list[str]],
        expectation: ExpectationType,
        processor: CSVProcessor,
    ) -> None:
        with expectation:
            for column, operator, value in zip(columns, operators, values):
                processor.filter(column=column, op=operator, value=value)
            assert processor.processed_data == expected_result

    @pytest.mark.parametrize(
        "column, agg_operation, expected_result, expectation",
        [
            ("price", "max", 500, does_not_raise()),
            ("price", "min", 100, does_not_raise()),
            ("price", "sum", 1500, does_not_raise()),
            ("price", "avg", 300, does_not_raise()),
            ("rating", "max", 4.8, does_not_raise()),
            # Exceptions
            (
                "non-exist",
                "max",
                NO_RESULT,
                pytest.raises(ValueError, match="Column 'non-exist' not found in CSV headers"),
            ),
            (
                "price",
                "non-exist",
                NO_RESULT,
                pytest.raises(ValueError, match="Invalid aggregation operation 'non-exist'"),
            ),
        ],
    )
    def test_aggregate(
        self,
        column: str,
        agg_operation: str,
        expected_result: int,
        expectation: ExpectationType,
        processor: CSVProcessor,
    ) -> None:
        with expectation:
            processor.aggregate(column=column, agg_operation=agg_operation)
            assert processor.processed_data == expected_result


@pytest.mark.parametrize(
    "argv, expected_file, expected_filters, expected_agg",
    [
        (
            ["main.py", "products.csv"],
            "products.csv",
            None,
            None,
        ),
        (
            ["main.py", "data.csv", "--filter", "price>100"],
            "data.csv",
            ["price>100"],
            None,
        ),
        (
            ["main.py", "test.csv", "--filter", "price>100", "--filter", "rating<5"],
            "test.csv",
            ["price>100", "rating<5"],
            None,
        ),
        (
            ["main.py", "input.csv", "--agg", "price=avg"],
            "input.csv",
            None,
            "price=avg",
        ),
        (
            ["main.py", "file.csv", "--filter", "brand=apple", "--agg", "rating=max"],
            "file.csv",
            ["brand=apple"],
            "rating=max",
        ),
        (
            ["main.py", "my products.csv"],
            "my products.csv",
            None,
            None,
        ),
    ],
)
def test_parse_args(argv: list[str], expected_file: str, expected_filters: str, expected_agg: str) -> None:
    with patch("sys.argv", argv):
        args = parse_args()
        assert args.file == expected_file
        assert args.filter == expected_filters
        assert args.agg == expected_agg
