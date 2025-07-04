import pytest
import pandas as pd
from datetime import datetime
from parse_thai_date import parse_thai_date

@pytest.mark.parametrize("thai_date_str, expected", [
    ("1 ม.ค. 2566", datetime(2023, 1, 1)),
    ("15 ก.พ. 2564", datetime(2021, 2, 15)),
    ("31 ธ.ค. 2565", datetime(2022, 12, 31)),
    ("9 เม.ย. 2563", datetime(2020, 4, 9)),
    ("01 พ.ค. 2560", datetime(2017, 5, 1)),
    ("", pd.NaT),
    ("31/12/2565", pd.NaT),
    ("25 ต.ค. xx", pd.NaT),
    ("ธ.ค. 2566", pd.NaT),
    (" 5 ก.ค. 2562 ", datetime(2019, 7, 5)),  # เช็คว่า strip ได้
])
def test_parse_thai_date(thai_date_str, expected):
    result = parse_thai_date(thai_date_str)
    if pd.isna(expected):
        assert pd.isna(result)
    else:
        assert result == expected
