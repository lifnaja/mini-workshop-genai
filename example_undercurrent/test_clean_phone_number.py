import pytest
from clean_phone_number import clean_phone_number  # เปลี่ยนชื่อ your_module ให้ตรงกับชื่อไฟล์จริง

@pytest.mark.parametrize("input_phone, expected", [
    (None, None),
    ("0812345678", "0812345678"),
    ("+66812345678", "0812345678"),
    ("00812345678", "0812345678"),
    ("081-234-5678", "0812345678"),
    ("081 234 5678", "0812345678"),
    ("abc0812345678xyz", "0812345678"),
    ("", None),
    ("/ ,", None),
    ("+6681-234-5678 / 0891234567", "0812345678,0891234567"),
    ("081-2345678/089-9999999", "0812345678,0899999999"),
])
def test_clean_phone_number(input_phone, expected):
    assert clean_phone_number(input_phone) == expected
