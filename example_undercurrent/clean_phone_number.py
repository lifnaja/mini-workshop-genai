from datetime import datetime
import re

import pandas as pd


def clean_phone_number(phone: str | None) -> str | None:
    if phone is None:
        return None

    if "/" in phone:
        phone = phone.replace("/", ",").replace(" ", "")

    cleaned = re.sub(r"[^0-9+,\s-]", "", phone)

    cleaned = re.sub(r"^\+66", "0", cleaned)
    cleaned = re.sub(r"^008", "08", cleaned)

    if len(cleaned) < 20:
        cleaned = cleaned.replace(" ", "-")
    phone_numbers = re.split(r"[,\s/]+", cleaned)

    cleaned_numbers = []
    for phone in phone_numbers:
        phone_numbers_cleaned = re.sub(r"[-\s]", "", phone)
        if phone_numbers_cleaned != "":
            cleaned_numbers.append(phone_numbers_cleaned)

    if not cleaned_numbers:
        return None

    return ",".join(cleaned_numbers)
