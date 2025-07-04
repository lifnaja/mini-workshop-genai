import csv
import json
from google import genai
from google.genai import types

GEMINI_API_KEY = "GEMINI_API_KEY"
GEMINI_AI_MODEL = "gemini-2.5-flash-preview-05-20"


def extract_data_with_ai(facebook_posts: str) -> dict:
    client = genai.Client(
        api_key=GEMINI_API_KEY,
    )

    response_schema = {
        "type": "ARRAY",
        "items": {
            "type": "OBJECT",
            "properties": {
                "brand": {
                    "type": "STRING",
                    "description": "car brand",
                    "nullable": True,
                },
                "model": {
                    "type": "STRING",
                    "description": "car model",
                    "nullable": True,
                },
                "sub_model": {
                    "type": "STRING",
                    "description": "car sub_model",
                    "nullable": True,
                },
                "year": {
                    "type": "INTEGER",
                    "description": "car year",
                    "nullable": True,
                },
                "gear": {
                    "type": "STRING",
                    "description": "car gear",
                    "nullable": True,
                    "enum": ["Automatic", "Manual"],
                },
                "price": {
                    "type": "INTEGER",
                    "description": "car price",
                    "example": 1000000,
                    "nullable": True,
                },
                "mileage": {
                    "type": "INTEGER",
                    "description": "car mileage",
                    "example": 100000,
                    "nullable": True,
                },
                "color": {
                    "type": "STRING",
                    "description": "car color in English",
                    "example": "White",
                    "nullable": True,
                },
            },
            "required": [
                "brand",
                "model",
                "sub_model",
                "year",
                "gear",
                "price",
                "mileage",
                "color",
            ],
        },
    }

    response = client.models.generate_content(
        model=GEMINI_AI_MODEL,
        contents=[
            "Extract car information from social media posts about selling cars. Respond only in English",
            facebook_posts,
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=response_schema,
            temperature=0.1,
            top_p=0.1,
            top_k=1,
        ),
    )

    return json.loads(response.text)


extract_used_car_data = []
with open("facebook_posts.csv", mode="r", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        post_id = row["post_id"]
        text = row["text"]

        extract_data = extract_data_with_ai(text)

        for item in extract_data:
            item["post_id"] = post_id

        extract_used_car_data.extend(extract_data)

output_file = "extract_facebook_posts.csv"
with open(output_file, mode="w", newline="", encoding="utf-8") as f:
    fieldnames = [
        "post_id",
        "brand",
        "model",
        "sub_model",
        "year",
        "gear",
        "price",
        "mileage",
        "color",
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)

    writer.writeheader()

    for car in extract_used_car_data:
        row = {
            "post_id": car["post_id"],
            "brand": car["brand"],
            "model": car["model"],
            "sub_model": car["sub_model"],
            "year": car["year"],
            "gear": car["gear"],
            "price": car["price"] if car["price"] is not None else "",
            "mileage": car["mileage"] if car["mileage"] is not None else "",
            "color": car["color"] if car["color"] is not None else "",
        }
        writer.writerow(row)
