import json
import re
from datetime import date
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel, field_validator, EmailStr

app = FastAPI()

STORAGE_DIR = Path("appeals")
STORAGE_DIR.mkdir(exist_ok=True)

CYRILLIC_CAPITAL = re.compile(r'^[А-ЯЁ][А-ЯЁа-яё]+$')
PHONE_RE = re.compile(r'^\+7\d{10}$|^8\d{10}$|^\+?\d{7,15}$')


class Appeal(BaseModel):
    last_name: str
    first_name: str
    birth_date: date
    phone: str
    email: EmailStr

    @field_validator('last_name', 'first_name')
    @classmethod
    def must_be_cyrillic_capitalized(cls, v: str, info) -> str:
        if not CYRILLIC_CAPITAL.match(v):
            raise ValueError(
                f"'{info.field_name}' must start with a capital Cyrillic letter "
                "and contain only Cyrillic characters"
            )
        return v

    @field_validator('phone')
    @classmethod
    def must_be_valid_phone(cls, v: str) -> str:
        cleaned = re.sub(r'[\s\-\(\)]', '', v)
        if not PHONE_RE.match(cleaned):
            raise ValueError("Invalid phone number format")
        return cleaned


@app.post("/appeal/", status_code=201)
async def create_appeal(appeal: Appeal):
    data = appeal.model_dump(mode="json")

    filename = STORAGE_DIR / f"{appeal.last_name}_{appeal.first_name}_{appeal.birth_date}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    return {"message": "Appeal saved", "file": str(filename)}