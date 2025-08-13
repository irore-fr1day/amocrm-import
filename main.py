from fastapi import FastAPI, Request, Form
from fastapi.responses import JSONResponse
from typing import Optional
import pandas as pd
import requests
from io import BytesIO
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

# Google Sheets Setup
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1Vl7Tj6gYVCCzmjPTo-X7TUlma9Db9Q2wh5n9m7UeENs"
RANGE = "Import 2407"

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
sheets = build("sheets", "v4", credentials=creds).spreadsheets()

def process_file(file_url: str):
    r = requests.get(file_url)
    file_data = BytesIO(r.content)

    df = pd.read_excel(file_data, engine="openpyxl")

    # Сопоставление этапов
    stage_mapping = {
        "Аппликация создана (In progress)": "Аппликация создана, но не закончена",
        "Передана на модерацию (Submitted)": "Ваша аппликация проверяется",
        "Заявка принята (Registered)": "Вы еще не до конца загрузили документы",
        "Модерация пройдена (Received)": "Проверка аппликации закончена, ожидайте дальнейших инструкций в bmu.slash.uz",
        "Оффер отправлен": "Поздравляем! Вы получили оффер от BMU в личном кабинете!",
        "Оффер принят": "Спасибо что приняли оффер! Свяжитесь с нами чтобы получить контракт",
        "Заявка одобрена": "В ближайшие дни вы получите оффер в личном кабинете! Аппликация одобрена!"
    }

    keep = [
        "Этап сделки",
        "Курс учащегося",
        "Рабочий email (контакт)",
        "Рабочий телефон (контакт)",
        "Полное имя (контакт)",
        "Номер аппликации (контакт)",
        "Дата рождения (контакт)",
        "ID Паспорта (контакт)"
    ]

    filtered = df[[col for col in keep if col in df.columns]]
    filtered["Этап сделки"] = filtered["Этап сделки"].map(stage_mapping).fillna(filtered["Этап сделки"])
    filtered = filtered.fillna("")

    values = [filtered.columns.tolist()] + filtered.values.tolist()
    sheets.values().clear(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
    sheets.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    return {"status": "ok", "rows": len(filtered)}


@app.post("/process")
async def process(request: Request):
    try:
        content_type = request.headers.get("Content-Type", "")
        if "application/json" in content_type:
            body = await request.json()
            file_url = body.get("file_url")
        else:
            form = await request.body()
            form_str = form.decode()
            # file_url=... ищем вручную
            import urllib.parse
            parsed = urllib.parse.parse_qs(form_str)
            file_url = parsed.get("file_url", [None])[0]

        if not file_url:
            return JSONResponse(status_code=400, content={"error": "Missing file_url"})

        result = process_file(file_url)
        return JSONResponse(content=result)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
