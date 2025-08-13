from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import pandas as pd
import requests
from io import BytesIO
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from urllib.parse import parse_qs

app = FastAPI()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1Vl7Tj6gYVCCzmjPTo-X7TUlma9Db9Q2wh5n9m7UeENs"
RANGE = "Import 2407"

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
sheets = build("sheets", "v4", credentials=creds).spreadsheets()


@app.post("/process")
async def process(request: Request):
    content_type = request.headers.get("Content-Type", "")
    raw_body = await request.body()
    file_url = None

    try:
        if "application/json" in content_type:
            body = await request.json()
            file_url = body.get("file_url")

        elif "application/x-www-form-urlencoded" in content_type:
            form = await request.form()
            file_url = form.get("file_url")

        elif "text/plain" in content_type or "text/" in content_type:
            text = raw_body.decode("utf-8")
            if "file_url=" in text:
                # parse file_url=something format
                parsed = parse_qs(text)
                file_url = parsed.get("file_url", [None])[0]
            elif "http" in text:
                file_url = text.strip()

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": "failed to parse input", "details": str(e)})

    if not file_url:
        return JSONResponse(status_code=400, content={"error": "No file_url provided"})

    try:
        r = requests.get(file_url)
        file_data = BytesIO(r.content)
        df = pd.read_excel(file_data, engine="openpyxl")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": "Failed to download or parse Excel", "details": str(e)})

    # нужные поля
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

    df = df[[col for col in keep if col in df.columns]]
    df = df.fillna("")

    # 🌟 Текстовая замена этапов
    replacements = {
        "Аппликация создана (In progress)": "Аппликация создана но не закончена",
        "Передана на модерацию (Submitted)": "Ваша аппликация проверяется",
        "Заявка принята (Registered)": "Вы еще не до конца загрузили документы",
        "Модерация пройдена (Received)": "Проверка аппликации закончена, ожидайте дальнейших инструкций в bmu.slash.uz",
        "Оффер отправлен": "Поздравляем! Вы получили оффер от BMU в личном кабинете!",
        "Оффер принят": "Спасибо что приняли оффер! Свяжитесь с нами чтобы получить контракт",
        "Заявка одобрена": "В ближайшие дни вы получите оффер в личном кабинете! Аппликация одобрена!"
    }

    df["Этап сделки"] = df["Этап сделки"].replace(replacements)

    values = [df.columns.tolist()] + df.values.tolist()

    try:
        sheets.values().clear(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
        sheets.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE,
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "Failed to update Google Sheet", "details": str(e)})

    return {"status": "ok", "rows": len(df)}
