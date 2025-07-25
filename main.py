from fastapi import FastAPI, Request
import pandas as pd
import requests
from io import BytesIO
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

# --- Google Sheets Setup ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1Vl7Tj6gYVCCzmjPTo-X7TUlma9Db9Q2wh5n9m7UeENs"
RANGE = "Import 2407"

# Получаем credentials из переменной среды (как строка JSON)
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
sheets = build("sheets", "v4", credentials=creds).spreadsheets()

@app.post("/process")
async def process(request: Request):
    body = await request.json()
    file_url = body.get("file_url")

    if not file_url:
        return {"error": "no file_url"}

    r = requests.get(file_url)
    file_data = BytesIO(r.content)

    try:
        df = pd.read_excel(file_data, engine="openpyxl")
    except Exception as e:
        return {
            "error": "не удалось прочитать файл",
            "details": str(e)
    }


    keep = [
        "Этап", "Курс учащегося", "Рабочий email (контакт)",
        "Рабочий телефон (контакт)", "Полное имя (контакт)",
        "Номер аппликации (контакт)", "Дата рождения (контакт)",
        "ID Паспорта (контакт)"
    ]

    filtered = df[[col for col in keep if col in df.columns]]
    values = [filtered.columns.tolist()] + filtered.values.tolist()

    # очищаем и записываем
    sheets.values().clear(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
    sheets.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    return {"status": "ok", "rows": len(filtered)}
