from dotenv import load_dotenv
import os
from google.oauth2 import service_account


def get_credentials():
    load_dotenv()

    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

    return service_account.Credentials.from_service_account_file(
        creds_path, scopes=scopes
    )
