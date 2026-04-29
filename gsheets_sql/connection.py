import json
import pickle
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials as SACredentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from .exceptions import AuthError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TOKEN_CACHE = Path(".gsheets_sql_token")


def build_client(credentials: str) -> gspread.Client:
    cred_path = Path(credentials)
    if not cred_path.exists():
        raise AuthError(f"Credentials file not found: {credentials}")

    with open(cred_path) as f:
        info = json.load(f)

    if info.get("type") == "service_account":
        creds = SACredentials.from_service_account_file(str(cred_path), scopes=SCOPES)
        return gspread.authorize(creds)

    # OAuth 2.0 client secrets — browser flow with local token cache
    creds = None
    if TOKEN_CACHE.exists():
        with open(TOKEN_CACHE, "rb") as f:
            creds = pickle.load(f)

    if creds and creds.valid:
        pass
    elif creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _save_token(creds)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(str(cred_path), SCOPES)
        creds = flow.run_local_server(port=0)
        _save_token(creds)

    return gspread.authorize(creds)


def _save_token(creds) -> None:
    with open(TOKEN_CACHE, "wb") as f:
        pickle.dump(creds, f)
