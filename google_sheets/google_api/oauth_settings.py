import json
import urllib.parse
from pathlib import Path
from typing import Any, Dict

__all__ = ["get_google_oauth_url", "get_token_request_data", "oauth2_settings"]


# Load client secret data from the JSON file
with Path("client_secret.json").open() as secret_file:
    client_secret_data = json.load(secret_file)

# OAuth2 configuration
oauth2_settings = {
    "auth_uri": client_secret_data["web"]["auth_uri"],
    "tokenUrl": client_secret_data["web"]["token_uri"],
    "clientId": client_secret_data["web"]["client_id"],
    "clientSecret": client_secret_data["web"]["client_secret"],
    "redirectUri": client_secret_data["web"]["redirect_uris"][0],
}


def get_google_oauth_url(user_id: int) -> str:
    google_oauth_url = (
        f"{oauth2_settings['auth_uri']}?client_id={oauth2_settings['clientId']}"
        f"&redirect_uri={oauth2_settings['redirectUri']}&response_type=code"
        f"&scope={urllib.parse.quote_plus('email https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive.metadata.readonly')}"
        f"&access_type=offline&prompt=consent&state={user_id}"
    )
    return google_oauth_url


def get_token_request_data(code: str) -> Dict[str, Any]:
    return {
        "code": code,
        "client_id": oauth2_settings["clientId"],
        "client_secret": oauth2_settings["clientSecret"],
        "redirect_uri": oauth2_settings["redirectUri"],
        "grant_type": "authorization_code",
    }
