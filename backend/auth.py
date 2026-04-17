"""
OAuth2 authentication handler for Google Drive API.
Manages credential creation, storage, and refresh.
"""

import json
import os
import threading
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

SCOPES = [
    "https://www.googleapis.com/auth/drive",
]

TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"
REDIRECT_PORT = 8085
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}"


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    auth_code = None

    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)
        if "code" in query:
            _OAuthCallbackHandler.auth_code = query["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h2>Authentication successful!</h2>"
                b"<p>You can close this tab and return to the app.</p></body></html>"
            )
        else:
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h2>Authentication failed.</h2></body></html>")

    def log_message(self, format, *args):
        pass


class AuthManager:
    def __init__(self, app_dir: str):
        self.app_dir = app_dir
        self.token_path = os.path.join(app_dir, TOKEN_FILE)
        self.creds_path = os.path.join(app_dir, CREDENTIALS_FILE)
        self._credentials = None

    @property
    def is_authenticated(self) -> bool:
        creds = self._load_credentials()
        return creds is not None and creds.valid

    def get_credentials(self):
        creds = self._load_credentials()
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                self._save_credentials(creds)
            except Exception:
                return None
        self._credentials = creds
        return creds

    def start_auth_flow(self) -> str:
        if not os.path.exists(self.creds_path):
            raise FileNotFoundError(
                f"credentials.json not found at {self.creds_path}. "
                "Download it from Google Cloud Console."
            )
        flow = Flow.from_client_secrets_file(
            self.creds_path, scopes=SCOPES, redirect_uri=REDIRECT_URI,
        )
        auth_url, _ = flow.authorization_url(
            access_type="offline", include_granted_scopes="true", prompt="consent",
        )
        return auth_url

    def complete_auth_flow(self, timeout: int = 120) -> bool:
        auth_url = self.start_auth_flow()
        _OAuthCallbackHandler.auth_code = None
        server = HTTPServer(("localhost", REDIRECT_PORT), _OAuthCallbackHandler)
        server.timeout = timeout
        webbrowser.open(auth_url)
        while _OAuthCallbackHandler.auth_code is None:
            server.handle_request()
            if _OAuthCallbackHandler.auth_code is not None:
                break
        server.server_close()
        if _OAuthCallbackHandler.auth_code is None:
            return False
        flow = Flow.from_client_secrets_file(
            self.creds_path, scopes=SCOPES, redirect_uri=REDIRECT_URI,
        )
        flow.fetch_token(code=_OAuthCallbackHandler.auth_code)
        self._credentials = flow.credentials
        self._save_credentials(flow.credentials)
        return True

    def logout(self):
        if os.path.exists(self.token_path):
            os.remove(self.token_path)
        self._credentials = None

    def _load_credentials(self):
        if self._credentials and self._credentials.valid:
            return self._credentials
        if os.path.exists(self.token_path):
            try:
                creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    self._save_credentials(creds)
                return creds
            except Exception:
                return None
        return None

    def _save_credentials(self, creds):
        with open(self.token_path, "w") as f:
            f.write(creds.to_json())
