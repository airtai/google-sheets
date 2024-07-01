# update pip
pip install --upgrade pip

# install dev packages
pip install -e ".[dev]"

# install pre-commit hook if not installed already
pre-commit install

prisma migrate deploy
prisma generate

echo '{
    "web": {
        "client_id": "1027914582771-g0bcsn4fhd6a59pp3d4n1ntjc03r1k9s.apps.googleusercontent.com",
        "project_id": "captn-sheets-dev",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "'${GOOGLE_SHEETS_CLIENT_SECRET}'",
        "redirect_uris": [
            "http://localhost:8000/login/callback"
        ]

    }
}' > client_secret.json
