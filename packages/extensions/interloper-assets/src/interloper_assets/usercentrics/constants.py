import httpx2

BASE_URL = "https://data-export.service.usercentrics.eu/download/v2"

DOWNLOAD_TIMEOUT = httpx2.Timeout(300.0, connect=30.0)
