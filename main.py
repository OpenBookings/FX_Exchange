import os
import io
import csv
from datetime import datetime, timezone
from typing import Dict, Optional

import requests
from fastapi import FastAPI, HTTPException, Query, Header
from google.cloud import firestore

app = FastAPI(title="ECB FX Service", version="1.0.0")

# ---- Config ----
FIRESTORE_COLLECTION = os.getenv("FIRESTORE_COLLECTION", "fx_rates")
UPDATE_TOKEN = os.getenv("UPDATE_TOKEN")  # set this in Cloud Run env vars
ECB_CSV_URL = (
    "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A"
    "?format=csvdata&lastNObservations=1"
)

db = firestore.Client()


# ---- Helpers ----
def _today_utc_iso_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def fetch_ecb_latest_csv() -> Dict:
    """
    Fetch latest daily FX reference rates from ECB as CSV.
    Returns:
      {
        "date": "YYYY-MM-DD",
        "base": "EUR",
        "rates": {"USD": 1.1721, ...},
        "source": "ECB"
      }
    """
    r = requests.get(ECB_CSV_URL, timeout=30)
    r.raise_for_status()

    # ECB CSV typically contains many columns; we only need:
    # TIME_PERIOD, CURRENCY, OBS_VALUE (sometimes also other dimensions)
    text = r.text
    reader = csv.DictReader(io.StringIO(text))

    rates: Dict[str, float] = {}
    found_date: Optional[str] = None

    for row in reader:
        # Common column names in SDMX CSV exports:
        # - "TIME_PERIOD"
        # - "CURRENCY"
        # - "OBS_VALUE"
        date = row.get("TIME_PERIOD")
        ccy = row.get("CURRENCY")
        val = row.get("OBS_VALUE")

        if not date or not ccy or not val:
            continue

        # Should all be same date due to lastNObservations=1
        found_date = found_date or date

        try:
            rates[ccy] = float(val)
        except ValueError:
            continue

    if not found_date or not rates:
        raise RuntimeError("ECB response parsed but no rates found")

    return {
        "date": found_date,
        "base": "EUR",
        "rates": rates,
        "source": "ECB",
    }


def store_snapshot(snapshot: Dict) -> None:
    doc_id = snapshot["date"]  # "YYYY-MM-DD"
    snapshot = {
        **snapshot,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    db.collection(FIRESTORE_COLLECTION).document(doc_id).set(snapshot)


def load_snapshot(date: str) -> Optional[Dict]:
    doc = db.collection(FIRESTORE_COLLECTION).document(date).get()
    if not doc.exists:
        return None
    return doc.to_dict()


def load_latest_snapshot() -> Optional[Dict]:
    # Query latest by date string (ISO YYYY-MM-DD sorts lexicographically)
    q = (
        db.collection(FIRESTORE_COLLECTION)
        .order_by("date", direction=firestore.Query.DESCENDING)
        .limit(1)
        .stream()
    )
    docs = list(q)
    if not docs:
        return None
    return docs[0].to_dict()


def convert_amount(amount: float, from_ccy: str, to_ccy: str, rates: Dict[str, float]) -> float:
    from_ccy = from_ccy.upper()
    to_ccy = to_ccy.upper()

    if amount < 0:
        raise ValueError("amount must be >= 0")

    if from_ccy == to_ccy:
        return amount

    # ECB base is EUR (implicit EUR rate = 1.0)
    def get_rate(ccy: str) -> float:
        if ccy == "EUR":
            return 1.0
        if ccy not in rates:
            raise KeyError(ccy)
        return float(rates[ccy])

    r_from = get_rate(from_ccy)
    r_to = get_rate(to_ccy)

    # X -> Y using EUR base:
    # amount_in_eur = amount / r_from  (unless from is EUR)
    # result = amount_in_eur * r_to
    # Combined: amount * (r_to / r_from)
    return amount * (r_to / r_from)


# ---- Endpoints ----
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/tasks/update")
def update_rates(x_update_token: Optional[str] = Header(default=None)):
    """
    Called by Cloud Scheduler (or manually) to fetch and store the latest ECB snapshot.
    Protect this endpoint using UPDATE_TOKEN (or Cloud Run IAM).
    """
    if UPDATE_TOKEN:
        if not x_update_token or x_update_token != UPDATE_TOKEN:
            raise HTTPException(status_code=401, detail="Unauthorized")

    snapshot = fetch_ecb_latest_csv()
    store_snapshot(snapshot)
    return {"stored": True, "date": snapshot["date"], "base": snapshot["base"], "count": len(snapshot["rates"])}


@app.get("/rates/latest")
def rates_latest():
    snap = load_latest_snapshot()
    if not snap:
        raise HTTPException(status_code=404, detail="No rates stored yet. Call /tasks/update first.")
    return snap


@app.get("/rates/{date}")
def rates_by_date(date: str):
    snap = load_snapshot(date)
    if not snap:
        raise HTTPException(status_code=404, detail=f"No rates found for {date}")
    return snap


@app.get("/convert")
def convert(
    amount: float = Query(..., ge=0),
    from_ccy: str = Query(..., alias="from", min_length=3, max_length=3),
    to_ccy: str = Query(..., alias="to", min_length=3, max_length=3),
    date: Optional[str] = Query(None, description="YYYY-MM-DD; if omitted uses latest stored snapshot"),
    precision: int = Query(2, ge=0, le=12),
):
    snap = load_snapshot(date) if date else load_latest_snapshot()
    if not snap:
        raise HTTPException(status_code=404, detail="No rates available. Call /tasks/update first.")

    rates = snap.get("rates", {})
    try:
        result = convert_amount(amount, from_ccy, to_ccy, rates)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Unsupported currency: {str(e).strip('\"')}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Output rounding only at the end
    rounded = round(result, precision)

    # Effective rate (per 1 unit of from currency)
    try:
        eff_rate = convert_amount(1.0, from_ccy, to_ccy, rates)
    except Exception:
        eff_rate = None

    return {
        "amount": amount,
        "from": from_ccy.upper(),
        "to": to_ccy.upper(),
        "result": rounded,
        "raw_result": result,
        "rate": round(eff_rate, 12) if eff_rate is not None else None,
        "date": snap["date"],
        "base": snap.get("base", "EUR"),
        "source": snap.get("source", "ECB"),
        "fetched_at": snap.get("fetched_at"),
    }
