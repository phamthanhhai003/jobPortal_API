from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from pathlib import Path
import csv
import time
from typing import Optional

app = FastAPI(default_response_class=ORJSONResponse)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
COMPANY_CSV = BASE_DIR / "source" / "company.csv"
RECRUIT_CSV = BASE_DIR / "source" / "recruit.csv"

# in-memory (per Vercel instance)
company_cache = {}
recruit_cache = {}
all_companies = []
all_recruits = []

_cache_loaded = False
_cache_loaded_at = 0.0
_cache_version = ""  # based on file mtimes


def _file_version() -> str:
    # cheap version string; if CSV changes, version changes
    c_m = COMPANY_CSV.stat().st_mtime if COMPANY_CSV.exists() else 0
    r_m = RECRUIT_CSV.stat().st_mtime if RECRUIT_CSV.exists() else 0
    return f"{c_m:.0f}-{r_m:.0f}"


def _normalize_row(row: dict) -> dict:
    # Convert empty string -> None (fast & enough for CSV)
    # Strip whitespace to make keys stable
    out = {}
    for k, v in row.items():
        if k is None:
            continue
        key = k.strip()
        if v is None:
            out[key] = None
        else:
            vv = v.strip()
            out[key] = None if vv == "" else vv
    return out


def load_cache_from_csv(force: bool = False):
    global company_cache, recruit_cache, all_companies, all_recruits
    global _cache_loaded, _cache_loaded_at, _cache_version

    if not COMPANY_CSV.exists():
        raise FileNotFoundError(f"Missing {COMPANY_CSV}")
    if not RECRUIT_CSV.exists():
        raise FileNotFoundError(f"Missing {RECRUIT_CSV}")

    version = _file_version()
    if _cache_loaded and not force and version == _cache_version:
        return

    # local vars (faster), then assign once
    _company_cache = {}
    _recruit_cache = {}
    _all_companies = []
    _all_recruits = []

    # Companies
    with COMPANY_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = _normalize_row(row)

            # drop columns like quality/updated_at if exist
            d.pop("quality", None)
            d.pop("updated_at", None)

            _all_companies.append(d)

            corp = d.get("corporate_number")
            if corp:
                _company_cache[corp] = d

    # Recruits
    with RECRUIT_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = _normalize_row(row)
            media_id = d.get("media_internal_id")
            if not media_id:
                continue
            _all_recruits.append(d)
            _recruit_cache[media_id] = d

    company_cache = _company_cache
    recruit_cache = _recruit_cache
    all_companies = _all_companies
    all_recruits = _all_recruits

    _cache_loaded = True
    _cache_loaded_at = time.time()
    _cache_version = version


def ensure_cache_loaded():
    if not _cache_loaded:
        load_cache_from_csv()


@app.get("/company/{corporate_number}")
async def get_company(corporate_number: str):
    ensure_cache_loaded()
    company = company_cache.get(corporate_number)
    if company is None:
        raise HTTPException(status_code=404, detail="Công ty không tồn tại")
    return company


@app.get("/recruitment/{media_internal_id}")
async def get_recruitment(media_internal_id: str):
    ensure_cache_loaded()
    recruitment = recruit_cache.get(media_internal_id)
    if recruitment is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy thông tin tuyển dụng")

    corp = recruitment.get("corporate_number")
    comp = company_cache.get(corp) if corp else None

    return {"recruitment": recruitment, "company": comp}


@app.get("/recruitment")
async def get_all_recruitments():
    ensure_cache_loaded()
    return all_recruits


@app.get("/")
async def root():
    ensure_cache_loaded()
    return all_companies


@app.get("/debug/check-data")
async def check_data():
    ensure_cache_loaded()
    return {
        "company_count": len(company_cache),
        "recruit_count": len(recruit_cache),
        "cache_loaded": _cache_loaded,
        "cache_version": _cache_version,
        "cache_loaded_at": _cache_loaded_at,
        "source": "csv"
    }


@app.get("/health")
async def health_check():
    try:
        ensure_cache_loaded()
        return {
            "status": "healthy",
            "source": "csv",
            "companies": len(company_cache),
            "recruitments": len(recruit_cache),
            "cache_status": "active"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/reload-cache")
async def reload_cache():
    try:
        load_cache_from_csv(force=True)
        return {
            "status": "success",
            "companies_loaded": len(company_cache),
            "recruitments_loaded": len(recruit_cache),
            "cache_version": _cache_version
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
