import os
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from hdbcli import dbapi
import uvicorn

# =========================================================
# 1. log
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("domestic_amount_dropdown_api")

# =========================================================
# 2. FastAPI 
# =========================================================
DESCRIPTION = "含税伝票通貨額から国内通貨額(税抜)を計算し、Dropdown 用 codes[] で返す API"

app = FastAPI(
    title="フォーム代入（国内通貨額・Dropdown・税抜）",
    description=DESCRIPTION,
    summary=(
        "F_Doc_curr_amt(含税・外貨) とレートから国内通貨額(税抜)を計算し、"
        "Dynamic Code List 形式のレスポンスで返却する。"
    ),
    version="3.0.0",
)

# =========================================================
# 3. 環境設定
# =========================================================
load_dotenv()

CF_PORT = int(os.getenv("PORT", "3000"))

# 税率（default 10% = 0.1）
TAX_RATE = float(os.getenv("DOMESTIC_TAX_RATE", "0.1"))

# 国内通货（TBL_EXCHANGE_RATE.QUOTED_CURRENCY）
DOMESTIC_CURRENCY = os.getenv("DOMESTIC_CURRENCY", "JPY")

DEFAULT_CATEGORY_CODE = int(os.getenv("EXRATE_CATEGORY_CODE", "1"))
DEFAULT_EXRATE_TYPE = os.getenv("EXRATE_TYPE", "M")

logger.info(
    "Domestic Amount service starting... "
    "(PORT=%s, TAX_RATE=%s, DOMESTIC_CURRENCY=%s, CATEGORY_CODE=%s, EXRATE_TYPE=%s)",
    CF_PORT, TAX_RATE, DOMESTIC_CURRENCY, DEFAULT_CATEGORY_CODE, DEFAULT_EXRATE_TYPE
)

# =========================================================
# 4. HANA info
# =========================================================
def get_hana_connection():
    address = os.environ.get("HANA_HOST")
    port = int(os.environ.get("HANA_PORT", "443"))
    user = os.environ.get("HANA_USER", "DBADMIN")
    password = os.environ.get("HANA_PASSWORD")

    if not address:
        raise RuntimeError("環境変数 HANA_HOST が設定されていません")
    if not password:
        raise RuntimeError("環境変数 HANA_PASSWORD が設定されていません")

    params: Dict[str, Any] = {
        "address": address,
        "port": port,
        "user": user,
        "password": password,
        "encrypt": True,
        "sslValidateCertificate": False, 
    }

    masked = {k: ("*****" if k == "password" else v) for k, v in params.items()}
    logger.debug("Connecting to HANA with params: %s", masked)

    return dbapi.connect(**params)

# =========================================================
# 5. 日期处理：PaymentDate → 'YYYY-MM-DD'
# =========================================================
def normalize_date(date_str: Any) -> str:
    if not date_str:
        return ""

    s = str(date_str).strip()

    if "T" in s:
        s = s.split("T", 1)[0]

    s = s.replace("/", "-")

    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        logger.warning("normalize_date: unexpected format '%s'", date_str)
        return s

    normalized = dt.strftime("%Y-%m-%d")
    logger.debug("normalize_date: '%s' -> '%s'", date_str, normalized)
    return normalized

# =========================================================
# 6. 通货描述 → 通货代码
# =========================================================
def find_currency_code_by_desc(description: str) -> Optional[str]:

    if not description:
        return None

    conn = get_hana_connection()
    try:
        cursor = conn.cursor()
        sql = """
            SELECT CURRENCY_CODE
              FROM "DBADMIN"."TBL_CURRENCY"
             WHERE CURRENCY_DESC_JP = ?
                OR CURRENCY_DESC    = ?
             LIMIT 1
        """
        cursor.execute(sql, (description, description))
        row = cursor.fetchone()
        if row:
            code = str(row[0])
            logger.debug(
                "TBL_CURRENCY DESC '%s' -> CURRENCY_CODE '%s'",
                description, code
            )
            return code

        logger.warning("No currency code found in TBL_CURRENCY for '%s'", description)
        return None
    finally:
        conn.close()


def find_currency_code_in_exrate_desc(description: str) -> Optional[str]:

    if not description:
        return None

    conn = get_hana_connection()
    try:
        cursor = conn.cursor()
        sql = """
            SELECT UNIT_CURRENCY
              FROM "DBADMIN"."TBL_EXCHANGE_RATE"
             WHERE CURRENCY_DESC = ?
             ORDER BY TECHNICAL_ID DESC
             LIMIT 1
        """
        cursor.execute(sql, (description,))
        row = cursor.fetchone()
        if row:
            code = str(row[0])
            logger.debug(
                "TBL_EXCHANGE_RATE CURRENCY_DESC '%s' -> UNIT_CURRENCY '%s'",
                description, code
            )
            return code

        logger.warning(
            "No UNIT_CURRENCY found in TBL_EXCHANGE_RATE for CURRENCY_DESC '%s'",
            description,
        )
        return None
    finally:
        conn.close()


def normalize_currency(raw_currency: Any) -> str:

    if raw_currency is None:
        raise HTTPException(status_code=400, detail="F_Currency is required.")

    value = str(raw_currency).strip()
    if not value:
        raise HTTPException(status_code=400, detail="F_Currency is empty.")

    
    if len(value) == 3 and value.isalpha():
        code = value.upper()
        logger.debug("Currency looks like code already: '%s' -> '%s'", value, code)
        return code

    
    code_from_currency = find_currency_code_by_desc(value)
    if code_from_currency:
        return code_from_currency

    
    code_from_exrate = find_currency_code_in_exrate_desc(value)
    if code_from_exrate:
        return code_from_exrate

    
    logger.warning(
        "Fallback: using raw currency value '%s' as CURRENCY_CODE (no match in CURRENCY/EXRATE tables)",
        value,
    )
    return value

# =========================================================
# 7.  HANA DBからrate取得
# =========================================================
def get_rate_from_db(unit_currency: str, quoted_date: str) -> Tuple[float, str]:

    conn = get_hana_connection()
    try:
        cursor = conn.cursor()

        # ① 带 CATEGORY_CODE & TYPE
        sql1 = """
            SELECT EXCHANGE_RATE, CURRENCY_DESC
              FROM "DBADMIN"."TBL_EXCHANGE_RATE"
             WHERE UNIT_CURRENCY      = ?
               AND QUOTED_CURRENCY    = ?
               AND QUOTED_DATE        = ?
               AND CATEGORY_CODE      = ?
               AND EXCHANGE_RATE_TYPE = ?
             ORDER BY TECHNICAL_ID DESC
        """
        params1 = (
            unit_currency,
            DOMESTIC_CURRENCY,
            quoted_date,
            DEFAULT_CATEGORY_CODE,
            DEFAULT_EXRATE_TYPE,
        )
        logger.debug("ExchangeRate SQL1: %s", sql1)
        logger.debug("ExchangeRate PARAMS1: %s", params1)

        cursor.execute(sql1, params1)
        row = cursor.fetchone()

        
        if not row:
            sql2 = """
                SELECT EXCHANGE_RATE, CURRENCY_DESC
                  FROM "DBADMIN"."TBL_EXCHANGE_RATE"
                 WHERE UNIT_CURRENCY   = ?
                   AND QUOTED_CURRENCY = ?
                   AND QUOTED_DATE     = ?
                 ORDER BY TECHNICAL_ID DESC
            """
            params2 = (unit_currency, DOMESTIC_CURRENCY, quoted_date)
            logger.debug("ExchangeRate SQL2: %s", sql2)
            logger.debug("ExchangeRate PARAMS2: %s", params2)

            cursor.execute(sql2, params2)
            row = cursor.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=(
                    "No exchange rate found in TBL_EXCHANGE_RATE "
                    f"(UNIT_CURRENCY={unit_currency}, "
                    f"QUOTED_CURRENCY={DOMESTIC_CURRENCY}, "
                    f"QUOTED_DATE={quoted_date})."
                ),
            )

        rate = float(row[0] or 0.0)
        currency_desc = str(row[1] or "").strip()

        logger.info(
            "Rate from DB: UNIT_CURRENCY=%s, QUOTED_CURRENCY=%s, "
            "QUOTED_DATE=%s, RATE=%s, DESC=%s",
            unit_currency, DOMESTIC_CURRENCY, quoted_date, rate, currency_desc
        )

        return rate, currency_desc

    finally:
        conn.close()

# =========================================================
# 8. Health Check
# =========================================================
@app.get("/health")
async def health():
    return JSONResponse(content={"status": "running"}, status_code=200)

# =========================================================
# 9.Domestic_amount
# =========================================================
@app.post("/Domestic_amount")
async def domestic_amount(request: Request):
   
    try:
       
        raw_body = await request.body()
        logger.debug("RAW BODY: %s", raw_body.decode("utf-8", errors="replace"))

        try:
            payload: Dict[str, Any] = await request.json()
        except Exception as exc:  
            raise HTTPException(
                status_code=400,
                detail=f"Invalid JSON payload: {str(exc)}",
            ) from exc

        logger.debug("Received payload: %s", payload)

        body: Dict[str, Any] = payload.get("requestBody", payload) or {}
        form: Dict[str, Any] = body.get("Form", {}) or {}
        case_block: Dict[str, Any] = body.get("case", {}) or {}
        extensions: Dict[str, Any] = case_block.get("extensions", {}) or {}

        meisai = form.get("F_meisai") or []
        item0: Dict[str, Any] = meisai[0] if isinstance(meisai, list) and meisai else {}

        raw_amount = (
            item0.get("F_Doc_curr_amt")
            if item0
            else form.get("F_Doc_curr_amt")
        )
        if raw_amount is None:
            raise HTTPException(status_code=400, detail="F_Doc_curr_amt is required.")

        try:
            gross_foreign = float(raw_amount)
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail="F_Doc_curr_amt must be numeric.",
            ) from exc

        raw_currency = form.get("F_Currency")
        unit_currency = normalize_currency(raw_currency)

        payment_date_raw = (
            extensions.get("PaymentDate")
            or extensions.get("Payment_Date")  
        )

        logger.info("Raw PaymentDate from request = %s", payment_date_raw)

        if not payment_date_raw:
            raise HTTPException(
                status_code=400,
                detail="case.extensions.PaymentDate is required.",
            )

        quoted_date = normalize_date(payment_date_raw)

        logger.info(
            "Input summary: gross_foreign=%s, currency_raw=%s, "
            "currency_normalized=%s, paymentDateRaw=%s, quotedDate=%s",
            gross_foreign, raw_currency, unit_currency, payment_date_raw, quoted_date
        )

        raw_rate = form.get("F_Rate")
        rate: Optional[float] = None
        currency_desc = ""

        if raw_rate not in (None, "", 0, "0"):
            try:
                rate = float(raw_rate)
                logger.info("Use F_Rate from form: %s", rate)
            except (TypeError, ValueError):
                logger.warning(
                    "F_Rate is not numeric (%s). Will try HANA instead.",
                    raw_rate,
                )

        if rate is None:
            rate, currency_desc = get_rate_from_db(unit_currency, quoted_date)

        if (1.0 + TAX_RATE) == 0:
            raise HTTPException(
                status_code=500,
                detail="Invalid TAX_RATE setting (1 + TAX_RATE == 0).",
            )

        net_domestic = gross_foreign * rate / (1.0 + TAX_RATE)
        net_domestic = round(net_domestic, 2)

        logger.info(
            "Calculation: gross_foreign(含税)=%s, rate=%s, TAX_RATE=%s => net_domestic(税抜)=%s",
            gross_foreign, rate, TAX_RATE, net_domestic,
        )

        key_str = f"{net_domestic:.2f}"
        desc_str = f"{key_str} {DOMESTIC_CURRENCY}"
        if currency_desc:
            desc_str += f" ({currency_desc}, rate={rate}, taxRate={TAX_RATE})"
        else:
            desc_str += f" (rate={rate}, taxRate={TAX_RATE})"

        response_body = {
            "responseBody": {
                "messages": [
                    {
                        "code": "S000",
                        "message": (
                            "Net domestic amount (tax excluded) calculated. "
                            f"paymentDateRaw={payment_date_raw}, quotedDate={quoted_date}, "
                            f"currency={unit_currency}, gross={gross_foreign}, rate={rate}, "
                            f"TAX_RATE={TAX_RATE}, net_domestic={key_str}"
                        ),
                        "type": "INFO",
                    }
                ],
                "value": {
                    "codes": [
                        {
                            "key": key_str,
                            "description": "",
                        }
                    ]
                },
                "isSuccess": True,
            }
        }

        return JSONResponse(content=response_body, status_code=200)

    except HTTPException:
        raise
    except Exception as ex:  
        logger.exception("Unexpected error in /Domestic_amount")
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error in /Domestic_amount: {str(ex)}",
        ) from ex

# =========================================================
# 10. Local起動
# =========================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=CF_PORT, log_level="info")
    logger.info("Domestic Amount service started....")
