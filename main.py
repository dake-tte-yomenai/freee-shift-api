# main.py
from datetime import date, time
from typing import List, Optional
import os, logging
import datetime as dt
import httpx

from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator, Field
import databases
import secrets

# ========= DB =========
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL が未設定です（本番環境の環境変数に設定してください）")

database = databases.Database(DATABASE_URL)

# ========= FastAPI =========
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 本番はフロントのドメインに絞ってください
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========= Pydantic models =========
class BreakIn(BaseModel):
    start_break: time
    end_break: time

    @field_validator("end_break")
    @classmethod
    def check_order(cls, v, info):
        sb = info.data.get("start_break")
        if sb and v and v <= sb:
            raise ValueError("end_break must be after start_break")
        return v

class ShiftIn(BaseModel):
    employee_id: int
    work_date: date          # "YYYY-MM-DD"
    year: int                # テーブル名生成用（ゼロ詰めなし）
    month: int
    day: int
    start_work: Optional[time] = None
    end_work: Optional[time] = None
    # 可変デフォルトは Field(default_factory=list)
    breaks: List[BreakIn] = Field(default_factory=list)

    @field_validator("end_work")
    @classmethod
    def check_work_order(cls, v, info):
        sw = info.data.get("start_work")
        if sw and v and v <= sw:
            raise ValueError("end_work must be after start_work")
        return v

class LiffBindIn(BaseModel):
    employee_id: int
    line_user_id: str
    display_name: str | None = None
    code: str

# ========= Helpers / DDL =========
def table_names_for(year: int, month: int) -> tuple[str, str]:
    # 例: "work_2025_9", "break_2025_9"（ダブルクォートで識別子を保護）
    work = f'"work_{year}_{month}"'
    brk  = f'"break_{year}_{month}"'
    return work, brk

async def ensure_month_tables(year: int, month: int):
    """指定の年/月テーブルが無ければ作成します。"""
    WORK_TBL, BREAK_TBL = table_names_for(year, month)

    create_work_sql = f"""
    CREATE TABLE IF NOT EXISTS {WORK_TBL} (
        id         BIGINT NOT NULL,
        work_date  DATE   NOT NULL,
        start_work TIME,
        end_work   TIME,
        PRIMARY KEY (id, work_date)
    );
    """

    create_break_sql = f"""
    CREATE TABLE IF NOT EXISTS {BREAK_TBL} (
        id          BIGINT   NOT NULL,
        work_date   DATE     NOT NULL,
        seq         SMALLINT NOT NULL,
        start_break TIME     NOT NULL,
        end_break   TIME     NOT NULL,
        PRIMARY KEY (id, work_date, seq)
    );
    """

    async with database.transaction():
        await database.execute(create_work_sql)
        await database.execute(create_break_sql)

async def ensure_binding_table():
    await database.execute("""
    CREATE TABLE IF NOT EXISTS line_binding (
      employee_id  BIGINT PRIMARY KEY,
      line_user_id TEXT UNIQUE NOT NULL,
      display_name TEXT,
      active       BOOLEAN NOT NULL DEFAULT TRUE,
      verified_at  TIMESTAMP NULL,
      updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

async def ensure_onboarding_table():
    await database.execute("""
    CREATE TABLE IF NOT EXISTS onboarding_code (
      employee_id BIGINT NOT NULL,
      code        TEXT   NOT NULL,
      expires_at  TIMESTAMP NOT NULL,
      used_at     TIMESTAMP NULL,
      PRIMARY KEY (employee_id, code)
    );
    """)

async def ensure_oauth_table():
    await database.execute("""
    CREATE TABLE IF NOT EXISTS oauth_token (
      provider      TEXT PRIMARY KEY,         -- 'freee'
      access_token  TEXT NOT NULL,
      refresh_token TEXT NOT NULL,
      expires_at    TIMESTAMP NOT NULL,       -- UTC
      token_type    TEXT,
      scope         TEXT,
      updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """)

# ========= DB lazy connect =========
async def ensure_db_ready():
    """必要なときだけ DB 接続 & 初回 DDL 実行。未準備なら 503 を返す。"""
    if not database.is_connected:
        try:
            await database.connect()
        except Exception as e:
            logging.exception("DB not ready: %s", e)
            raise HTTPException(status_code=503, detail="Database not ready")
    if not getattr(app.state, "ddl_done", False):
        await ensure_binding_table()
        await ensure_onboarding_table()
        await ensure_oauth_table()
        app.state.ddl_done = True

# ========= Lifecycle =========
@app.on_event("startup")
async def startup():
    # 起動時は DB に繋がない（PaaS の起動順依存を避ける）
    pass

@app.on_event("shutdown")
async def shutdown():
    if database.is_connected:
        await database.disconnect()

# ========= Shifts APIs =========
@app.post("/postShifts")
async def post_shift(p: ShiftIn):
    await ensure_db_ready()

    WORK_TBL, BREAK_TBL = table_names_for(p.year, p.month)
    try:
        await ensure_month_tables(p.year, p.month)

        async with database.transaction():
            # 1) 休憩 全削除
            await database.execute(
                f"DELETE FROM {BREAK_TBL} WHERE id = :id AND work_date = :wd",
                {"id": p.employee_id, "wd": p.work_date},
            )
            # 2) 勤務 全削除
            await database.execute(
                f"DELETE FROM {WORK_TBL} WHERE id = :id AND work_date = :wd",
                {"id": p.employee_id, "wd": p.work_date},
            )
            # 3) 勤務 再挿入（開始/終了が両方ある場合のみ）
            if p.start_work and p.end_work:
                await database.execute(
                    f"INSERT INTO {WORK_TBL} (id, work_date, start_work, end_work) VALUES (:id, :wd, :sw, :ew)",
                    {"id": p.employee_id, "wd": p.work_date, "sw": p.start_work, "ew": p.end_work},
                )
            # 4) 休憩 再挿入（seq: 1..n）
            if p.breaks:
                insert_break = f"""
                    INSERT INTO {BREAK_TBL} (id, work_date, seq, start_break, end_break)
                    VALUES (:id, :wd, :seq, :sb, :eb)
                """
                values = [
                    {"id": p.employee_id, "wd": p.work_date, "seq": i, "sb": br.start_break, "eb": br.end_break}
                    for i, br in enumerate(p.breaks, start=1)
                ]
                await database.execute_many(insert_break, values)

        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/getDetailShifts")
async def get_shifts(year: int = Query(...), month: int = Query(...), day: int = Query(...)):
    await ensure_db_ready()
    await ensure_month_tables(year, month)

    work_tbl, break_tbl = table_names_for(year, month)
    wd = date(year, month, day)

    work_rows = await database.fetch_all(
        f"SELECT id, start_work, end_work FROM {work_tbl} WHERE work_date = :wd",
        {"wd": wd},
    )
    break_rows = await database.fetch_all(
        f"SELECT id, start_break, end_break, seq FROM {break_tbl} WHERE work_date = :wd ORDER BY id, seq",
        {"wd": wd},
    )

    breaks_by_id = {}
    for r in break_rows:
        breaks_by_id.setdefault(r["id"], []).append({
            "start_break": r["start_break"].strftime("%H:%M:%S"),
            "end_break":   r["end_break"].strftime("%H:%M:%S"),
        })

    result = []
    for w in work_rows:
        result.append({
            "employee_id": w["id"],
            "start_work":  w["start_work"].strftime("%H:%M:%S") if w["start_work"] else None,
            "end_work":    w["end_work"].strftime("%H:%M:%S") if w["end_work"] else None,
            "breaks":      breaks_by_id.get(w["id"], []),
        })
    return result

@app.get("/getWorkMonth")
async def get_work_month(id: int = Query(..., alias="id"), year: int = Query(...), month: int = Query(...)):
    await ensure_db_ready()
    await ensure_month_tables(year, month)

    work_tbl, _ = table_names_for(year, month)
    rows = await database.fetch_all(
        f"""
        SELECT work_date, start_work, end_work
        FROM {work_tbl}
        WHERE id = :id
        ORDER BY work_date
        """,
        {"id": id},
    )
    return [
        {
            "work_date": r["work_date"].isoformat(),
            "start_work": r["start_work"].strftime("%H:%M:%S") if r["start_work"] else None,
            "end_work": r["end_work"].strftime("%H:%M:%S") if r["end_work"] else None,
        }
        for r in rows
    ]

# ========= Onboarding / Binding =========
@app.post("/onboarding/code")
async def issue_code(payload: dict):
    await ensure_db_ready()
    """管理用: 6桁コード発行（有効10分）"""
    employee_id = int(payload.get("employee_id"))
    code = f"{secrets.randbelow(1_000_000):06d}"
    expires = dt.datetime.utcnow() + dt.timedelta(minutes=10)
    await database.execute(
        "INSERT INTO onboarding_code (employee_id, code, expires_at) VALUES (:e,:c,:x)",
        {"e": employee_id, "c": code, "x": expires},
    )
    return {"ok": True, "employee_id": employee_id, "code": code, "expires_at": expires.isoformat()}

@app.post("/bindings/liff")
async def bind_from_liff(p: LiffBindIn):
    await ensure_db_ready()

    # コード検証
    row = await database.fetch_one(
        "SELECT employee_id, expires_at, used_at FROM onboarding_code WHERE employee_id=:e AND code=:c",
        {"e": p.employee_id, "c": p.code},
    )
    if not row:
        raise HTTPException(403, "invalid code")
    if row["used_at"] is not None:
        raise HTTPException(403, "code already used")
    if row["expires_at"] < dt.datetime.utcnow():
        raise HTTPException(403, "code expired")

    # 既存の別社員への紐付けをブロック
    own = await database.fetch_one(
        "SELECT employee_id FROM line_binding WHERE line_user_id=:u",
        {"u": p.line_user_id},
    )
    if own and own["employee_id"] != p.employee_id:
        raise HTTPException(409, "line_user_id already linked to another employee")

    # upsert
    await database.execute("""
      INSERT INTO line_binding (employee_id, line_user_id, display_name, active, verified_at)
      VALUES (:e,:u,:d, TRUE, NOW())
      ON CONFLICT (employee_id) DO UPDATE
         SET line_user_id=:u, display_name=:d, active=TRUE, verified_at=NOW(), updated_at=NOW()
    """, {"e": p.employee_id, "u": p.line_user_id, "d": p.display_name})

    # コード消費
    await database.execute(
        "UPDATE onboarding_code SET used_at=NOW() WHERE employee_id=:e AND code=:c",
        {"e": p.employee_id, "c": p.code},
    )
    return {"ok": True}

@app.get("/bindings")
async def list_bindings(active: Optional[bool] = None):
    await ensure_db_ready()

    where = ""
    params = {}
    if active is not None:
        where = "WHERE active = :active"
        params["active"] = active
    rows = await database.fetch_all(f"""
      SELECT employee_id, line_user_id, display_name, active, verified_at, updated_at
      FROM line_binding
      {where}
      ORDER BY employee_id
    """, params)
    return [dict(r) for r in rows]

# ヘルスチェック（DB 非依存）
@app.get("/healthz")
async def healthz():
    return {"ok": True}

# ========= Freee OAuth token 管理 =========
FREEE_TOKEN_URL = "https://accounts.secure.freee.co.jp/public_api/token"
CLIENT_ID = os.getenv("FREEE_CLIENT_ID")
CLIENT_SECRET = os.getenv("FREEE_CLIENT_SECRET")
INTERNAL_SECRET = os.getenv("INTERNAL_API_KEY")
SKEW = dt.timedelta(seconds=60)

async def _get_freee_row():
    return await database.fetch_one("SELECT * FROM oauth_token WHERE provider='freee'")

async def _save_freee_row(at, rt, exp, typ=None, scope=None):
    await database.execute("""
      INSERT INTO oauth_token(provider,access_token,refresh_token,expires_at,token_type,scope,updated_at)
      VALUES('freee', :at, :rt, :exp, :typ, :scope, NOW())
      ON CONFLICT (provider) DO UPDATE
      SET access_token=:at, refresh_token=:rt, expires_at=:exp, token_type=:typ, scope=:scope, updated_at=NOW()
    """, {"at": at, "rt": rt, "exp": exp, "typ": typ, "scope": scope})

async def _refresh_with_freee(rt: str):
    async with httpx.AsyncClient(timeout=15) as cli:
        resp = await cli.post(
            FREEE_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "refresh_token": rt,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
    if resp.status_code != 200:
        raise HTTPException(502, f"freee token refresh failed: {resp.text}")
    j = resp.json()
    exp = dt.datetime.utcnow() + dt.timedelta(seconds=j.get("expires_in", 21600)) - SKEW
    # freee は refresh_token がローテーションすることがある → あれば必ず保存
    return j["access_token"], j.get("refresh_token", rt), exp, j.get("token_type"), j.get("scope")

@app.get("/oauth/freee/access_token")
async def issue_access_token(x_internal_secret: str = Header(None)):
    await ensure_db_ready()
    if x_internal_secret != INTERNAL_SECRET:
        raise HTTPException(403, "forbidden")

    row = await _get_freee_row()
    if not row:
        raise HTTPException(404, "seed required")

    now = dt.datetime.utcnow()
    if row["expires_at"] and row["expires_at"] > now + SKEW:
        return {"access_token": row["access_token"], "expires_at": row["expires_at"].isoformat()}

    # 期限切れ/間近 → リフレッシュ（簡易二重実行対策で再読込）
    async with database.transaction():
        row = await _get_freee_row()
        if row["expires_at"] and row["expires_at"] > dt.datetime.utcnow() + SKEW:
            return {"access_token": row["access_token"], "expires_at": row["expires_at"].isoformat()}
        at, rt, exp, typ, scope = await _refresh_with_freee(row["refresh_token"])
        await _save_freee_row(at, rt, exp, typ, scope)
        return {"access_token": at, "expires_at": exp.isoformat()}

# 初期投入（最初の1回だけ）※終わったら無効化してもOK
@app.post("/oauth/freee/seed")
async def seed_token(payload: dict, x_internal_secret: str = Header(None)):
    await ensure_db_ready()
    if x_internal_secret != INTERNAL_SECRET:
        raise HTTPException(403, "forbidden")
    at = payload["access_token"]
    rt = payload["refresh_token"]
    exp = payload.get("expires_at")  # ISO でも秒でも可
    if isinstance(exp, (int, float)):
        exp = (dt.datetime.utcnow() + dt.timedelta(seconds=exp)).isoformat()
    await _save_freee_row(at, rt, dt.datetime.fromisoformat(exp.replace("Z","")))
    return {"ok": True}
