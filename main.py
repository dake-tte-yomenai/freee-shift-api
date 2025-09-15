# main.py
from datetime import date, time, datetime, timedelta
from typing import List, Optional
import os
import logging

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator, Field
import databases
import secrets

# ========= DB =========
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL が未設定です（Render の環境変数に設定してください）")

database = databases.Database(DATABASE_URL)

# ========= FastAPI =========
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 本番は許可ドメインに絞ってください
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
    # 可変デフォルトは Field(default_factory=list) に
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
        app.state.ddl_done = True

# ========= Lifecycle =========
@app.on_event("startup")
async def startup():
    # 起動時は DB に繋がない（Render の起動順による接続拒否で落ちないように）
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
            # 2) 勤務 削除
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
    expires = datetime.utcnow() + timedelta(minutes=10)
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
    if row["expires_at"] < datetime.utcnow():
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

# ヘルスチェック（DB 非依存に）
@app.get("/healthz")
async def healthz():
    return {"ok": True}
