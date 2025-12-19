
import json
import os
from typing import Any, Dict, List, Tuple
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Helpers bên trong Flow ---
def _parse_jdbc_mysql(jdbc_url: str) -> Tuple[str, int, str]:
    if not jdbc_url.startswith("jdbc:mysql://"):
        raise ValueError(f"Unsupported JDBC URL: {jdbc_url}")
    raw = jdbc_url[len("jdbc:mysql://"):]
    hostport, _, db = raw.partition("/")
    host, sep, port = hostport.partition(":")
    return host, int(port) if sep else 3306, db

def _make_sqlalchemy_url(jdbc_url: str, user: str, passwd: str) -> str:
    host, port, db = _parse_jdbc_mysql(jdbc_url)
    return f"mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}"

def _connect(jdbc_url: str, user: str, passwd: str) -> Engine:
    return create_engine(_make_sqlalchemy_url(jdbc_url, user, passwd), pool_pre_ping=True)

def _extract_drop_columns(pipeline: Dict[str, Any]) -> List[str]:
    cols = []
    for t in pipeline.get("transfers") or []:
        action = t.get("action")
        if not action: continue
        spec = json.loads(action)
        for step in spec.get("transformSteps") or []:
            if step.get("type") == "DropColumns":
                cols += step.get("args", [])
    return sorted(set(cols))

# --- Tasks ---
@task
def get_columns(jdbc: str, user: str, passwd: str, table: str) -> List[str]:
    eng = _connect(jdbc, user, passwd)
    _, _, db = _parse_jdbc_mysql(jdbc)
    q = text("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_schema=:db AND table_name=:tbl ORDER BY ordinal_position
    """)
    with eng.begin() as conn:
        return [r[0] for r in conn.execute(q, {"db": db, "tbl": table}).fetchall()]

@task
def recreate_dest_table(jdbc: str, user: str, passwd: str, table: str, keep_cols: List[str]):
    eng = _connect(jdbc, user, passwd)
    col_def = ", ".join([f"`{c}` TEXT" for c in keep_cols]) or "`dummy` INT"
    with eng.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
        conn.execute(text(f"CREATE TABLE `{table}` ({col_def})"))

@task
def copy_data(src_jdbc, dest_jdbc, table_source, table_dest, keep_cols, user, passwd):
    src = _connect(src_jdbc, user, passwd)
    dest = _connect(dest_jdbc, user, passwd)
    col_sql = ", ".join(f"`{c}`" for c in keep_cols)
    select_sql = text(f"SELECT {col_sql} FROM `{table_source}` ")
    insert_sql = f"INSERT INTO `{table_dest}` ({col_sql}) VALUES ({','.join(['%s'] * len(keep_cols))})"
    
    inserted = 0
    with src.connect() as s, dest.begin() as d:
        rs = s.execution_options(stream_results=True).execute(select_sql)
        batch = []
        for row in rs:
            batch.append(tuple(row))
            if len(batch) >= 1000:
                d.exec_driver_sql(insert_sql, batch)
                inserted += len(batch)
                batch.clear()
        if batch:
            d.exec_driver_sql(insert_sql, batch)
            inserted += len(batch)
    return inserted

@flow(name="Pipeline_1762418079598_135")
def pipeline_runner(pipeline: Dict[str, Any]):
    log = get_run_logger()
    db_user = "root"
    db_pass = "123abc@A"

    src_jdbc = pipeline["sources"][0]["path"]
    source_table = pipeline["sources"][0].get("selectedTable", "account_cms")
    dest_jdbc = pipeline["destinations"][0]["destInfo"]["url"]
    dest_table = pipeline["destinations"][0].get("destTable", source_table)
    drop_cols = _extract_drop_columns(pipeline)

    log.info(f"ETL Start: {source_table} -> {dest_table}")
    
    all_cols = get_columns(src_jdbc, db_user, db_pass, source_table)
    keep_cols = [c for c in all_cols if c not in drop_cols]
    
    recreate_dest_table(dest_jdbc, db_user, db_pass, dest_table, keep_cols)
    inserted = copy_data(src_jdbc, dest_jdbc, source_table, dest_table, keep_cols, db_user, db_pass)
    
    log.info(f"✅ DONE - {inserted} rows copied.")
    return {"inserted": inserted}
