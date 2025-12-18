
import json
import os
from typing import Any, Dict, List, Tuple
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ... (Giữ nguyên các hàm helper cho đến get_columns)

@task
def get_columns(jdbc: str, user: str, passwd: str, table: str) -> List[str]:
    eng = _connect(jdbc, user, passwd)
    host, port, db = _parse_jdbc_mysql(jdbc)
    # SỬA Ở ĐÂY: Sử dụng dấu ngoặc nhọn đôi cho các biến placeholder của SQL nếu cần
    # Hoặc đơn giản là đảm bảo không có dấu ngoặc nhọn đơn bao quanh :db
    q = text("SELECT column_name FROM information_schema.columns WHERE table_schema=:db AND table_name=:tbl ORDER BY ordinal_position")
    with eng.begin() as conn:
        return [r[0] for r in conn.execute(q, {"db": db, "tbl": table}).fetchall()] # Dùng { } ở đây

@task
def recreate_dest_table(jdbc: str, user: str, passwd: str, table: str, keep_cols: List[str]):
    eng = _connect(jdbc, user, passwd)
    # Dùng { } để thoát dấu ngoặc nhọn cho list comprehension
    col_def = ", ".join([f"`{c}` TEXT" for c in keep_cols]) or "`dummy` INT"
    with eng.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
        conn.execute(text(f"CREATE TABLE `{table}` ({col_def})"))
    return True

@task
def copy_data(src_jdbc: str, dest_jdbc: str, table_source: str, table_dest: str, keep_cols: List[str], user: str, passwd: str):
    src = _connect(src_jdbc, user, passwd)
    dest = _connect(dest_jdbc, user, passwd)
    col_sql = ", ".join(f"`{c}`" for c in keep_cols)
    select_sql = text(f"SELECT {col_sql} FROM `{table_source}`")
    # SỬA Ở ĐÂY: Dấu ngoặc nhọn quanh join cần được thoát
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

# --- FLOW ---
@flow(name="TEST_FLOW_01")
def pipeline_runner(pipeline: Dict[str, Any]):
    # ... các phần còn lại dùng { } cho biến f-string bên trong template
