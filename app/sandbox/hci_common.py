# -*- coding: utf-8 -*-
"""
Thư viện dùng chung cho các ETL trong ``app/sandbox``.

Bản port của ``hci_common.pyw`` (Jupyter/Airflow PythonOperator) sang mô hình
KubernetesPodOperator của data_ops.

Khác biệt duy nhất so với bản gốc là **cách lấy kết nối**:

================  =========================================================
Bản gốc           ``PostgresHook(postgres_conn_id=DICT_CONN[name])``
Bản này           URI đọc từ biến môi trường, do DAG truyền vào qua env_vars
================  =========================================================

DAG resolve Airflow Connection thành URI ở phía scheduler rồi truyền xuống pod,
giống hệt cách ``app/jupyter_sync`` và ``app/acb_fx`` đang làm. Nhờ vậy file này
không import airflow và chạy được cả ở local lẫn trong pod.

Biến môi trường
---------------
``POSTGRES_URI``
    URI mặc định, dùng khi gọi hàm mà không truyền tên DB. Đây là DB đích của
    toàn bộ ETL trong thư mục này.
``POSTGRES_URI_<TEN>``
    URI phụ, tra theo tên viết hoa. Ví dụ ``query_df('sanbox', ...)`` sẽ tìm
    ``POSTGRES_URI_SANBOX``.

Giữ nguyên tên và chữ ký hàm của bản gốc để code ETL port sang không phải sửa
lời gọi.
"""

import gc
import inspect
import io
import os
import re
import unicodedata

import numpy as np
import pandas as pd
import psycopg2
from loguru import logger
from psycopg2 import sql as pgsql

__version__ = "1.0.0"

# Tên biến môi trường chứa URI mặc định
DEFAULT_URI_ENV = "POSTGRES_URI"

# Các tên DB ngắn gọn của bản gốc đều trỏ về DB đích duy nhất của pod.
# Giữ lại để code cũ gọi query_df('dev', ...) vẫn chạy.
ALIAS_DEFAULT = {"dev", "product", "new_product", "dev_staging_sci", ""}

_CACHE = {}


# ===========================================================================
# KẾT NỐI
# ===========================================================================


def resolve_uri(dbname=None):
    """Trả về URI Postgres theo tên DB ngắn gọn.

    Tên rỗng, None hoặc nằm trong ALIAS_DEFAULT -> dùng ``POSTGRES_URI``.
    Tên khác -> tìm ``POSTGRES_URI_<TEN_VIET_HOA>``.
    """
    name = (dbname or "").strip()
    env = DEFAULT_URI_ENV if name.lower() in ALIAS_DEFAULT else f"{DEFAULT_URI_ENV}_{name.upper()}"
    uri = os.getenv(env)
    if not uri:
        raise RuntimeError(
            err_msg(
                f"Thiếu biến môi trường {env} (tên DB '{dbname}'). "
                f"Kiểm tra env_vars trong DAG."
            )
        )
    return uri


def connect_database(dbname=None):
    """Trả về psycopg2 connection. Nhớ đóng sau khi dùng."""
    return psycopg2.connect(
        resolve_uri(dbname),
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def get_engine(dbname=None):
    """Trả về SQLAlchemy engine, dùng cho pd.read_sql() / df.to_sql()."""
    from sqlalchemy import create_engine

    return create_engine(resolve_uri(dbname))


# Giữ tương thích với code cũ: hai hàm này không còn ý nghĩa ngoài Airflow,
# nhưng vẫn được gọi rải rác nên để lại dạng tối giản.
def get_conn_id(dbname=None):
    name = (dbname or "").strip()
    return DEFAULT_URI_ENV if name.lower() in ALIAS_DEFAULT else f"{DEFAULT_URI_ENV}_{name.upper()}"


def get_hook(dbname=None):
    raise NotImplementedError(
        err_msg(
            "get_hook() không dùng được trong pod (không có Airflow). "
            "Dùng connect_database() hoặc get_engine() thay thế."
        )
    )


# ===========================================================================
# TRUY VẤN
# ===========================================================================


def query_df(conn=None, query="", ret="df", params=None):
    """Thực thi SQL, trả kết quả theo tham số ``ret``.

    Args:
        conn:   tên DB ngắn gọn. Bỏ trống thì dùng POSTGRES_URI.
        query:  câu SQL. Dùng %s làm placeholder.
        ret:    'df' -> DataFrame | 'exe' -> số dòng ảnh hưởng | 'cols' -> tên cột
        params: tuple/dict giá trị cho placeholder.
    """
    if ret not in ("df", "exe", "cols"):
        raise ValueError(err_msg("ret chỉ nhận 'df', 'exe' hoặc 'cols'"))

    conn_db = connect_database(conn)
    try:
        with conn_db.cursor() as cur:
            cur.execute(query, params)
            if ret == "df":
                cols = [str(d[0]) for d in cur.description]
                result = pd.DataFrame(cur.fetchall(), columns=cols)
            elif ret == "exe":
                result = cur.rowcount
                logger.info(f"Đã thực thi lệnh SQL ({result} dòng)")
            else:
                result = [d[0] for d in cur.description]
        conn_db.commit()
        return result
    except Exception:
        conn_db.rollback()
        raise
    finally:
        conn_db.close()


# ===========================================================================
# TIỆN ÍCH CHUỖI / KIỂU DỮ LIỆU
# ===========================================================================


def check_substr(input_str, re_str, vect=False):
    re_str = "" if re_str is None else re_str.lower()
    if vect:
        return input_str.map(str).str.lower().str.contains(re_str, regex=True)
    return bool(re.search(re_str, str(input_str).lower()))


def get_fmatch(input_str, re_str):
    """Trả về đoạn khớp regex đầu tiên, không khớp thì trả None."""
    s_match = re.search(re_str, str(input_str), re.I)
    return s_match[0] if s_match else None


def fix_type(df, ls_col_name, as_type: str, fill_na=0):
    """Ép kiểu các cột trong DataFrame. Sửa trực tiếp trên df và trả về df."""
    ls_col_name = [str(c) for c in dict.fromkeys(ls_col_name) if str(c) in df.columns]
    for c in ls_col_name:
        try:
            if as_type == "Int64":
                df[c] = np.floor(pd.to_numeric(df[c]))
            df[c] = df[c].fillna(fill_na).astype(as_type)
        except (ValueError, TypeError) as e:
            logger.warning(f"Không thể chuyển đổi cột {c} sang kiểu {as_type}: {e}")
    return df


def join_cols(df, cols, sep="-", na="0"):
    s = df[cols].astype("string").fillna(na)
    return s[cols[0]].str.cat(s[cols[1:]], sep=sep)


def remove_accent(str_ip, keep_space=False):
    """Bỏ dấu, chuyển thường, xoá ký tự không phải chữ/số."""
    str_ip = str(str_ip).lower().replace("đ", "d")
    chuoi_normalized = (
        unicodedata.normalize("NFKD", str_ip).encode("ASCII", "ignore").decode("utf-8")
    )
    if keep_space:
        result = re.sub(r"[^\w\s]|_", "", chuoi_normalized)
        return re.sub(r"\s+", " ", result).strip()
    return re.sub(r"\W+", "", chuoi_normalized)


# ===========================================================================
# GHI DỮ LIỆU XUỐNG DATABASE
# ===========================================================================

pg_int_oids = {20, 21, 23}


def _split_table(table):
    """'schema.table' -> ('schema', 'table'); 'table' -> ('public', 'table')."""
    parts = str(table).split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])


def _table_identifier(table):
    schema, name = _split_table(table)
    return pgsql.Identifier(schema, name)


def get_table_columns(cur, table):
    """Trả về dict {tên_cột: type_oid} của bảng; None nếu bảng không tồn tại."""
    schema, name = _split_table(table)
    cur.execute(
        """SELECT 1 FROM information_schema.tables
           WHERE table_type = 'BASE TABLE' AND table_schema = %s AND table_name = %s""",
        (schema, name),
    )
    if cur.fetchone() is None:
        return None
    cur.execute(pgsql.SQL("SELECT * FROM {} LIMIT 0").format(_table_identifier(table)))
    return {d[0]: d[1] for d in cur.description}


def copy_df_to_table(cur, df, table, columns, int_columns=()):
    """COPY df[columns] vào bảng. KHÔNG commit, để người gọi quyết định transaction."""
    if df is None or df.empty:
        return 0
    data = df[columns].copy()
    if int_columns:
        fix_type(data, list(int_columns), "Int64", np.nan)
    buf = io.StringIO()
    data.to_csv(buf, index=False, header=False)
    buf.seek(0)
    copy_sql = pgsql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER ',')"
    ).format(
        _table_identifier(table),
        pgsql.SQL(", ").join(pgsql.Identifier(c) for c in columns),
    )
    cur.copy_expert(copy_sql.as_string(cur), buf)
    return len(data)


def insert_data_to_db(df, table="table_name", conn=None, chunk_size=100_000):
    """Chèn DataFrame vào bảng Postgres bằng COPY, trong một transaction."""
    if df is None or df.empty:
        logger.warning("Dataframe không có dữ liệu (0 dòng)")
        return table

    conn_db = connect_database(conn)
    try:
        with conn_db.cursor() as cur:
            dtype_col = get_table_columns(cur, table)
            if dtype_col is None:
                logger.error(f"Database không tồn tại tên Bảng: {table}")
                return table

            existing_columns = list(dtype_col.keys())
            miss_columns = [c for c in existing_columns if c not in df.columns]
            if miss_columns:
                logger.warning(f"Dữ liệu đầu vào thiếu cột: {', '.join(miss_columns)}")
                return table

            col_int = [k for k, v in dtype_col.items() if v in pg_int_oids]
            total = 0
            for start in range(0, len(df), chunk_size):
                total += copy_df_to_table(
                    cur,
                    df.iloc[start : start + chunk_size],
                    table,
                    existing_columns,
                    col_int,
                )
        conn_db.commit()
        logger.success(f'HOÀN THÀNH! Bảng "{table}": {total} bản ghi.')
    except Exception as e:
        conn_db.rollback()
        logger.error(f'Lỗi khi insert vào bảng "{table}": {e}')
        raise
    finally:
        conn_db.close()
    return table


def replace_table_data(df, table, where=None, conn=None, chunk_size=100_000):
    """Xoá dữ liệu cũ rồi COPY dữ liệu mới, trong CÙNG một transaction."""
    if df is None or df.empty:
        raise ValueError(
            err_msg(f'DataFrame rỗng, không ghi vào "{table}". Kiểm tra lại bước lấy dữ liệu.')
        )

    conn_db = connect_database(conn)
    try:
        with conn_db.cursor() as cur:
            dtype_col = get_table_columns(cur, table)
            if dtype_col is None:
                raise ValueError(f"Database không tồn tại tên Bảng: {table}")

            existing_columns = list(dtype_col.keys())
            miss_columns = [c for c in existing_columns if c not in df.columns]
            if miss_columns:
                raise ValueError(f"Dữ liệu đầu vào thiếu cột: {', '.join(miss_columns)}")

            cur.execute(f"DELETE FROM {table}" + (f" WHERE {where}" if where else ""))
            deleted = cur.rowcount

            col_int = [k for k, v in dtype_col.items() if v in pg_int_oids]
            total = 0
            for start in range(0, len(df), chunk_size):
                total += copy_df_to_table(
                    cur, df.iloc[start : start + chunk_size], table, existing_columns, col_int
                )
        conn_db.commit()
        logger.success(f'HOÀN THÀNH "{table}": xoá {deleted} dòng, ghi {total} dòng.')
        return total
    except Exception:
        conn_db.rollback()
        raise
    finally:
        conn_db.close()


def get_df_cache(dfname, dict_get_df, conn=None):
    """Lấy DataFrame tra cứu, cache trong vòng đời của pod."""
    if dict_get_df is None or dfname not in dict_get_df:
        logger.warning("Chưa tồn tại tên gọi DataFrame trong Dict")
        return None
    if dfname not in _CACHE:
        df = query_df(conn, dict_get_df[dfname])
        if "employee" in df.columns:
            df["employee"] = df["employee"].str.title()
            ls_col_int = [
                c
                for c in df.columns
                if check_substr(c, r"(id|phanquyen)$")
                if "identif" not in c
            ]
            fix_type(df, ls_col_int, "Int64", np.nan)
        _CACHE[dfname] = df
    return _CACHE[dfname]


# ===========================================================================
# THÔNG BÁO
# ===========================================================================

# Log của pod đi vào Airflow, không hiển thị màu -> tắt ANSI.
USE_ANSI = os.getenv("HCI_USE_ANSI", "0") == "1"

_ANSI = {"do": "\033[1;31m", "vang": "\033[1;33m", "xanh": "\033[1;32m", "tat": "\033[0m"}
_VIEN = "=" * 68


def _to_mau(text, mau):
    return f"{_ANSI[mau]}{text}{_ANSI['tat']}" if USE_ANSI else text


def err_msg(msg, icon="[LOI]"):
    """Bọc thông báo lỗi. Dùng: raise ValueError(err_msg('...'))."""
    return _to_mau(f"\n{_VIEN}\n{icon}  {msg}\n{_VIEN}", "do")


def warn_msg(msg, icon="[CANH BAO]"):
    return _to_mau(f"{icon}  {msg}", "vang")


def ok_msg(msg, icon="[OK]"):
    return _to_mau(f"{icon}  {msg}", "xanh")


# ===========================================================================
# DỌN DẸP
# ===========================================================================


def clean_temp_table(conn=None, namespace=None):
    """Xoá các bảng tạm có tên bắt đầu bằng 'temp'.

    Bản gốc quét ``globals()`` của chính hci_common — trong pod thì namespace đó
    rỗng nên không xoá được gì. Bản này quét namespace của nơi gọi, hoặc nhận
    namespace truyền vào.
    """
    if namespace is None:
        namespace = inspect.currentframe().f_back.f_globals
    temp_table = sorted(
        {
            v
            for k, v in namespace.items()
            if isinstance(v, str) and v.startswith("temp") and not k.startswith("_")
        }
    )
    if not temp_table:
        logger.info("Không có bảng tạm nào để dọn.")
        return []
    qr_clean = ";\n".join(f"DROP TABLE IF EXISTS {tb};" for tb in temp_table)
    query_df(conn, qr_clean, ret="exe")
    logger.info(f"Đã dọn {len(temp_table)} bảng tạm: {', '.join(temp_table)}")
    return temp_table


def clear_df(dname=None, namespace=None, keep=(), verbose=True):
    """Xoá biến DataFrame ở nơi gọi hàm và giải phóng bộ nhớ."""
    if namespace is None:
        namespace = inspect.currentframe().f_back.f_globals

    keep = set(keep)
    removed = []
    for name in list(namespace):
        if name.startswith("_") or name in keep:
            continue
        if dname and not name.startswith(dname):
            continue
        if isinstance(namespace.get(name), pd.DataFrame):
            del namespace[name]
            removed.append(name)
    gc.collect()
    if verbose:
        ds = ", ".join(sorted(removed)) if removed else "không có biến nào"
        logger.info(f"Đã dọn {len(removed)} DataFrame: {ds}")
    return removed
