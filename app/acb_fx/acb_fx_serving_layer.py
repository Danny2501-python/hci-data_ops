"""
ACB FX – SERVING layer
silver.acb_fx.ty_gia_acb (Iceberg) -> Postgres <TARGET_SCHEMA>.<TARGET_TABLE> (mặc định public.ty_gia_acb)

Bảng Postgres giữ TOÀN BỘ lịch sử (Iceberg chỉ giữ vài ngày). Mỗi lượt chạy:
  1 transaction: COPY vào bảng tạm -> xoá các NGÀY có trong lượt chạy -> insert lại -> COMMIT.
  Lỗi -> ROLLBACK, bảng giữ nguyên dữ liệu cũ. Ngày không có trong lượt chạy không bị động tới.
Khoá chính: (ngay, loai_tien, deal_type, instrument_type, menh_gia).

Env: POSTGRES_URI (dwh_product_new / dwh_sandbox), TARGET_SCHEMA, TARGET_TABLE
Ví dụ:
  python acb_fx_serving_layer.py --snapshot-date 2026-10-08
"""

import argparse
import io
import os
import sys

import psycopg2
import pyarrow.csv as pacsv
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from acb_fx.acb_fx_core import parse_date_arg  # noqa: E402
from acb_fx.acb_fx_lakehouse import load_table, read_partition  # noqa: E402

POSTGRES_URI = os.getenv("POSTGRES_URI")
TARGET_SCHEMA = os.getenv("TARGET_SCHEMA", "public")
TARGET_TABLE = os.getenv("TARGET_TABLE", "ty_gia_acb")

COLUMNS = ["ngay", "loai_tien", "deal_type", "instrument_type", "menh_gia", "ty_gia",
           "max_sequence", "effective_datetime", "ingestion_timestamp"]

DDL = """
CREATE TABLE IF NOT EXISTS {full} (
    ngay                date          NOT NULL,
    loai_tien           text          NOT NULL,
    deal_type           text          NOT NULL,
    instrument_type     text          NOT NULL,
    menh_gia            integer       NOT NULL DEFAULT 0,
    ty_gia              numeric(20,6) NOT NULL,
    max_sequence        integer,
    effective_datetime  text,
    ingestion_timestamp timestamp,
    PRIMARY KEY (ngay, loai_tien, deal_type, instrument_type, menh_gia)
)
"""

COMMENTS = {
    None: "Tỷ giá ACB quy ra VND theo ngày (chốt 23:59 giờ VN), nguồn API acb.com.vn/ty-gia-hoi-doai. "
          "Pipeline data_ops dags/acb_fx. Quy đổi PHP: loai_tien='PHP', deal_type='ASK', "
          "instrument_type='TRANSFER' (ACB chỉ niêm yết giá bán chuyển khoản cho PHP).",
    "ngay": "Ngày áp dụng tỷ giá (giờ VN)",
    "loai_tien": "Mã ngoại tệ: USD, PHP, AUD, EUR, ...",
    "deal_type": "BID = ngân hàng mua vào, ASK = ngân hàng bán ra",
    "instrument_type": "TRANSFER = chuyển khoản, CASH = tiền mặt",
    "menh_gia": "Chỉ USD: 3 = mệnh giá 50-100 (dòng chuyển khoản cũng là 3), 2 = 5-10-20, 1 = 1-2; 0 = loại tiền không phân mệnh giá",
    "ty_gia": "Số VND cho 1 đơn vị ngoại tệ",
    "max_sequence": "Số lần ACB cập nhật bảng tỷ giá trong ngày (theo API)",
    "effective_datetime": "Thời điểm gọi API (giờ VN), mặc định 23:59:59 của ngày",
    "ingestion_timestamp": "Thời điểm pipeline xử lý (giờ VN)",
}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def load(conn, data) -> int:
    full = f"{q(TARGET_SCHEMA)}.{q(TARGET_TABLE)}"
    cols = ", ".join(q(c) for c in COLUMNS)
    buf = io.BytesIO()
    pacsv.write_csv(data, buf)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {q(TARGET_SCHEMA)}")
        cur.execute(DDL.format(full=full))
        for col, text in COMMENTS.items():
            target = f"TABLE {full}" if col is None else f"COLUMN {full}.{q(col)}"
            cur.execute(f"COMMENT ON {target} IS %s", (text,))
        cur.execute(f"CREATE TEMP TABLE tmp_ty_gia_acb (LIKE {full} INCLUDING DEFAULTS) ON COMMIT DROP")
        cur.copy_expert(f"COPY tmp_ty_gia_acb ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)", buf)
        cur.execute(f"DELETE FROM {full} t USING (SELECT DISTINCT ngay FROM tmp_ty_gia_acb) d "
                    f"WHERE t.ngay = d.ngay")
        deleted = cur.rowcount
        cur.execute(f"INSERT INTO {full} ({cols}) SELECT {cols} FROM tmp_ty_gia_acb")
        inserted = cur.rowcount
    conn.commit()
    logger.info(f"{full}: xoá {deleted} dòng cũ của các ngày trong lượt chạy, ghi {inserted} dòng")
    return inserted


def parse_args():
    p = argparse.ArgumentParser(description="ACB FX serving layer")
    p.add_argument("--snapshot-date", required=True)
    return p.parse_args()


def main():
    snapshot_date = parse_date_arg(parse_args().snapshot_date)
    if not POSTGRES_URI:
        logger.error("Thiếu env POSTGRES_URI")
        sys.exit(1)
    logger.info(f"Serving acb_fx -> {TARGET_SCHEMA}.{TARGET_TABLE} | snapshot {snapshot_date}")

    data = read_partition(load_table("silver"), snapshot_date).select(COLUMNS)
    if data.num_rows == 0:
        logger.error(f"silver.acb_fx.ty_gia_acb không có dữ liệu snapshot {snapshot_date} -> không ghi")
        sys.exit(1)

    conn = psycopg2.connect(POSTGRES_URI)
    try:
        n = load(conn, data)
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        logger.error(f"❌ Ghi Postgres lỗi, đã rollback (bảng giữ nguyên): {e}")
        sys.exit(1)
    finally:
        conn.close()
    logger.success(f"✅ {TARGET_SCHEMA}.{TARGET_TABLE}: {n} dòng")


if __name__ == "__main__":
    main()
