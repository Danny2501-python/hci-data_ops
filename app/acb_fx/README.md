# acb_fx — Tỷ giá ACB theo ngày

Lấy bảng tỷ giá ACB (quy ra VND) mỗi ngày vào kho để quy đổi doanh thu ngoại tệ, trước hết là
doanh thu Philippines (PHP) của ERP SUN, schema `ph`. Techcombank và Vietcombank không niêm yết
PHP; ACB thì có.

```
API acb.com.vn ──► bronze.acb_fx.ty_gia_acb ──► silver.acb_fx.ty_gia_acb ──► public.ty_gia_acb
(chốt 23:59/ngày)        (Iceberg, giữ 7 lượt)        (Iceberg, giữ 7 lượt)     (prod + sandbox, toàn bộ lịch sử)
```

| File | Việc |
|---|---|
| `acb_fx_core.py` | Gọi API, tính khoảng ngày, chuẩn hoá bản ghi. Thuần Python, không cần Iceberg |
| `acb_fx_lakehouse.py` | Helper Iceberg (namespace `acb_fx`), theo khuôn `hci_care` |
| `acb_fx_bronze_ingestion.py` | API → bronze. Giữ nguyên mọi bản ghi kèm JSON gốc |
| `acb_fx_silver_transformation.py` | Bỏ dòng vàng (ACB/XAU), chuẩn hoá cột, bỏ trùng |
| `acb_fx_serving_layer.py` | Silver → Postgres. Ghi đè theo **ngày**, 1 transaction |
| `dags/acb_fx/acb_fx_etl_dag.py` | DAG `acb_fx_etl`, chạy 06:15 giờ VN mỗi ngày |

## Nguồn dữ liệu

`GET https://acb.com.vn/api/front/v1/currency?currency=VND&effectiveDateTime=YYYY-MM-DDTHH:MM:SS.mmm`

Đây là API mà chính trang https://acb.com.vn/ty-gia-hoi-doai gọi tới.

- **`effectiveDateTime`**: tính theo giờ Việt Nam. API trả bảng tỷ giá có hiệu lực tại thời điểm đó, nên lấy được cả ngày trong quá khứ.
- **Thời điểm lấy**: pipeline lấy mỗi ngày D tại `D 23:59:59`, tức tỷ giá chốt cuối ngày. Trong một ngày ACB có thể cập nhật nhiều lần.
- **Ngày nghỉ**: API vẫn trả tỷ giá đang hiệu lực, nên ngày nào cũng có số.

## Bảng `public.ty_gia_acb`

| Cột | Ý nghĩa |
|---|---|
| `ngay` | Ngày áp dụng (giờ VN) |
| `loai_tien` | Mã ngoại tệ: USD, PHP, AUD, EUR… |
| `deal_type` | `BID` = ngân hàng mua vào, `ASK` = ngân hàng bán ra |
| `instrument_type` | `TRANSFER` = chuyển khoản, `CASH` = tiền mặt |
| `menh_gia` | Chỉ dùng cho USD: 3 = mệnh giá 50–100 (dòng chuyển khoản của USD cũng là 3), 2 = 5–10–20, 1 = 1–2. Loại tiền khác là 0 |
| `ty_gia` | Số VND đổi được 1 đơn vị ngoại tệ |
| `max_sequence`, `effective_datetime`, `ingestion_timestamp` | Thông tin kỹ thuật |

- **Khoá chính**: `(ngay, loai_tien, deal_type, instrument_type, menh_gia)`.
- **PHP chỉ có 1 dòng mỗi ngày**: `deal_type = 'ASK'`, `instrument_type = 'TRANSFER'` (giá bán chuyển khoản).
- Để tham khảo: lúc 17:00 ngày 10/09/2026 ACB niêm yết PHP là 423,89, sáng 08/10/2026 là 422,92. Số chốt 23:59 trong bảng có thể chênh nhẹ so với các số này.

### Quy đổi doanh thu Philippines

```sql
SELECT p.id, p.payment_date, cur.name AS loai_tien, p.amount,
       tg.ty_gia,
       p.amount * tg.ty_gia AS so_tien_vnd
FROM ph.account_payment p
JOIN ph.res_currency cur ON cur.id = p.currency_id
LEFT JOIN public.ty_gia_acb tg
       ON tg.ngay            = p.payment_date
      AND tg.loai_tien       = cur.name
      AND tg.deal_type       = 'ASK'
      AND tg.instrument_type = 'TRANSFER'
WHERE p.state = 'posted';
```

Phiếu VND không có dòng tỷ giá nên `tg.ty_gia` sẽ NULL. Dùng `coalesce(tg.ty_gia, 1)` khi `cur.name = 'VND'`.

## Chạy

- **Lịch**: hằng ngày 06:15 giờ VN. Mỗi lượt lấy 7 ngày gần nhất tính tới hôm qua và ghi đè các ngày đó, để bắt kịp nếu ACB sửa số. Model dbt nào đọc `ty_gia_acb` thì nên chạy sau khoảng 06:30.
- **Backfill**: Trigger DAG `acb_fx_etl` với config `{"start_date": "2026-09-10", "end_date": "2026-10-07"}`. `end_date` phải trước hôm nay, vì tỷ giá trong ngày chưa chốt.
- **Khi lỗi**: chỉ cần một ngày gọi API lỗi (sau 3 lần thử) là task bronze FAILED. Các tầng sau không chạy và bảng Postgres giữ nguyên.
- **Chạy tay trong pod**:

  ```bash
  python /app/acb_fx/acb_fx_bronze_ingestion.py --snapshot-date 2026-10-08 --start-date 2026-09-10 --end-date 2026-10-07
  python /app/acb_fx/acb_fx_silver_transformation.py --snapshot-date 2026-10-08
  python /app/acb_fx/acb_fx_serving_layer.py --snapshot-date 2026-10-08
  ```
