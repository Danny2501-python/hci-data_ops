# Custom Fields Extraction - Visual Explanation
## Loop Through Objects and Create Columns Dynamically

---

## ✅ Đúng! Bạn Hiểu Chính Xác!

**Process:**
1. Loop qua **từng row** trong DataFrame
2. Loop qua **từng object** trong `custom_fields` array
3. Gặp ID nào **thỏa mãn** → Tạo/Update cột mới
4. Gán value vào **đúng row đó**

---

## 📊 Ví Dụ Cụ Thể - Step by Step

### Input Data (3 tickets)

```python
# DataFrame ban đầu (sau khi call API)
df = pd.DataFrame([
    {
        "ticket_id": 100,
        "ticket_status": "open",
        "custom_fields": '[{"id": 6014, "label": "Ghi chú", "value": "Khách VIP"}, {"id": 7937, "label": "Phân loại", "value": "Tư vấn"}]'
    },
    {
        "ticket_id": 200,
        "ticket_status": "closed",
        "custom_fields": '[{"id": 6014, "label": "Ghi chú", "value": "Khách thường"}, {"id": 7937, "label": "Phân loại", "value": "Hỏi giá"}]'
    },
    {
        "ticket_id": 300,
        "ticket_status": "pending",
        "custom_fields": '[{"id": 6014, "label": "Ghi chú", "value": "Khách mới"}, {"id": 7937, "label": "Phân loại", "value": "Khiếu nại"}]'
    }
])
```

**DataFrame trước khi extract:**
```
   ticket_id  ticket_status  custom_fields
0        100           open  [{"id": 6014, ...}, {"id": 7937, ...}]
1        200         closed  [{"id": 6014, ...}, {"id": 7937, ...}]
2        300        pending  [{"id": 6014, ...}, {"id": 7937, ...}]
```

---

### Step 1: Hardcode IDs (Brand 1)

```python
# Hardcode IDs cho Brand 1
ghi_chu = 6014
id_value_phan_loai_ket_noi = 7937
```

---

### Step 2: Loop Through Each Row

```python
# Loop qua TỪNG ROW
for i, row in df.iterrows():
    # i = 0, 1, 2 (row index)
    # row = data của row đó
    
    custom_fields = row["custom_fields"]
    custom_fields_dict = json.loads(custom_fields)
    
    # custom_fields_dict = [
    #     {"id": 6014, "label": "Ghi chú", "value": "..."},
    #     {"id": 7937, "label": "Phân loại", "value": "..."}
    # ]
```

---

### Step 3: Loop Through Each Object in custom_fields

#### Processing Row 0 (ticket_id = 100)

```python
# Row 0: ticket_id = 100
i = 0
custom_fields_dict = [
    {"id": 6014, "label": "Ghi chú", "value": "Khách VIP"},
    {"id": 7937, "label": "Phân loại", "value": "Tư vấn"}
]

# Loop qua TỪNG OBJECT trong array
for item in custom_fields_dict:
    
    # Object 1: {"id": 6014, "label": "Ghi chú", "value": "Khách VIP"}
    if item["id"] == ghi_chu:  # 6014 == 6014 ✅ MATCH!
        df.at[i, "ghi_chu"] = item["value"]  # ← TẠO CỘT "ghi_chu", GÁN "Khách VIP" vào row 0
    
    # Object 2: {"id": 7937, "label": "Phân loại", "value": "Tư vấn"}
    if item["id"] == id_value_phan_loai_ket_noi:  # 7937 == 7937 ✅ MATCH!
        df.at[i, "id_value_phan_loai_ket_noi"] = item["value"]  # ← TẠO CỘT "id_value_phan_loai_ket_noi", GÁN "Tư vấn" vào row 0
```

**DataFrame sau khi xử lý row 0:**
```
   ticket_id  ticket_status  custom_fields                      ghi_chu  id_value_phan_loai_ket_noi
0        100           open  [...]                              Khách VIP  Tư vấn
1        200         closed  [...]                              NaN        NaN
2        300        pending  [...]                              NaN        NaN
```

---

#### Processing Row 1 (ticket_id = 200)

```python
# Row 1: ticket_id = 200
i = 1
custom_fields_dict = [
    {"id": 6014, "label": "Ghi chú", "value": "Khách thường"},
    {"id": 7937, "label": "Phân loại", "value": "Hỏi giá"}
]

# Loop qua TỪNG OBJECT
for item in custom_fields_dict:
    
    # Object 1
    if item["id"] == ghi_chu:  # 6014 == 6014 ✅
        df.at[i, "ghi_chu"] = item["value"]  # ← GÁN "Khách thường" vào row 1
    
    # Object 2
    if item["id"] == id_value_phan_loai_ket_noi:  # 7937 == 7937 ✅
        df.at[i, "id_value_phan_loai_ket_noi"] = item["value"]  # ← GÁN "Hỏi giá" vào row 1
```

**DataFrame sau khi xử lý row 1:**
```
   ticket_id  ticket_status  custom_fields                      ghi_chu        id_value_phan_loai_ket_noi
0        100           open  [...]                              Khách VIP      Tư vấn
1        200         closed  [...]                              Khách thường   Hỏi giá
2        300        pending  [...]                              NaN            NaN
```

---

#### Processing Row 2 (ticket_id = 300)

```python
# Row 2: ticket_id = 300
i = 2
custom_fields_dict = [
    {"id": 6014, "label": "Ghi chú", "value": "Khách mới"},
    {"id": 7937, "label": "Phân loại", "value": "Khiếu nại"}
]

# Loop qua TỪNG OBJECT
for item in custom_fields_dict:
    
    # Object 1
    if item["id"] == ghi_chu:  # 6014 == 6014 ✅
        df.at[i, "ghi_chu"] = item["value"]  # ← GÁN "Khách mới" vào row 2
    
    # Object 2
    if item["id"] == id_value_phan_loai_ket_noi:  # 7937 == 7937 ✅
        df.at[i, "id_value_phan_loai_ket_noi"] = item["value"]  # ← GÁN "Khiếu nại" vào row 2
```

**DataFrame CUỐI CÙNG:**
```
   ticket_id  ticket_status  custom_fields                      ghi_chu        id_value_phan_loai_ket_noi
0        100           open  [...]                              Khách VIP      Tư vấn
1        200         closed  [...]                              Khách thường   Hỏi giá
2        300        pending  [...]                              Khách mới      Khiếu nại
```

---

## 🔄 Complete Code Flow

```python
# Hardcode IDs
if self.obj_brand.erp_id == 1:
    ghi_chu = 6014
    id_value_phan_loai_ket_noi = 7937

# Loop qua TỪNG ROW
for i, row in df.iterrows():
    custom_fields = row["custom_fields"]
    custom_fields_dict = json.loads(custom_fields)
    
    # Loop qua TỪNG OBJECT trong custom_fields array
    for item in custom_fields_dict:
        
        # Check từng ID
        if item["id"] == ghi_chu:  # 6014
            df.at[i, "ghi_chu"] = item["value"]  # ← TẠO/UPDATE cột "ghi_chu" tại row i
        
        if item["id"] == id_value_phan_loai_ket_noi:  # 7937
            df.at[i, "id_value_phan_loai_ket_noi"] = item["value"]  # ← TẠO/UPDATE cột "id_value_phan_loai_ket_noi" tại row i
```

---

## 📊 Visual Flow

```
DataFrame (3 rows)
│
├─ Row 0 (ticket_id=100)
│  │
│  ├─ custom_fields = [{"id": 6014, ...}, {"id": 7937, ...}]
│  │
│  ├─ Loop object 1: {"id": 6014, "value": "Khách VIP"}
│  │  └─ Match! → df.at[0, "ghi_chu"] = "Khách VIP"
│  │
│  └─ Loop object 2: {"id": 7937, "value": "Tư vấn"}
│     └─ Match! → df.at[0, "id_value_phan_loai_ket_noi"] = "Tư vấn"
│
├─ Row 1 (ticket_id=200)
│  │
│  ├─ custom_fields = [{"id": 6014, ...}, {"id": 7937, ...}]
│  │
│  ├─ Loop object 1: {"id": 6014, "value": "Khách thường"}
│  │  └─ Match! → df.at[1, "ghi_chu"] = "Khách thường"
│  │
│  └─ Loop object 2: {"id": 7937, "value": "Hỏi giá"}
│     └─ Match! → df.at[1, "id_value_phan_loai_ket_noi"] = "Hỏi giá"
│
└─ Row 2 (ticket_id=300)
   │
   ├─ custom_fields = [{"id": 6014, ...}, {"id": 7937, ...}]
   │
   ├─ Loop object 1: {"id": 6014, "value": "Khách mới"}
   │  └─ Match! → df.at[2, "ghi_chu"] = "Khách mới"
   │
   └─ Loop object 2: {"id": 7937, "value": "Khiếu nại"}
      └─ Match! → df.at[2, "id_value_phan_loai_ket_noi"] = "Khiếu nại"
```

---

## 🎯 Key Points

### 1. Nested Loop
```python
for i, row in df.iterrows():           # ← Loop 1: Qua từng ROW
    for item in custom_fields_dict:    # ← Loop 2: Qua từng OBJECT trong array
        if item["id"] == target_id:    # ← Check ID
            df.at[i, "column"] = value # ← Gán value vào ĐÚNG row i
```

### 2. Dynamic Column Creation
- Lần đầu gặp `df.at[i, "ghi_chu"]` → **TẠO** cột "ghi_chu"
- Lần sau gặp `df.at[i, "ghi_chu"]` → **UPDATE** giá trị

### 3. Row-Specific Values
- Mỗi row có `custom_fields` khác nhau
- Extract value và gán vào **ĐÚNG row đó**
- Không ảnh hưởng đến rows khác

---

## 📋 Before vs After

### Before Extraction

```python
df = pd.DataFrame([
    {"ticket_id": 100, "custom_fields": '[{"id": 6014, "value": "Khách VIP"}]'},
    {"ticket_id": 200, "custom_fields": '[{"id": 6014, "value": "Khách thường"}]'}
])

# Columns: ticket_id, custom_fields
```

### After Extraction

```python
# Columns: ticket_id, custom_fields, ghi_chu, id_value_phan_loai_ket_noi

df:
   ticket_id  custom_fields                      ghi_chu        id_value_phan_loai_ket_noi
0        100  [{"id": 6014, ...}]                Khách VIP      Tư vấn
1        200  [{"id": 6014, ...}]                Khách thường   Hỏi giá
```

---

## ✅ Summary

**Đúng như bạn nói:**

1. ✅ **Loop qua các object** trong `custom_fields` array
2. ✅ **Gặp ID nào thỏa mãn** (match với hardcode ID)
3. ✅ **Tạo cột mới** (nếu chưa có)
4. ✅ **Gán dữ liệu vào đúng row đó** (`df.at[i, "column"] = value`)

**Kết quả:**
- Mỗi row có giá trị riêng cho các cột extracted
- Columns được tạo dynamically khi loop
- Data được gán đúng vị trí (row index)
