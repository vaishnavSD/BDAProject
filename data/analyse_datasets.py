"""
Deep CSV Analysis Script
Covers: columns, dtypes, row counts, nulls, duplicates, invalid values,
        cardinality, FK relationship checks, join column identification.
"""

import csv
import os
from collections import Counter, defaultdict

BASE = r"c:\Users\DELL\OneDrive\Desktop\CDAC\BigDataProject\data\raw"

FILES = {
    "suppliers":                  os.path.join(BASE, "reference",    "suppliers.csv"),
    "stores":                     os.path.join(BASE, "reference",    "stores.csv"),
    "products":                   os.path.join(BASE, "reference",    "products.csv"),
    "orders":                     os.path.join(BASE, "transactions",  "orders.csv"),
    "shipments":                  os.path.join(BASE, "transactions",  "shipments.csv"),
    "stock_movements":            os.path.join(BASE, "transactions",  "stock_movements.csv"),
    "historical_sales":           os.path.join(BASE, "historical",   "historical_sales.csv"),
    "inventory_demand_stockout_risk": os.path.join(BASE, "historical","inventory_demand_stockout_risk.csv"),
    "live_orders":                os.path.join(BASE, "streaming",    "live_orders.csv"),
    "live_shipments":             os.path.join(BASE, "streaming",    "live_shipments.csv"),
    "live_dispatches":            os.path.join(BASE, "streaming",    "live_dispatches.csv"),
}

VALID_STATUS = {
    "orders":     {"PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"},
    "shipments":  {"IN_TRANSIT","DELIVERED","DELAYED","RETURNED"},
    "movements":  {"INBOUND","OUTBOUND","ADJUSTMENT","TRANSFER","RETURN"},
}

def load_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def analyse(name, rows):
    print(f"\n{'='*70}")
    print(f"  DATASET: {name.upper()}")
    print(f"{'='*70}")
    if not rows:
        print("  [EMPTY FILE]")
        return {}

    columns = list(rows[0].keys())
    total   = len(rows)
    print(f"  Row count : {total}")
    print(f"  Columns   : {len(columns)}")
    print(f"  Fields    : {', '.join(columns)}")

    # Null / empty analysis
    print(f"\n  --- Null / Empty Values ---")
    null_counts = defaultdict(int)
    for row in rows:
        for col in columns:
            if row[col].strip() == "" or row[col].strip().upper() == "NULL":
                null_counts[col] += 1
    if null_counts:
        for col, cnt in null_counts.items():
            print(f"    {col:<35} {cnt:>5} nulls  ({cnt/total*100:.1f}%)")
    else:
        print("    No null values found.")

    # Duplicate rows
    print(f"\n  --- Duplicate Rows ---")
    seen = set()
    dup_count = 0
    for row in rows:
        key = tuple(row.values())
        if key in seen:
            dup_count += 1
        seen.add(key)
    print(f"    Duplicate rows: {dup_count}")

    # Cardinality of key columns
    print(f"\n  --- Column Cardinality (unique values) ---")
    for col in columns:
        vals = [r[col].strip() for r in rows]
        unique = len(set(vals))
        print(f"    {col:<35} {unique:>5} unique values")

    # Numeric range checks
    print(f"\n  --- Numeric Range Checks ---")
    numeric_cols = []
    for col in columns:
        sample = [r[col].strip() for r in rows if r[col].strip() != ""][:20]
        is_num = all(_is_numeric(v) for v in sample) if sample else False
        if is_num:
            numeric_cols.append(col)
    for col in numeric_cols:
        vals = []
        for r in rows:
            v = r[col].strip()
            if v and _is_numeric(v):
                vals.append(float(v))
        if vals:
            neg = sum(1 for v in vals if v < 0)
            zero = sum(1 for v in vals if v == 0)
            print(f"    {col:<35} min={min(vals):.2f}  max={max(vals):.2f}  negatives={neg}  zeros={zero}")

    # Invalid categorical values
    print(f"\n  --- Categorical Value Checks ---")
    if name == "orders":
        _check_enum(rows, "status", VALID_STATUS["orders"])
    if name == "shipments":
        _check_enum(rows, "status", VALID_STATUS["shipments"])
    if name == "stock_movements":
        _check_enum(rows, "movement_type", VALID_STATUS["movements"])
    if name in ("inventory_demand_stockout_risk",):
        _check_bool(rows, "stockout_risk_flag")
        _check_bool(rows, "overstock_flag")

    result = {col: set(r[col].strip() for r in rows if r[col].strip()) for col in columns}
    return result


def _is_numeric(v):
    try:
        float(v)
        return True
    except ValueError:
        return False

def _check_enum(rows, col, valid_set):
    invalids = [r[col] for r in rows if r[col].strip() not in valid_set]
    if invalids:
        c = Counter(invalids)
        print(f"    INVALID '{col}' values: {dict(c)}")
    else:
        print(f"    '{col}' — all values valid.")

def _check_bool(rows, col):
    valid = {"TRUE","FALSE","1","0","true","false"}
    invalids = [r[col] for r in rows if r[col].strip() not in valid]
    if invalids:
        print(f"    INVALID '{col}' values: {Counter(invalids)}")
    else:
        print(f"    '{col}' — all values valid.")

# ─────────────────────────────────────────────
# LOAD ALL
# ─────────────────────────────────────────────
data = {}
for name, path in FILES.items():
    rows = load_csv(path)
    data[name] = rows

# ─────────────────────────────────────────────
# ANALYSE ALL
# ─────────────────────────────────────────────
col_data = {}
for name, rows in data.items():
    col_data[name] = analyse(name, rows)

# ─────────────────────────────────────────────
# FK RELATIONSHIP VERIFICATION
# ─────────────────────────────────────────────
print(f"\n{'='*70}")
print("  FOREIGN KEY RELATIONSHIP VERIFICATION")
print(f"{'='*70}")

def fk_check(child_name, child_col, parent_name, parent_col):
    child_rows  = data[child_name]
    parent_vals = col_data[parent_name].get(parent_col, set())
    child_vals  = set(r[child_col].strip() for r in child_rows if r[child_col].strip())
    orphans     = child_vals - parent_vals
    print(f"  {child_name}.{child_col} -> {parent_name}.{parent_col}")
    if orphans:
        print(f"    ⚠  Orphan values ({len(orphans)}): {list(orphans)[:5]}")
    else:
        print(f"    ✓  All values resolve correctly.")

fk_check("products",     "supplier_id",  "suppliers", "supplier_id")
fk_check("orders",       "product_id",   "products",  "product_id")
fk_check("orders",       "store_id",     "stores",    "store_id")
fk_check("orders",       "supplier_id",  "suppliers", "supplier_id")
fk_check("shipments",    "order_id",     "orders",    "order_id")
fk_check("shipments",    "product_id",   "products",  "product_id")
fk_check("shipments",    "store_id",     "stores",    "store_id")
fk_check("shipments",    "supplier_id",  "suppliers", "supplier_id")
fk_check("stock_movements","product_id", "products",  "product_id")
fk_check("stock_movements","store_id",   "stores",    "store_id")
fk_check("historical_sales","product_id","products",  "product_id")
fk_check("historical_sales","store_id",  "stores",    "store_id")
fk_check("inventory_demand_stockout_risk","product_id","products","product_id")
fk_check("inventory_demand_stockout_risk","store_id",  "stores",  "store_id")
fk_check("live_orders",  "product_id",   "products",  "product_id")
fk_check("live_orders",  "store_id",     "stores",    "store_id")
fk_check("live_orders",  "supplier_id",  "suppliers", "supplier_id")
fk_check("live_shipments","product_id",  "products",  "product_id")
fk_check("live_shipments","store_id",    "stores",    "store_id")
fk_check("live_dispatches","product_id", "products",  "product_id")
fk_check("live_dispatches","from_store_id","stores",  "store_id")
fk_check("live_dispatches","to_store_id",  "stores",  "store_id")

print(f"\n{'='*70}")
print("  JOIN COLUMN SUMMARY")
print(f"{'='*70}")
print("""
  Common join keys identified:
  ┌─────────────────┬──────────────────────────────────────────────────────┐
  │ Column          │ Present In                                           │
  ├─────────────────┼──────────────────────────────────────────────────────┤
  │ product_id      │ products, orders, shipments, stock_movements,        │
  │                 │ historical_sales, inventory_demand_stockout_risk,    │
  │                 │ live_orders, live_shipments, live_dispatches          │
  ├─────────────────┼──────────────────────────────────────────────────────┤
  │ store_id        │ stores, orders, shipments, stock_movements,          │
  │                 │ historical_sales, inventory_demand_stockout_risk,    │
  │                 │ live_orders, live_shipments, live_dispatches          │
  ├─────────────────┼──────────────────────────────────────────────────────┤
  │ supplier_id     │ suppliers, products, orders, shipments, live_orders  │
  ├─────────────────┼──────────────────────────────────────────────────────┤
  │ order_id        │ orders, shipments                                    │
  ├─────────────────┼──────────────────────────────────────────────────────┤
  │ shipment_id     │ shipments, live_shipments                            │
  └─────────────────┴──────────────────────────────────────────────────────┘
""")
print("Analysis complete.")
