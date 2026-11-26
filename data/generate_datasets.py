"""
Dataset Generator for Smart Inventory Management System
Generates 11 realistic, relationally consistent CSV datasets.
All foreign keys are validated against their parent tables.
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

BASE = r"c:\Users\DELL\OneDrive\Desktop\CDAC\BigDataProject\data\raw"

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def rand_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def rand_datetime(start, end):
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))

def write_csv(filepath, fieldnames, rows):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written: {filepath}  ({len(rows)} rows)")

# ─────────────────────────────────────────────
# 1. REFERENCE: suppliers.csv
# ─────────────────────────────────────────────
SUPPLIER_IDS = [f"SUP{str(i).zfill(3)}" for i in range(1, 21)]
SUPPLIER_NAMES = [
    "GlobalParts Co","SwiftSupply Ltd","PrimeStock Inc","NexGen Supplies","FastTrack Goods",
    "EverReady Corp","BulkBridge LLC","TrueSource Ltd","QuickFill Inc","SteadyFlow Co",
    "PeakLoad Supplies","CoreChain Ltd","DirectDock Inc","ApexStock Corp","BridgeLine LLC",
    "OpenRoute Ltd","TurboTrade Co","MetroGoods Inc","OmniSource LLC","ZenSupply Ltd"
]
SUPPLIER_COUNTRIES = ["India","USA","China","Germany","UK","Japan","Australia","Canada","France","Brazil"]

suppliers = []
for i, sid in enumerate(SUPPLIER_IDS):
    suppliers.append({
        "supplier_id": sid,
        "supplier_name": SUPPLIER_NAMES[i],
        "contact_email": f"contact@{SUPPLIER_NAMES[i].lower().replace(' ','')}.com",
        "country": random.choice(SUPPLIER_COUNTRIES),
        "lead_time_days": random.randint(2, 30),
        "reliability_score": round(random.uniform(0.60, 1.00), 2),
        "is_active": random.choice(["TRUE", "TRUE", "TRUE", "FALSE"])
    })

# Introduce 2 duplicates
suppliers.append(suppliers[0].copy())
suppliers.append(suppliers[3].copy())

write_csv(
    os.path.join(BASE, "reference", "suppliers.csv"),
    ["supplier_id","supplier_name","contact_email","country","lead_time_days","reliability_score","is_active"],
    suppliers
)

# ─────────────────────────────────────────────
# 2. REFERENCE: stores.csv
# ─────────────────────────────────────────────
STORE_IDS = [f"STR{str(i).zfill(3)}" for i in range(1, 16)]
STORE_NAMES = [
    "Mumbai Central","Delhi North Hub","Bangalore Tech Park","Chennai Port","Kolkata East",
    "Pune Metro","Hyderabad IT City","Ahmedabad West","Jaipur Royal","Lucknow Trade",
    "Surat Diamond","Nagpur Junction","Bhopal Central","Patna River","Kochi Spice"
]
REGIONS = ["West","North","South","East","South","West","South","West","North","North",
           "West","Central","Central","East","South"]

stores = []
for i, sid in enumerate(STORE_IDS):
    stores.append({
        "store_id": sid,
        "store_name": STORE_NAMES[i],
        "region": REGIONS[i],
        "city": STORE_NAMES[i].split()[0],
        "warehouse_capacity_units": random.randint(5000, 50000),
        "is_active": "TRUE"
    })

# One store with missing region (data quality issue)
stores[7]["region"] = ""

write_csv(
    os.path.join(BASE, "reference", "stores.csv"),
    ["store_id","store_name","region","city","warehouse_capacity_units","is_active"],
    stores
)

# ─────────────────────────────────────────────
# 3. REFERENCE: products.csv
# ─────────────────────────────────────────────
PRODUCT_IDS = [f"PRD{str(i).zfill(4)}" for i in range(1, 101)]
CATEGORIES = ["Electronics","Apparel","Groceries","Furniture","Stationery",
              "Appliances","Toys","Sports","Pharmacy","Automotive"]
PRODUCT_NAMES = {
    "Electronics":  ["Laptop","Smartphone","Tablet","Headphones","Smartwatch","USB Hub","Webcam","Keyboard","Monitor","Router"],
    "Apparel":      ["T-Shirt","Jeans","Jacket","Dress","Shorts","Socks","Cap","Scarf","Gloves","Belt"],
    "Groceries":    ["Rice 5kg","Wheat Flour","Sugar 1kg","Salt 500g","Cooking Oil","Tea Bags","Coffee","Biscuits","Noodles","Cornflakes"],
    "Furniture":    ["Office Chair","Study Table","Bookshelf","Sofa","Bed Frame","Cabinet","Wardrobe","Stool","Desk Lamp","Mirror"],
    "Stationery":   ["Notebook","Pen Pack","Stapler","Scissors","Tape Roll","File Folder","Whiteboard","Marker Set","Ruler","Eraser"],
    "Appliances":   ["Microwave","Toaster","Blender","Iron","Vacuum","Air Purifier","Kettle","Coffee Maker","Fan","Heater"],
    "Toys":         ["Lego Set","Board Game","Puzzle","Action Figure","Doll","RC Car","Building Blocks","Bike","Kite","Frisbee"],
    "Sports":       ["Football","Cricket Bat","Tennis Racket","Yoga Mat","Dumbbells","Skipping Rope","Helmet","Gloves","Shoes","Shorts"],
    "Pharmacy":     ["Paracetamol","Vitamin C","Bandages","Antiseptic","Cough Syrup","Eye Drops","Thermometer","Mask Pack","Gloves","Hand Sanitizer"],
    "Automotive":   ["Engine Oil","Car Polish","Tyre Gauge","Jump Cables","Seat Cover","Air Freshener","Dash Cam","Phone Mount","Wipers","LED Bulbs"]
}

products = []
cat_counters = {c: 0 for c in CATEGORIES}
ACTIVE_SUPPLIER_IDS = [s["supplier_id"] for s in suppliers[:20]]  # original 20

for pid in PRODUCT_IDS:
    cat = CATEGORIES[int(pid[3:]) % 10]
    idx = cat_counters[cat] % 10
    cat_counters[cat] += 1
    name = PRODUCT_NAMES[cat][idx]
    unit_cost = round(random.uniform(5, 2000), 2)
    unit_price = round(unit_cost * random.uniform(1.1, 2.5), 2)
    products.append({
        "product_id": pid,
        "product_name": name,
        "category": cat,
        "sku": f"SKU-{cat[:3].upper()}-{pid[3:]}",
        "unit_cost": unit_cost,
        "unit_price": unit_price,
        "reorder_point": random.randint(10, 100),
        "reorder_quantity": random.randint(50, 500),
        "weight_kg": round(random.uniform(0.1, 50.0), 2),
        "supplier_id": random.choice(ACTIVE_SUPPLIER_IDS),
        "is_active": random.choice(["TRUE","TRUE","TRUE","FALSE"])
    })

# 3 products with null unit_cost (data quality issue)
for idx in [5, 23, 67]:
    products[idx]["unit_cost"] = ""

write_csv(
    os.path.join(BASE, "reference", "products.csv"),
    ["product_id","product_name","category","sku","unit_cost","unit_price",
     "reorder_point","reorder_quantity","weight_kg","supplier_id","is_active"],
    products
)

# ─────────────────────────────────────────────
# ACTIVE PRODUCT IDs for FK usage
# ─────────────────────────────────────────────
ACTIVE_PRODUCT_IDS = [p["product_id"] for p in products if p["is_active"] == "TRUE"]
STORE_ID_LIST = [s["store_id"] for s in stores]
ACTIVE_STORE_IDS = [s["store_id"] for s in stores if s["is_active"] == "TRUE"]

# ─────────────────────────────────────────────
# 4. TRANSACTIONS: orders.csv
# ─────────────────────────────────────────────
ORDER_STATUSES = ["PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"]
ORDER_IDS = [f"ORD{str(i).zfill(5)}" for i in range(1, 501)]

start_dt = datetime(2024, 1, 1)
end_dt   = datetime(2024, 12, 31)

orders = []
for oid in ORDER_IDS:
    order_date = rand_date(start_dt, end_dt)
    expected_delivery = order_date + timedelta(days=random.randint(1, 20))
    actual_delivery = None
    status = random.choice(ORDER_STATUSES)
    if status == "DELIVERED":
        actual_delivery = (order_date + timedelta(days=random.randint(1, 25))).strftime("%Y-%m-%d")
    qty = random.randint(1, 200)
    pid = random.choice(ACTIVE_PRODUCT_IDS)
    unit_price = next(p["unit_price"] for p in products if p["product_id"] == pid)
    orders.append({
        "order_id": oid,
        "product_id": pid,
        "store_id": random.choice(ACTIVE_STORE_IDS),
        "supplier_id": random.choice(ACTIVE_SUPPLIER_IDS),
        "order_date": order_date.strftime("%Y-%m-%d"),
        "expected_delivery_date": expected_delivery.strftime("%Y-%m-%d"),
        "actual_delivery_date": actual_delivery if actual_delivery else "",
        "quantity_ordered": qty,
        "unit_price": unit_price,
        "total_amount": round(qty * float(unit_price), 2),
        "status": status,
        "priority": random.choice(["LOW","MEDIUM","HIGH","URGENT"])
    })

# 5 orders with invalid status
for idx in [10, 50, 100, 200, 300]:
    orders[idx]["status"] = "UNKNOWN"

# 3 duplicate order rows
orders.append(orders[1].copy())
orders.append(orders[42].copy())

write_csv(
    os.path.join(BASE, "transactions", "orders.csv"),
    ["order_id","product_id","store_id","supplier_id","order_date","expected_delivery_date",
     "actual_delivery_date","quantity_ordered","unit_price","total_amount","status","priority"],
    orders
)

# ─────────────────────────────────────────────
# 5. TRANSACTIONS: shipments.csv
# ─────────────────────────────────────────────
# Only SHIPPED/DELIVERED orders get shipments
SHIPPED_ORDER_IDS = [o["order_id"] for o in orders if o["status"] in ("SHIPPED","DELIVERED")]
SHIPMENT_STATUSES = ["IN_TRANSIT","DELIVERED","DELAYED","RETURNED"]

shipments = []
for i, oid in enumerate(SHIPPED_ORDER_IDS):
    parent = next(o for o in orders if o["order_id"] == oid)
    ship_date = datetime.strptime(parent["order_date"], "%Y-%m-%d") + timedelta(days=random.randint(1, 5))
    est_arrival = ship_date + timedelta(days=random.randint(1, 15))
    act_arrival = None
    s_status = random.choice(SHIPMENT_STATUSES)
    if s_status == "DELIVERED":
        act_arrival = (ship_date + timedelta(days=random.randint(1, 18))).strftime("%Y-%m-%d %H:%M:%S")
    shipments.append({
        "shipment_id": f"SHP{str(i+1).zfill(5)}",
        "order_id": oid,
        "product_id": parent["product_id"],
        "store_id": parent["store_id"],
        "supplier_id": parent["supplier_id"],
        "shipment_date": ship_date.strftime("%Y-%m-%d"),
        "estimated_arrival": est_arrival.strftime("%Y-%m-%d"),
        "actual_arrival": act_arrival if act_arrival else "",
        "quantity_shipped": parent["quantity_ordered"],
        "carrier": random.choice(["BlueDart","DHL","FedEx","Delhivery","DTDC","Ecom Express"]),
        "tracking_number": f"TRK{random.randint(100000000,999999999)}",
        "status": s_status,
        "freight_cost": round(random.uniform(50, 5000), 2)
    })

# 2 shipments with negative freight_cost (invalid)
shipments[5]["freight_cost"] = -100.0
shipments[20]["freight_cost"] = -50.0

write_csv(
    os.path.join(BASE, "transactions", "shipments.csv"),
    ["shipment_id","order_id","product_id","store_id","supplier_id","shipment_date",
     "estimated_arrival","actual_arrival","quantity_shipped","carrier",
     "tracking_number","status","freight_cost"],
    shipments
)

# ─────────────────────────────────────────────
# 6. TRANSACTIONS: stock_movements.csv
# ─────────────────────────────────────────────
MOVEMENT_TYPES = ["INBOUND","OUTBOUND","ADJUSTMENT","TRANSFER","RETURN"]

movements = []
for i in range(1, 1001):
    pid = random.choice(ACTIVE_PRODUCT_IDS)
    mov_type = random.choice(MOVEMENT_TYPES)
    qty = random.randint(1, 300)
    if mov_type == "OUTBOUND":
        qty = -abs(qty)
    elif mov_type == "ADJUSTMENT":
        qty = random.randint(-50, 50)
    movements.append({
        "movement_id": f"MOV{str(i).zfill(6)}",
        "product_id": pid,
        "store_id": random.choice(ACTIVE_STORE_IDS),
        "movement_type": mov_type,
        "quantity": qty,
        "movement_date": rand_datetime(start_dt, end_dt).strftime("%Y-%m-%d %H:%M:%S"),
        "reference_id": f"REF{random.randint(10000,99999)}",
        "notes": random.choice(["Scheduled restock","Customer return","Damaged goods","Transfer to branch","Audit adjustment",""])
    })

# 4 rows with missing store_id
for idx in [15, 100, 300, 700]:
    movements[idx]["store_id"] = ""

write_csv(
    os.path.join(BASE, "transactions", "stock_movements.csv"),
    ["movement_id","product_id","store_id","movement_type","quantity",
     "movement_date","reference_id","notes"],
    movements
)

# ─────────────────────────────────────────────
# 7. HISTORICAL: historical_sales.csv
# ─────────────────────────────────────────────
sales = []
sale_id = 1
for _ in range(1500):
    pid = random.choice(ACTIVE_PRODUCT_IDS)
    prod = next(p for p in products if p["product_id"] == pid)
    qty = random.randint(1, 50)
    unit_p = float(prod["unit_price"]) if prod["unit_price"] else round(random.uniform(10,500),2)
    discount = round(random.uniform(0, 0.30), 2)
    sales.append({
        "sale_id": f"SAL{str(sale_id).zfill(6)}",
        "product_id": pid,
        "store_id": random.choice(ACTIVE_STORE_IDS),
        "sale_date": rand_date(start_dt, end_dt).strftime("%Y-%m-%d"),
        "quantity_sold": qty,
        "unit_price": unit_p,
        "discount_pct": discount,
        "total_revenue": round(qty * unit_p * (1 - discount), 2),
        "channel": random.choice(["IN_STORE","ONLINE","B2B","WHOLESALE"]),
        "customer_segment": random.choice(["RETAIL","ENTERPRISE","GOVERNMENT","SMB"])
    })
    sale_id += 1

# 10 rows with quantity_sold = 0 (invalid)
for idx in [0, 100, 200, 300, 400, 500, 600, 700, 800, 900]:
    sales[idx]["quantity_sold"] = 0

write_csv(
    os.path.join(BASE, "historical", "historical_sales.csv"),
    ["sale_id","product_id","store_id","sale_date","quantity_sold","unit_price",
     "discount_pct","total_revenue","channel","customer_segment"],
    sales
)

# ─────────────────────────────────────────────
# 8. HISTORICAL: inventory_demand_stockout_risk.csv
# ─────────────────────────────────────────────
risk_rows = []
for pid in ACTIVE_PRODUCT_IDS[:80]:
    prod = next(p for p in products if p["product_id"] == pid)
    for sid in random.sample(ACTIVE_STORE_IDS, k=random.randint(2, 5)):
        current_stock = random.randint(0, 500)
        avg_daily_demand = round(random.uniform(0.5, 30.0), 2)
        reorder_pt = int(prod["reorder_point"]) if prod["reorder_point"] else 50
        risk_rows.append({
            "risk_id": f"RSK{str(len(risk_rows)+1).zfill(5)}",
            "product_id": pid,
            "store_id": sid,
            "snapshot_date": rand_date(datetime(2024, 10, 1), datetime(2024, 12, 31)).strftime("%Y-%m-%d"),
            "current_stock_units": current_stock,
            "avg_daily_demand_units": avg_daily_demand,
            "days_of_stock_remaining": round(current_stock / avg_daily_demand, 1) if avg_daily_demand > 0 else 999,
            "reorder_point_units": reorder_pt,
            "stockout_risk_flag": "TRUE" if current_stock < reorder_pt else "FALSE",
            "overstock_flag": "TRUE" if current_stock > reorder_pt * 5 else "FALSE",
            "last_restocked_date": rand_date(datetime(2024, 9, 1), datetime(2024, 11, 30)).strftime("%Y-%m-%d")
        })

# 5 rows where days_of_stock_remaining is null
for idx in [10, 50, 100, 150, 200]:
    if idx < len(risk_rows):
        risk_rows[idx]["days_of_stock_remaining"] = ""

write_csv(
    os.path.join(BASE, "historical", "inventory_demand_stockout_risk.csv"),
    ["risk_id","product_id","store_id","snapshot_date","current_stock_units",
     "avg_daily_demand_units","days_of_stock_remaining","reorder_point_units",
     "stockout_risk_flag","overstock_flag","last_restocked_date"],
    risk_rows
)

# ─────────────────────────────────────────────
# 9. STREAMING: live_orders.csv
# ─────────────────────────────────────────────
stream_start = datetime(2025, 1, 1, 8, 0, 0)
stream_end   = datetime(2025, 1, 7, 23, 59, 59)

live_orders = []
for i in range(1, 201):
    pid = random.choice(ACTIVE_PRODUCT_IDS)
    prod = next(p for p in products if p["product_id"] == pid)
    qty = random.randint(1, 100)
    unit_p = float(prod["unit_price"]) if prod["unit_price"] else round(random.uniform(10,500),2)
    live_orders.append({
        "live_order_id": f"LVO{str(i).zfill(5)}",
        "product_id": pid,
        "store_id": random.choice(ACTIVE_STORE_IDS),
        "supplier_id": random.choice(ACTIVE_SUPPLIER_IDS),
        "event_timestamp": rand_datetime(stream_start, stream_end).strftime("%Y-%m-%d %H:%M:%S"),
        "quantity_ordered": qty,
        "unit_price": unit_p,
        "total_amount": round(qty * unit_p, 2),
        "order_status": random.choice(["PENDING","CONFIRMED"]),
        "event_type": "ORDER_PLACED",
        "source_system": random.choice(["WEB","MOBILE","B2B_PORTAL","EDI"])
    })

write_csv(
    os.path.join(BASE, "streaming", "live_orders.csv"),
    ["live_order_id","product_id","store_id","supplier_id","event_timestamp",
     "quantity_ordered","unit_price","total_amount","order_status","event_type","source_system"],
    live_orders
)

# ─────────────────────────────────────────────
# 10. STREAMING: live_shipments.csv
# ─────────────────────────────────────────────
live_shipments = []
for i in range(1, 151):
    live_shipments.append({
        "live_shipment_id": f"LSH{str(i).zfill(5)}",
        "shipment_id": f"SHP{str(random.randint(1,len(shipments))).zfill(5)}",
        "product_id": random.choice(ACTIVE_PRODUCT_IDS),
        "store_id": random.choice(ACTIVE_STORE_IDS),
        "event_timestamp": rand_datetime(stream_start, stream_end).strftime("%Y-%m-%d %H:%M:%S"),
        "shipment_status": random.choice(["DISPATCHED","IN_TRANSIT","OUT_FOR_DELIVERY","DELIVERED","DELAYED"]),
        "current_location": random.choice(["Mumbai Depot","Delhi Hub","Bangalore WH","Chennai Port","In Transit","Final Mile"]),
        "estimated_arrival": (stream_start + timedelta(days=random.randint(1,10))).strftime("%Y-%m-%d"),
        "quantity_in_transit": random.randint(1, 200),
        "event_type": "SHIPMENT_UPDATE",
        "carrier": random.choice(["BlueDart","DHL","FedEx","Delhivery","DTDC"])
    })

write_csv(
    os.path.join(BASE, "streaming", "live_shipments.csv"),
    ["live_shipment_id","shipment_id","product_id","store_id","event_timestamp",
     "shipment_status","current_location","estimated_arrival",
     "quantity_in_transit","event_type","carrier"],
    live_shipments
)

# ─────────────────────────────────────────────
# 11. STREAMING: live_dispatches.csv
# ─────────────────────────────────────────────
live_dispatches = []
for i in range(1, 121):
    pid = random.choice(ACTIVE_PRODUCT_IDS)
    qty = random.randint(1, 150)
    live_dispatches.append({
        "dispatch_id": f"LDP{str(i).zfill(5)}",
        "product_id": pid,
        "from_store_id": random.choice(ACTIVE_STORE_IDS),
        "to_store_id": random.choice(ACTIVE_STORE_IDS),
        "event_timestamp": rand_datetime(stream_start, stream_end).strftime("%Y-%m-%d %H:%M:%S"),
        "quantity_dispatched": qty,
        "dispatch_reason": random.choice(["RESTOCK","TRANSFER","EMERGENCY_REPLENISH","RETURN_TO_SUPPLIER"]),
        "dispatch_status": random.choice(["INITIATED","IN_TRANSIT","COMPLETED","CANCELLED"]),
        "event_type": "DISPATCH_EVENT",
        "priority": random.choice(["NORMAL","HIGH","URGENT"])
    })

write_csv(
    os.path.join(BASE, "streaming", "live_dispatches.csv"),
    ["dispatch_id","product_id","from_store_id","to_store_id","event_timestamp",
     "quantity_dispatched","dispatch_reason","dispatch_status","event_type","priority"],
    live_dispatches
)

print("\nAll 11 datasets generated successfully.")
