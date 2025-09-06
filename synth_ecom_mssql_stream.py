#!/usr/bin/env python3
"""
synth_ecom_mssql_stream.py

SQL Server-ready robust synthetic e-commerce generator with streaming mode.

CONFIGURATION - Modify these values to change data amounts:
- DEFAULT_CUSTOMERS = 1000 (customers to seed)
- DEFAULT_PRODUCTS = 500 (products to seed)  
- DEFAULT_ORDERS = 5000 (orders for one-shot mode)
- DEFAULT_SESSIONS = 3000 (clickstream sessions for one-shot mode)
- DEFAULT_INTERVAL_MINUTES = 15.0 (streaming interval)
- DEFAULT_ORDERS_PER_INTERVAL = 100 (orders every 15 minutes)
- DEFAULT_CLICKS_PER_INTERVAL = 200 (clickstream sessions every 15 minutes)

Usage examples:
  # one-shot with hardcoded amounts (1000 customers, 500 products, 5000 orders, 3000 sessions)
  python synth_ecom_mssql_stream.py

  # streaming mode with hardcoded amounts (100 orders + 200 sessions every 15 minutes)
  python synth_ecom_mssql_stream.py --stream

  # custom webhook URL
  python synth_ecom_mssql_stream.py --stream --webhook-url "https://your-api.com/webhook"

Notes:
- Default SQL Server connection: sa/Gova#ss123@localhost:1433/ecom_db
- fast_executemany is enabled for pyodbc bulk inserts.
- Webhook notifications sent for each data insertion batch.
"""
# =============================================================================
# CONFIGURATION SECTION - Modify these values to change data amounts
# =============================================================================
DEFAULT_CUSTOMERS = 1000          # Number of customers to seed
DEFAULT_PRODUCTS = 500            # Number of products to seed
DEFAULT_ORDERS = 5000             # Number of orders for one-shot mode
DEFAULT_SESSIONS = 3000           # Number of clickstream sessions for one-shot mode
DEFAULT_INTERVAL_MINUTES = 15.0   # Streaming interval in minutes
DEFAULT_ORDERS_PER_INTERVAL = 100 # Orders to create every 15 minutes
DEFAULT_CLICKS_PER_INTERVAL = 200 # Clickstream sessions every 15 minutes
DEFAULT_WEBHOOK_URL = "https://api.example.com/webhook"  # Webhook URL for notifications

# =============================================================================
# IMPORTS
# =============================================================================
import argparse
import logging
import random
import uuid
import urllib.parse
import time
import signal
import threading
import requests
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext, ROUND_HALF_UP
from typing import List, Dict, Any, Optional

from faker import Faker
from dateutil.relativedelta import relativedelta
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, BigInteger, String, DateTime, Numeric, Text,
    ForeignKey, Index, select, insert, update, func, case
)
from sqlalchemy.exc import SQLAlchemyError, DBAPIError

# Decimal settings
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP
MONEY_QUANT = Decimal("0.01")


def money(x) -> Decimal:
    return (Decimal(x)).quantize(MONEY_QUANT)


def chunks(seq: List[Any], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def send_webhook_notification(webhook_url: str, data_type: str, count: int, timestamp: datetime):
    """Send webhook notification about data insertion."""
    if not webhook_url or webhook_url == "https://api.example.com/webhook":
        return  # Skip if using default URL
    
    try:
        payload = {
            "event": "data_inserted",
            "data_type": data_type,
            "count": count,
            "timestamp": timestamp.isoformat(),
            "message": f"Successfully inserted {count} {data_type} records at {timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        }
        
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
        logging.info(f"Webhook notification sent successfully for {data_type}: {count} records")
    except requests.exceptions.RequestException as e:
        logging.warning(f"Failed to send webhook notification: {e}")
    except Exception as e:
        logging.warning(f"Unexpected error sending webhook notification: {e}")


# ---------------------
# Schema definition
# ---------------------
def define_schema(metadata: MetaData):
    customers = Table(
        "customers", metadata,
        Column("customer_id", Integer, primary_key=True, autoincrement=True),
        Column("first_name", String(60), nullable=False),
        Column("last_name", String(60), nullable=False),
        Column("email", String(255), nullable=False, unique=True),
        Column("phone", String(40)),
        Column("country", String(80)),
        Column("state", String(80)),
        Column("city", String(80)),
        Column("postal_code", String(20)),
        Column("street", String(255)),
        Column("signup_date", DateTime, nullable=False),
        Column("last_login", DateTime),
        Column("segment", String(30)),
        Column("lifetime_value", Numeric(14, 2), nullable=False, server_default="0.00"),
    )

    products = Table(
        "products", metadata,
        Column("product_id", Integer, primary_key=True, autoincrement=True),
        Column("sku", String(64), nullable=False, unique=True),
        Column("product_name", String(255), nullable=False),
        Column("category", String(80), nullable=False),
        Column("brand", String(80)),
        Column("description", Text),
        Column("price", Numeric(10, 2), nullable=False),
        Column("rrp", Numeric(10, 2)),
        Column("created_at", DateTime, nullable=False),
        Column("popularity", Integer, nullable=False, default=1),
    )

    inventory = Table(
        "inventory", metadata,
        Column("inventory_id", Integer, primary_key=True, autoincrement=True),
        Column("product_id", Integer, ForeignKey("products.product_id"), nullable=False, unique=True, index=True),
        Column("stock_level", Integer, nullable=False),
        Column("reorder_threshold", Integer, nullable=False),
        Column("last_restocked", DateTime),
    )

    orders = Table(
        "orders", metadata,
        Column("order_id", Integer, primary_key=True, autoincrement=True),
        Column("order_uuid", String(36), nullable=False, unique=True),
        Column("customer_id", Integer, ForeignKey("customers.customer_id"), nullable=False, index=True),
        Column("order_date", DateTime, nullable=False, index=True),
        Column("status", String(30)),
        Column("payment_method", String(40)),
        Column("subtotal", Numeric(12, 2), nullable=False),
        Column("shipping", Numeric(10, 2), nullable=False),
        Column("tax", Numeric(10, 2), nullable=False),
        Column("discount", Numeric(10, 2), nullable=False),
        Column("total_amount", Numeric(12, 2), nullable=False, index=True),
        Column("placed_via", String(64)),
    )

    order_items = Table(
        "order_items", metadata,
        Column("order_item_id", Integer, primary_key=True, autoincrement=True),
        Column("order_id", Integer, ForeignKey("orders.order_id"), nullable=False, index=True),
        Column("product_id", Integer, ForeignKey("products.product_id"), nullable=False),
        Column("sku", String(64)),
        Column("quantity", Integer, nullable=False),
        Column("unit_price", Numeric(10, 2), nullable=False),
        Column("line_total", Numeric(12, 2), nullable=False),
    )

    clickstream = Table(
        "clickstream", metadata,
        Column("click_id", BigInteger, primary_key=True, autoincrement=True),
        Column("customer_id", Integer, ForeignKey("customers.customer_id"), nullable=True, index=True),
        Column("session_id", String(64), index=True),
        Column("page", String(80)),
        Column("page_url", String(255)),
        Column("product_id", Integer, ForeignKey("products.product_id"), nullable=True),
        Column("timestamp", DateTime, index=True),
        Column("device", String(40)),
        Column("user_agent", String(255)),
        Column("referrer", String(255)),
        Column("utm_source", String(80)),
        Column("ip_address", String(45)),
    )

    Index("ix_orders_customer_date", orders.c.customer_id, orders.c.order_date)
    Index("ix_click_session_time", clickstream.c.session_id, clickstream.c.timestamp)

    return {
        "__meta__": metadata,
        "customers": customers,
        "products": products,
        "inventory": inventory,
        "orders": orders,
        "order_items": order_items,
        "clickstream": clickstream,
    }


# ------------------------------
# Core generator
# ------------------------------
class RobustSynth:
    def __init__(self, engine, tables: Dict[str, Table], cfg: Dict[str, Any], webhook_url: str = None):
        self.engine = engine
        self.tables = tables
        self.cfg = cfg
        self.webhook_url = webhook_url
        self.fake = Faker()
        if cfg.get("seed") is not None:
            Faker.seed(cfg["seed"])
            random.seed(cfg["seed"])
            self.fake.seed_instance(cfg["seed"])

        self.categories = {
            "Electronics": (50, 2500, 199),
            "Clothing": (5, 350, 39),
            "Books": (3, 80, 15),
            "Home": (10, 800, 79),
            "Toys": (5, 220, 29),
            "Beauty": (3, 180, 29),
            "Sports": (8, 600, 69),
        }
        self.segments = {
            "loyal": {"weight": 0.12, "recent_alpha": 2.0},
            "frequent": {"weight": 0.10, "recent_alpha": 1.6},
            "casual": {"weight": 0.46, "recent_alpha": 1.0},
            "bargain": {"weight": 0.18, "recent_alpha": 1.0},
            "window_shopper": {"weight": 0.14, "recent_alpha": 0.8},
        }
        self.payment_methods = ["Credit Card", "PayPal", "UPI", "Apple Pay", "Google Pay", "COD"]
        self.placed_via = ["web", "mobile_app", "affiliate", "api"]
        self.devices = ["Mobile", "Desktop", "Tablet"]

    def _format_phone(self, country: str) -> str:
        return self.fake.phone_number()

    def seed_customers(self, n_customers: int, chunk_size: int = 500):
        tbl = self.tables["customers"]
        with self.engine.begin() as conn:
            existing = conn.execute(select(func.count()).select_from(tbl)).scalar()
            to_create = max(0, n_customers - int(existing))
            if to_create <= 0:
                logging.info("Customers: %d already present; skipping seeding.", existing)
                return

            logging.info("Seeding %d customers (existing %d).", to_create, existing)
            created = []
            self.fake.unique.clear()
            now = datetime.now(timezone.utc)
            three_years = now - relativedelta(years=3)
            attempts = 0
            while len(created) < to_create and attempts < to_create * 6:
                first = self.fake.first_name()
                last = self.fake.last_name()
                email = self.fake.unique.email()
                country = self.fake.country()
                street = self.fake.street_address()
                city = self.fake.city()
                state = self.fake.state()
                postal = self.fake.postcode()
                signup_dt = self.fake.date_time_between(start_date=three_years, end_date=now).replace(tzinfo=timezone.utc)
                last_login_dt = self.fake.date_time_between(start_date=signup_dt, end_date=now).replace(tzinfo=timezone.utc)
                seg = random.choices(list(self.segments.keys()),
                                     weights=[v["weight"] for v in self.segments.values()], k=1)[0]
                phone = self._format_phone(country)
                created.append({
                    "first_name": first,
                    "last_name": last,
                    "email": email,
                    "phone": phone,
                    "country": country,
                    "state": state,
                    "city": city,
                    "postal_code": postal,
                    "street": street,
                    "signup_date": signup_dt,
                    "last_login": last_login_dt,
                    "segment": seg,
                    "lifetime_value": Decimal("0.00"),
                })
                attempts += 1

            candidate_emails = [r["email"] for r in created]
            if not candidate_emails:
                logging.info("No candidate customers to insert.")
                return

            existing_rows = conn.execute(
                select(tbl.c.email).where(tbl.c.email.in_(candidate_emails))
            ).fetchall()
            existing_emails = {r[0] for r in existing_rows}
            to_insert = [r for r in created if r["email"] not in existing_emails]
            inserted = 0
            for chunk in chunks(to_insert, chunk_size):
                conn.execute(insert(tbl), chunk)
                inserted += len(chunk)
            logging.info("Inserted %d new customers (skipped %d duplicate emails).",
                         inserted, len(created) - len(to_insert))
            
            # Send webhook notification
            if inserted > 0:
                send_webhook_notification(self.webhook_url, "customers", inserted, datetime.now(timezone.utc))

    def seed_products_and_inventory(self, n_products: int, chunk_size: int = 500):
        p_tbl = self.tables["products"]
        inv_tbl = self.tables["inventory"]
        with self.engine.begin() as conn:
            existing = conn.execute(select(func.count()).select_from(p_tbl)).scalar()
            to_create = max(0, n_products - int(existing))
            if to_create <= 0:
                logging.info("Products: %d already present; skipping seeding.", existing)
                return

            logging.info("Seeding %d products (existing %d).", to_create, existing)
            created_products = []
            used_skus = set()
            adjective = ["Ultra", "Pro", "Max", "Mini", "Lite", "Smart", "Eco", "Deluxe", "Prime", "Neo"]
            materials = ["Carbon", "Steel", "Leather", "Silk", "Cotton", "Bamboo", "Alloy", "Glass"]
            self.fake.unique.clear()
            now = datetime.now(timezone.utc)
            one_year = now - relativedelta(years=1)

            attempts = 0
            while len(created_products) < to_create and attempts < to_create * 6:
                category = random.choice(list(self.categories.keys()))
                minp, maxp, mode = self.categories[category]
                price = float(random.triangular(minp, maxp, mode))
                price = money(price)
                rrp = money(price * Decimal(random.uniform(1.05, 1.5)))
                name = f"{random.choice(adjective)} {random.choice(materials)} {self.fake.word().capitalize()}"
                sku = f"{category[:3].upper()}-{self.fake.bothify(text='??-#####').upper()}"
                if sku in used_skus:
                    sku = f"{sku}-{str(uuid.uuid4())[:4]}"
                used_skus.add(sku)
                description = self.fake.paragraph(nb_sentences=2)
                created_at = self.fake.date_time_between(start_date=one_year, end_date=now).replace(tzinfo=timezone.utc)
                popularity = max(1, int(random.expovariate(1 / 20) * 100))
                created_products.append({
                    "sku": sku,
                    "product_name": name,
                    "category": category,
                    "brand": self.fake.company(),
                    "description": description,
                    "price": price,
                    "rrp": rrp,
                    "created_at": created_at,
                    "popularity": popularity,
                })
                attempts += 1

            candidate_skus = [r["sku"] for r in created_products]
            existing_skus_rows = conn.execute(select(p_tbl.c.sku).where(p_tbl.c.sku.in_(candidate_skus))).fetchall()
            existing_skus = {r[0] for r in existing_skus_rows}
            to_insert = [r for r in created_products if r["sku"] not in existing_skus]

            for chunk in chunks(to_insert, chunk_size):
                conn.execute(insert(p_tbl), chunk)
            logging.info("Inserted %d new products (skipped %d duplicate skus).", len(to_insert), len(created_products) - len(to_insert))
            
            # Send webhook notification
            if len(to_insert) > 0:
                send_webhook_notification(self.webhook_url, "products", len(to_insert), datetime.now(timezone.utc))

            # initialize inventory for products that lack inventory rows
            product_ids = [r[0] for r in conn.execute(
                select(p_tbl.c.product_id).where(~p_tbl.c.product_id.in_(select(inv_tbl.c.product_id)))
            ).fetchall()]
            inv_rows = []
            for pid in product_ids:
                inv_rows.append({
                    "product_id": pid,
                    "stock_level": random.randint(20, 800),
                    "reorder_threshold": random.randint(5, 50),
                    "last_restocked": datetime.now(timezone.utc) - timedelta(days=random.randint(1, 180))
                })
            if inv_rows:
                for chunk in chunks(inv_rows, chunk_size):
                    conn.execute(insert(inv_tbl), chunk)
            logging.info("Initialized inventory for %d products.", len(inv_rows))

    def _load_customers_products(self):
        with self.engine.begin() as conn:
            customers = [dict(r) for r in conn.execute(select(self.tables["customers"])).fetchall()]
            products = [dict(r) for r in conn.execute(select(self.tables["products"])).fetchall()]
        if not customers:
            raise RuntimeError("No customers present -- seed customers first.")
        if not products:
            raise RuntimeError("No products present -- seed products first.")
        return customers, products

    def generate_orders(self, n_orders: int, batch_size: int = 200, force_order_date_now: bool = False):
        """
        Generate n_orders and flush in batches.
        If force_order_date_now is True, each generated order_date will be now (good for streaming).
        """
        orders_tbl = self.tables["orders"]
        items_tbl = self.tables["order_items"]
        inv_tbl = self.tables["inventory"]
        cust_tbl = self.tables["customers"]
        now = datetime.now(timezone.utc)

        customers, products = self._load_customers_products()

        prod_weights = [p.get("popularity", 10) for p in products]
        cust_weights = []
        for c in customers:
            seg = c.get("segment", "casual")
            base = self.segments.get(seg, {"weight": 0.1})["weight"]
            signup = c.get("signup_date") or (now - timedelta(days=365))
            # Ensure both datetimes are timezone-aware
            if signup.tzinfo is None:
                signup = signup.replace(tzinfo=timezone.utc)
            if now.tzinfo is None:
                now = now.replace(tzinfo=timezone.utc)
            days_active = max(1, (now - signup).days)
            cust_weights.append(base * (1 + days_active / 365.0))

        logging.info("Generating %d orders (batch_size=%d, force_order_date_now=%s)...", n_orders, batch_size, force_order_date_now)
        generated = 0
        orders_buffer = []
        items_buffer = []
        ltv_accumulator = {}

        for i in range(n_orders):
            c = random.choices(customers, weights=cust_weights, k=1)[0]
            c_id = c["customer_id"]
            if force_order_date_now:
                order_date = datetime.now(timezone.utc)
            else:
                order_date = self.fake.date_time_between(start_date=c["signup_date"], end_date=now).replace(tzinfo=timezone.utc)

            order_uuid = str(uuid.uuid4())
            placed_via = random.choices(self.placed_via, weights=[0.7, 0.2, 0.05, 0.05])[0]
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.06, 0.04])[0]
            chosen_products = random.choices(products, weights=prod_weights, k=num_items)

            subtotal = Decimal("0.00")
            per_order_items = []
            for prod in chosen_products:
                pid = prod["product_id"]
                unit_price = money(Decimal(prod["price"]) * Decimal(random.choice([1.0, 0.98, 0.95, 0.9])))
                qty = random.choices([1, 1, 1, 2, 2, 3], weights=[0.4, 0.2, 0.2, 0.1, 0.06, 0.04])[0]
                line_total = money(unit_price * qty)
                subtotal += line_total
                per_order_items.append({
                    "order_uuid": order_uuid,
                    "product_id": pid,
                    "sku": prod["sku"],
                    "quantity": int(qty),
                    "unit_price": unit_price,
                    "line_total": line_total
                })

            shipping = money(0 if subtotal >= Decimal("75.00") else min(12.99, subtotal * Decimal("0.08")))
            coupon = None
            discount = Decimal("0.00")
            if random.random() < 0.07:
                coupon = random.choice(["WELCOME10", "FREESHIP", "VIP50", "SAVE15"])
                if coupon == "WELCOME10":
                    discount = money(subtotal * Decimal("0.10"))
                elif coupon == "FREESHIP":
                    shipping = Decimal("0.00")
                elif coupon == "VIP50":
                    discount = min(money(subtotal * Decimal("0.5")), Decimal("50.00"))
                else:
                    discount = money(subtotal * Decimal("0.15"))

            tax = money(subtotal * Decimal("0.07"))
            total_amount = money(subtotal + shipping + tax - discount)

            days_old = (now - order_date).days
            if days_old < 1:
                status = "Pending"
            elif days_old < 3:
                status = random.choices(["Processing", "Shipped"], weights=[0.4, 0.6])[0]
            elif days_old < 20:
                status = random.choices(["Shipped", "Delivered"], weights=[0.2, 0.8])[0]
            else:
                status = random.choices(["Delivered", "Returned", "Cancelled"], weights=[0.9, 0.05, 0.05])[0]

            orders_buffer.append({
                "order_uuid": order_uuid,
                "customer_id": c_id,
                "order_date": order_date,
                "status": status,
                "payment_method": random.choice(self.payment_methods),
                "subtotal": subtotal,
                "shipping": shipping,
                "tax": tax,
                "discount": discount,
                "total_amount": total_amount,
                "placed_via": placed_via
            })
            items_buffer.extend(per_order_items)
            ltv_accumulator[c_id] = ltv_accumulator.get(c_id, Decimal("0.00")) + total_amount
            generated += 1

            if len(orders_buffer) >= batch_size or (i == n_orders - 1 and orders_buffer):
                final_items = []
                try:
                    with self.engine.begin() as conn:
                        conn.execute(insert(orders_tbl), orders_buffer)

                        uuids = [o["order_uuid"] for o in orders_buffer]
                        rows = conn.execute(select(orders_tbl.c.order_id, orders_tbl.c.order_uuid).where(orders_tbl.c.order_uuid.in_(uuids))).fetchall()
                        uuid_to_order_id = {r['order_uuid']: r['order_id'] for r in rows}

                        for it in [it for it in items_buffer if it["order_uuid"] in uuid_to_order_id]:
                            final_items.append({
                                "order_id": uuid_to_order_id[it["order_uuid"]],
                                "product_id": it["product_id"],
                                "sku": it["sku"],
                                "quantity": int(it["quantity"]),
                                "unit_price": it["unit_price"],
                                "line_total": it["line_total"]
                            })
                        if final_items:
                            conn.execute(insert(items_tbl), final_items)

                        # deduct inventory using CASE to avoid negative stock
                        sold = {}
                        for it in final_items:
                            sold[it["product_id"]] = sold.get(it["product_id"], 0) + it["quantity"]

                        if sold:
                            for pid, qty_sold in sold.items():
                                stmt = update(inv_tbl).where(inv_tbl.c.product_id == pid).values(
                                    stock_level=case(
                                        [
                                            (inv_tbl.c.stock_level >= qty_sold, inv_tbl.c.stock_level - qty_sold)
                                        ],
                                        else_=0
                                    ),
                                    last_restocked=datetime.now(timezone.utc)
                                )
                                res = conn.execute(stmt)
                                if res.rowcount == 0:
                                    conn.execute(insert(inv_tbl).values(
                                        product_id=pid,
                                        stock_level=0,
                                        reorder_threshold=5,
                                        last_restocked=datetime.now(timezone.utc)
                                    ))

                        # bump lifetime_value atomically
                        for cid, delta in ltv_accumulator.items():
                            conn.execute(update(cust_tbl).where(cust_tbl.c.customer_id == cid)
                                         .values(lifetime_value=cust_tbl.c.lifetime_value + delta))
                    logging.info("Flushed batch: orders=%d items=%d", len(orders_buffer), len(final_items))
                    
                    # Send webhook notification for orders
                    if len(orders_buffer) > 0:
                        send_webhook_notification(self.webhook_url, "orders", len(orders_buffer), datetime.now(timezone.utc))
                except (SQLAlchemyError, DBAPIError) as e:
                    logging.exception("Database error while flushing batch: %s. Transaction rolled back.", e)
                finally:
                    orders_buffer.clear()
                    items_buffer.clear()
                    ltv_accumulator.clear()

        logging.info("Generated %d orders", generated)

    def generate_clickstream(self, n_sessions: int = 1000, avg_events_per_session: int = 8, chunk_size: int = 2000):
        clicks_tbl = self.tables["clickstream"]
        orders_tbl = self.tables["orders"]
        products_tbl = self.tables["products"]
        cust_tbl = self.tables["customers"]

        now = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            prods = [r[0] for r in conn.execute(select(products_tbl.c.product_id)).fetchall()]
            custs = [r[0] for r in conn.execute(select(cust_tbl.c.customer_id)).fetchall()]
            recent_orders = [dict(r) for r in conn.execute(select(orders_tbl.c.order_id, orders_tbl.c.order_date, orders_tbl.c.customer_id).order_by(orders_tbl.c.order_date.desc()).limit(n_sessions)).fetchall()]

        prods_ids = prods or [1]
        cust_ids = custs or []

        events = []
        # sessions from recent orders (funnels)
        for o in recent_orders:
            session_id = str(uuid.uuid4())
            order_date = o["order_date"] if o["order_date"] is not None else now
            start = order_date - timedelta(minutes=random.randint(1, 240))
            ts = start
            pages = ["Homepage", "Category", "Search"]
            pages += ["Product"] * random.randint(1, 4)
            pages += ["Cart", "Checkout", "OrderConfirmation"]
            for p in pages:
                ts += timedelta(seconds=random.randint(5, 90))
                product_id = None
                page_url = f"https://www.example.com/{p.lower()}"
                if p == "Product":
                    product_id = random.choice(prods_ids)
                    page_url = f"https://www.example.com/product/{product_id}"
                events.append({
                    "customer_id": o["customer_id"],
                    "session_id": session_id,
                    "page": p,
                    "page_url": page_url,
                    "product_id": product_id,
                    "timestamp": ts,
                    "device": random.choice(self.devices),
                    "user_agent": self.fake.user_agent(),
                    "referrer": random.choice(["https://google.com", "https://facebook.com", ""]),
                    "utm_source": random.choice(["google", "facebook", "newsletter", "direct"]),
                    "ip_address": self.fake.ipv4_public(),
                })

        # random browsing sessions
        for _ in range(n_sessions):
            session_id = str(uuid.uuid4())
            cust_id = random.choice(cust_ids) if cust_ids and random.random() >= 0.1 else None
            session_start = self.fake.date_time_between(start_date=now - timedelta(days=90), end_date=now).replace(tzinfo=timezone.utc)
            ts = session_start
            seq_len = max(3, int(random.gauss(avg_events_per_session, 2)))
            for _ in range(seq_len):
                ts += timedelta(seconds=random.randint(5, 300))
                page = random.choices(["Homepage", "Category", "Search", "Product", "Cart"], weights=[0.2,0.25,0.15,0.3,0.1])[0]
                product_id = None
                page_url = f"https://www.example.com/{page.lower()}"
                if page == "Product" and random.random() < 0.6:
                    pid = random.choice(prods_ids)
                    product_id = pid
                    page_url = f"https://www.example.com/product/{pid}"
                events.append({
                    "customer_id": cust_id,
                    "session_id": session_id,
                    "page": page,
                    "page_url": page_url,
                    "product_id": product_id,
                    "timestamp": ts,
                    "device": random.choice(self.devices),
                    "user_agent": self.fake.user_agent(),
                    "referrer": random.choice(["https://google.com", "https://bing.com", ""]),
                    "utm_source": random.choice(["google", "facebook", "newsletter", "direct"]),
                    "ip_address": self.fake.ipv4_public(),
                })

        inserted = 0
        with self.engine.begin() as conn:
            for chunk in chunks(events, chunk_size):
                conn.execute(insert(clicks_tbl), chunk)
                inserted += len(chunk)
        logging.info("Inserted %d clickstream events.", inserted)
        
        # Send webhook notification for clickstream
        if inserted > 0:
            send_webhook_notification(self.webhook_url, "clickstream", inserted, datetime.now(timezone.utc))

    def validate_data(self) -> Dict[str, Any]:
        report = {}
        orders_tbl = self.tables["orders"]
        items_tbl = self.tables["order_items"]
        products_tbl = self.tables["products"]
        cust_tbl = self.tables["customers"]
        inv_tbl = self.tables["inventory"]

        with self.engine.begin() as conn:
            orphan_orders = conn.execute(
                select(func.count()).select_from(
                    orders_tbl.join(cust_tbl, orders_tbl.c.customer_id == cust_tbl.c.customer_id, isouter=True)
                ).where(cust_tbl.c.customer_id == None)
            ).scalar()
            report["orphan_orders"] = int(orphan_orders)

            orphan_items = conn.execute(
                select(func.count()).select_from(
                    items_tbl.join(products_tbl, items_tbl.c.product_id == products_tbl.c.product_id, isouter=True)
                ).where(products_tbl.c.product_id == None)
            ).scalar()
            report["orphan_order_items"] = int(orphan_items)

            inv_orphans = conn.execute(
                select(func.count()).select_from(
                    inv_tbl.join(products_tbl, inv_tbl.c.product_id == products_tbl.c.product_id, isouter=True)
                ).where(products_tbl.c.product_id == None)
            ).scalar()
            report["inventory_orphans"] = int(inv_orphans)

            item_sums = conn.execute(
                select(items_tbl.c.order_id, func.coalesce(func.sum(items_tbl.c.line_total), 0).label("items_sum")).group_by(items_tbl.c.order_id)
            ).fetchall()
            items_map = {r["order_id"]: Decimal(r["items_sum"]) for r in item_sums}

            mismatches = []
            ord_rows = conn.execute(select(orders_tbl.c.order_id, orders_tbl.c.subtotal, orders_tbl.c.shipping,
                                           orders_tbl.c.tax, orders_tbl.c.discount, orders_tbl.c.total_amount)).fetchall()
            for o in ord_rows:
                oid = o["order_id"]
                subtotal_db = Decimal(o["subtotal"] or 0)
                items_sum = items_map.get(oid, Decimal("0.00"))
                if money(items_sum) != money(subtotal_db):
                    mismatches.append({"order_id": oid, "subtotal_db": str(subtotal_db), "items_sum": str(items_sum)})
                expected_total = money(subtotal_db + Decimal(o["shipping"] or 0) + Decimal(o["tax"] or 0) - Decimal(o["discount"] or 0))
                if money(Decimal(o["total_amount"])) != expected_total:
                    mismatches.append({"order_id": oid, "total_db": str(o["total_amount"]), "expected_total": str(expected_total)})

            report["order_amount_mismatches"] = mismatches
        logging.info("Validation done: orphans orders=%d items=%d inventory=%d mismatches=%d",
                     report["orphan_orders"], report["orphan_order_items"], report["inventory_orphans"], len(mismatches))
        return report


# ---------------------
# CLI and main
# ---------------------
def parse_args():
    p = argparse.ArgumentParser(description="Robust synthetic e-commerce data generator (SQL Server) with streaming")
    
    # Try different default connection options
    import platform
    is_windows = platform.system().lower() == 'windows'
    
    if is_windows:
        # Windows: Try Windows Authentication first
        default_options = [
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;Trusted_Connection=yes;TrustServerCertificate=Yes",
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=;TrustServerCertificate=Yes",
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=sa;TrustServerCertificate=Yes",
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=password;TrustServerCertificate=Yes"
        ]
        default_db_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(default_options[0])}"
    else:
        # Linux/Unix: Try SQL Server authentication first
        default_options = [
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=Gova#ss123;TrustServerCertificate=Yes",
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=;TrustServerCertificate=Yes",
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=sa;TrustServerCertificate=Yes",
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=password;TrustServerCertificate=Yes"
        ]
        default_db_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(default_options[0])}"
    
    p.add_argument("--db-url", required=False, default=default_db_url, help="Full SQLAlchemy DB URL. If not provided, tries Windows Authentication. Example: mssql+pyodbc://sa:password@localhost:1433/ecom_db?driver=ODBC+Driver+17+for+SQL+Server")

    p.add_argument("--customers", type=int, default=DEFAULT_CUSTOMERS, help=f"Number of customers to seed (default: {DEFAULT_CUSTOMERS})")
    p.add_argument("--products", type=int, default=DEFAULT_PRODUCTS, help=f"Number of products to seed (default: {DEFAULT_PRODUCTS})")
    p.add_argument("--orders", type=int, default=DEFAULT_ORDERS, help=f"Number of orders to generate (one-shot mode) (default: {DEFAULT_ORDERS})")
    p.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS, help=f"Number of clickstream sessions to generate (one-shot mode) (default: {DEFAULT_SESSIONS})")
    p.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducibility")
    p.add_argument("--batch-size", type=int, default=200, help="Batch size for order insertion")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")

    # streaming options
    p.add_argument("--stream", action="store_true", help="Run continuously in streaming mode")
    p.add_argument("--interval-minutes", type=float, default=DEFAULT_INTERVAL_MINUTES, help=f"Interval between streaming batches (minutes). Default: {DEFAULT_INTERVAL_MINUTES} minutes")
    p.add_argument("--orders-per-interval", type=int, default=DEFAULT_ORDERS_PER_INTERVAL, help=f"Orders to create each interval when streaming. Default: {DEFAULT_ORDERS_PER_INTERVAL} orders per {DEFAULT_INTERVAL_MINUTES} minutes")
    p.add_argument("--clicks-per-interval", type=int, default=DEFAULT_CLICKS_PER_INTERVAL, help=f"Clickstream sessions to create each interval when streaming. Default: {DEFAULT_CLICKS_PER_INTERVAL} sessions per {DEFAULT_INTERVAL_MINUTES} minutes")
    p.add_argument("--max-iterations", type=int, default=0, help="If >0, stop after this many streaming iterations (for testing)")
    p.add_argument("--use-now-timestamp", action="store_true", help="When streaming, set order_date to current timestamp (default: True recommended)")
    
    # URL parameter for webhook or API endpoint
    p.add_argument("--webhook-url", type=str, default=DEFAULT_WEBHOOK_URL, help=f"Webhook URL to send data notifications (default: {DEFAULT_WEBHOOK_URL})")

    return p.parse_args()


def try_connection_options(db_urls):
    """Try multiple connection options and return the first working one."""
    for i, url in enumerate(db_urls):
        try:
            logging.info(f"Trying connection option {i+1}...")
            engine = create_engine(url, pool_pre_ping=True, connect_args={"fast_executemany": True})
            # Test the connection
            with engine.connect() as conn:
                conn.execute(select(1))
            logging.info(f"Successfully connected with option {i+1}")
            return engine
        except Exception as e:
            logging.warning(f"Connection option {i+1} failed: {str(e)[:100]}...")
            continue
    return None

def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    metadata = MetaData()
    tables = define_schema(metadata)

    # If using default connection, try multiple options
    if args.db_url and ("Trusted_Connection=yes" in args.db_url or "UID=sa" in args.db_url):
        logging.info("Using default connection - trying multiple authentication methods...")
        
        # Generate multiple connection URLs to try based on platform
        import platform
        is_windows = platform.system().lower() == 'windows'
        
        if is_windows:
            connection_options = [
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;Trusted_Connection=yes;TrustServerCertificate=Yes')}",
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=;TrustServerCertificate=Yes')}",
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=sa;TrustServerCertificate=Yes')}",
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=password;TrustServerCertificate=Yes')}"
            ]
        else:
            connection_options = [
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=Gova#ss123;TrustServerCertificate=Yes')}",
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=;TrustServerCertificate=Yes')}",
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=sa;TrustServerCertificate=Yes')}",
                f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ecom_db;UID=sa;PWD=password;TrustServerCertificate=Yes')}"
            ]
        
        engine = try_connection_options(connection_options)
        if engine is None:
            logging.error("""
Failed to connect to SQL Server with any default option. Please try one of these solutions:

1. Provide a custom connection string:
   python synth_ecom_mssql_stream.py --db-url "mssql+pyodbc://sa:YOUR_PASSWORD@localhost:1433/ecom_db?driver=ODBC+Driver+17+for+SQL+Server"

2. Make sure SQL Server is running and accessible

3. Check your SQL Server authentication settings

4. Try these common connection strings:
   - Windows Auth: --db-url "mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+17+for+SQL+Server%7D%3BSERVER%3Dlocalhost%3BDATABASE%3Decom_db%3BTrusted_Connection%3Dyes%3BTrustServerCertificate%3DYes"
   - SQL Auth: --db-url "mssql+pyodbc://sa:YOUR_PASSWORD@localhost:1433/ecom_db?driver=ODBC+Driver+17+for+SQL+Server"
            """)
            return
    else:
        # Use the provided connection string
        engine = create_engine(args.db_url, pool_pre_ping=True, connect_args={"fast_executemany": True})
    
    metadata.create_all(engine)
    logging.info("Tables ensured/created (if needed).")

    synth = RobustSynth(engine, tables, {"seed": args.seed}, args.webhook_url)

    # seed once (idempotent)
    synth.seed_customers(args.customers)
    synth.seed_products_and_inventory(args.products)

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        logging.info("Signal %s received: shutting down gracefully...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    if args.stream:
        logging.info("Starting streaming mode: interval=%.1f minutes, orders_per_interval=%d, clicks_per_interval=%d, webhook_url=%s",
                     args.interval_minutes, args.orders_per_interval, args.clicks_per_interval, args.webhook_url)
        interval_sec = max(1.0, args.interval_minutes * 60.0)
        iteration = 0
        # schedule with monotonic clock to avoid drift
        next_run = time.monotonic()
        try:
            while not stop_event.is_set():
                iteration += 1
                start_monotonic = time.monotonic()
                logging.info("Stream iteration %d starting...", iteration)

                # generate orders this interval
                if args.orders_per_interval > 0:
                    synth.generate_orders(args.orders_per_interval, batch_size=min(args.batch_size, args.orders_per_interval),
                                          force_order_date_now=args.use_now_timestamp)

                # optional clickstream per interval
                if args.clicks_per_interval > 0:
                    synth.generate_clickstream(n_sessions=args.clicks_per_interval)

                # validation optionally every N iterations could be added; we skip expensive validation every time
                logging.info("Stream iteration %d finished (took %.1fs).", iteration, time.monotonic() - start_monotonic)

                # check max iterations
                if args.max_iterations and iteration >= args.max_iterations:
                    logging.info("Reached max iterations (%d). Stopping.", args.max_iterations)
                    break

                # schedule next run precisely
                next_run += interval_sec
                sleep_time = next_run - time.monotonic()
                if sleep_time > 0:
                    logging.info("Sleeping %.1fs until next iteration...", sleep_time)
                    # wait but wake up early if stop_event set
                    stop_event.wait(timeout=sleep_time)
                else:
                    logging.warning("Generation took longer (%.1fs) than interval (%.1f s); continuing immediately",
                                    time.monotonic() - start_monotonic, interval_sec)
        except Exception:
            logging.exception("Unhandled exception in streaming loop")
        finally:
            logging.info("Streaming stopped; running final validation...")
            report = synth.validate_data()
            logging.info("Final integrity report: %s", report)
    else:
        # one-shot run
        logging.info("Running one-shot generation: orders=%d, sessions=%d", args.orders, args.sessions)
        synth.generate_orders(args.orders, batch_size=args.batch_size)
        if args.sessions > 0:
            synth.generate_clickstream(n_sessions=args.sessions)
        report = synth.validate_data()
        logging.info("Integrity report: %s", report)


if __name__ == "__main__":
    main()