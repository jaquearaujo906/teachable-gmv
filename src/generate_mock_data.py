# -----------------------------
#  EN
# -----------------------------
# Generate synthetic data that mimics CDC (Change Data Capture) for 3 entities:
# purchase: purchase lifecycle events (created, paid, reshipped/corrected)
# product_item: items and financial values ​​associated with the purchase
# Purchase_extra_info: dimensional attributes, such as subsidiary
# The script saves this data as CSV in the bronze layer, simulating a data lake (S3 in production).
# -----------------------------
#  BR
# -----------------------------
# Gera dados sintéticos que simulam o CDC (Change Data Capture) para 3 entidades:
# compra: eventos do ciclo de vida da compra (criada, paga, reenviada/corrigida)
# item_produto: itens e valores financeiros associados à compra
# informações_extras_da_compra: atributos dimensionais, como subsidiária
# O script salva esses dados como CSV na camada bronze, simulando um data lake (S3 em produção).

import os
import pandas as pd
from datetime import datetime, timedelta
import random
import uuid

random.seed(42)

BASE_DIR = "data_lake/bronze"

def ensure_directory(path: str):
    os.makedirs(path, exist_ok=True)


def to_date_str(dt:datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def to_dt_str(dt:datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def main():
    ensure_directory(BASE_DIR)

    # Pastas (bronze)
    purchase_dir = os.path.join(BASE_DIR, "purchase")
    product_item_dir = os.path.join(BASE_DIR, "product_item")
    extra_dir = os.path.join(BASE_DIR, "product_extra_info")


    ensure_directory(purchase_dir)
    ensure_directory(product_item_dir)
    ensure_directory(extra_dir)

    # Create purchases and events CDC
    purchase_ids = [55, 56, 78, 29, 70, 202]
    subsidiaries = ["nacional", "internacional"]

    purchase_events = []
    product_item_events = []
    extra_events = []


    now = datetime(2023, 3, 15, 10, 0, 0)


    for pid in purchase_ids:
        buyer_id = random.randint(1000, 2000)
        producer_id = random.randint(500, 1000)


        # "prod_item_id" 
        prod_item_id = random.choice([random.randint(1, 3), random.randint(10000, 60000)])

        # order_date = data do pedido
        order_dt = now - timedelta(days=random.choice([40, 50, 60]))
        order_date = to_date_str(order_dt)

        # event 1: create
        t1 = now - timedelta(days=random.choice([40, 35, 30]), hours=random.randint(0, 10))
        purchase_events.append({
            "transaction_datetime": to_dt_str(t1),
            "transaction_date": to_date_str(t1),
            "purchase_id": pid,
            "buyer_id": buyer_id,
            "prod_item_id": prod_item_id,
            "order_date": order_date,
            "release_date": None,
            "producer_id": producer_id
        })

        # product_item event 1
        product_id = random.randint(10000, 99999)
        item_quantity = random.choice([1, 2, 10, 10, 5])
        purchase_value = round(random.choice([50, 55, 2000, 150, 300]) + random.random(), 2)


        product_item_events.append({
            "transaction_datetime": to_dt_str(t1),
            "transaction_date": to_date_str(t1),
            "purchase_id": pid,
            "product_id": product_id,
            "item_quantity": item_quantity,
            "purchase_value": purchase_value
        })


        # extra info event 1
        t_extra1 = t1 + timedelta(hours=1)
        extra_events.append({
            "transaction_datetime": to_dt_str(t_extra1),
            "transaction_date": to_date_str(t_extra1),
            "purchase_id": pid,
            "subsidiary": random.choice(subsidiaries)
        })

        # event 2: update (release_date)
        if pid % 5 != 0:
            t2 = t1 + timedelta(days=random.choice([5, 10, 15]))
            release_date = to_date_str(t2)

            purchase_events.append({
                "transaction_datetime": to_dt_str(t2),
                "transaction_date": to_date_str(t2),
                "purchase_id": pid,
                "buyer_id": buyer_id,
                "prod_item_id": prod_item_id,
                "order_date": order_date,
                "release_date": release_date,
                "producer_id": producer_id
            })

        # event 3: duplicate (item_quantity)
        if pid in [55, 69]:
            t3 = t1 + timedelta(days=25)
            # corrige purchase_value 
            corrected_value = round(purchase_value + random.choice([5.0, -3.0, 10.0]), 2)


            product_item_events.append({
                "transaction_datetime": to_dt_str(t3),
                "transaction_date": to_date_str(t3),
                "purchase_id": pid,
                "product_id": product_id,
                "item_quantity": item_quantity,
                "purchase_value": corrected_value
            })

            # muda subsidiary
            extra_events.append({
                "transaction_datetime": to_dt_str(t3 + timedelta(hours=1)),
                "transaction_date": to_date_str(t3 + timedelta(hours=1)),
                "purchase_id": pid,
                "subsidiary": random.choice(subsidiaries)
            })

            # send purchase event again
            purchase_events.append({
                "transaction_datetime": to_dt_str(t3),
                "transaction_date": to_date_str(t3),
                "purchase_id": pid,
                "buyer_id": buyer_id,
                "prod_item_id": prod_item_id,
                "order_date": order_date,
                "release_date": None if pid % 5 == 0 else to_date_str(t3 - timedelta(days=2)),
                "producer_id": producer_id
            })


    # create dataframes
    df_purchase = pd.DataFrame(purchase_events).sort_values("transaction_datetime")
    df_product_item = pd.DataFrame(product_item_events).sort_values("transaction_datetime")
    df_extra = pd.DataFrame(extra_events).sort_values("transaction_datetime")

    # save to csv
    df_purchase.to_csv(os.path.join(purchase_dir, "purchase.csv"), index=False)
    df_product_item.to_csv(os.path.join(product_item_dir, "product_item.csv"), index=False)
    df_extra.to_csv(os.path.join(extra_dir, "product_extra_info.csv"), index=False)


    print("Mock CDC data generated successfully.")
    print(" -", os.path.join(purchase_dir, "purchase.csv"), f"({len(df_purchase)} rows)")
    print(" -", os.path.join(product_item_dir, "product_item.csv"), f"({len(df_product_item)} rows)")
    print(" -", os.path.join(extra_dir, "purchase_extra_info.csv"), f"({len(df_extra)} rows)")

if __name__ == "__main__":
    main()
