import os
import pandas as pd
from sqlalchemy import create_engine,text
from dotenv import load_dotenv

load_dotenv()

#set up connection
def get_db_engine():
    db_name = os.getenv("DB_NAME", "ecommerce_db")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST","172.29.16.1")
    db_port = os.getenv("DB_PORT", "5432")
    connection_str=(
        f"postgresql+psycopg2://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )
    return create_engine(connection_str)

#query lay data tu order
def extract_raw_orders(engine) -> pd.DataFrame:
    query = """SELECT
            order_id,
            customer_id,
            item_name,
            price,
            quantity,
            total_amount,
            status,
            created_at
        FROM orders
        WHERE order_id IS NOT NULL
          AND customer_id IS NOT NULL
          AND item_name IS NOT NULL
          AND created_at IS NOT NULL;"""
    return pd.read_sql(query,engine)

def tranform_orders(df: pd.DataFrame) -> pd.DataFrame: #lam sach va chuan hoa data
    if df.empty:
        return df

    clean_df = df.copy()


    #loai trung order id
    clean_df = clean_df.drop_duplicates(subset=["order_id"], keep="last")

    # chuan hoa kieu chuoi
    clean_df["customer_id"] = clean_df["customer_id"].astype(str).str.strip()
    clean_df["iteam_name"] = clean_df["item_name"].astype(str).str.strip()
    clean_df["status"] = clean_df["status"].astype(str).str.strip()

    #chuan hoa kieu so
    clean_df["price"] = pd.to_numeric(clean_df["price"], errors="coerce")
    clean_df["quantity"] = pd.to_numeric(clean_df["quantity"], errors="coerce")
    clean_df["total_amount"] = pd.to_numeric(clean_df["total_amount"], errors="coerce")

    #loc du lieu so hop le
    clean_df = clean_df.dropna(subset=["price","quantity"])
    clean_df = clean_df[(clean_df["price"]>=0) & (clean_df["quantity"]>0)]

    #tinh toan 
    cal_total = (clean_df["price"]* clean_df["quantity"]).round(2)
    clean_df["total_amount"] = clean_df["total_amount"].fillna(cal_total)


    #dinh dang ngay thang
    clean_df["order_timestamp"] = pd.to_datetime(clean_df["created_at"], errors= "coerce")
    clean_df= clean_df.dropna(subset=["order_timestamp"])
    clean_df["full_date"] = clean_df["order_timestamp"].dt.date
    clean_df["date_key"] = clean_df["order_timestamp"].dt.strftime("%Y%m%d").astype(int)

    return clean_df

def load_dimensions_and_fact(engine, df: pd.DataFrame):
    if df.empty:
        print("Du lieu khong hop le")
        return 

    with engine.begin() as conn:
        # upsert customer
        customers = (
            df[["customer_id"]].drop_duplicates().to_dict(orient="records")
        )
        conn.execute(
            text("""
                INSERT INTO dim_customer (customer_id)
                VALUES (:customer_id)
                ON CONFLICT (customer_id) DO NOTHING;
            """),
            customers,
        )

        #upsert items
        items = (
            df.sort_values("order_timestamp").groupby("item_name", as_index=False).agg(standard_price = ("price", "last")).to_dict(orient="records")
        )
        conn.execute(
            text("""
                INSERT INTO dim_item (item_name, standard_price)
                VALUES (:item_name, :standard_price)
                ON CONFLICT (item_name)
                DO UPDATE SET standard_price = EXCLUDED.standard_price;
            """),
            items,
        )

        #upsert date

        dates_df = df[["date_key", "full_date", "order_timestamp"]].drop_duplicates(
            subset=["date_key"]
        )
        dates = [
            {
                "date_key": int(row["date_key"]),
                "full_date": row["full_date"],
                "year": int(row["order_timestamp"].year),
                "quarter": int(row["order_timestamp"].quarter),
                "month": int(row["order_timestamp"].month),
                "day": int(row["order_timestamp"].day),
                "day_of_week": row["order_timestamp"].day_name(),
            }
            for _, row in dates_df.iterrows()
        ]
        conn.execute(
            text("""
                INSERT INTO dim_date (
                    date_key, full_date, year, quarter, month, day, day_of_week
                )
                VALUES (
                    :date_key, :full_date, :year, :quarter, :month, :day, :day_of_week
                )
                ON CONFLICT (date_key) DO NOTHING;
            """),
            dates,
        )



        dim_cust_map = pd.read_sql(
            "SELECT customer_key, customer_id FROM dim_customer;", conn
        )
        dim_item_map = pd.read_sql(
            "SELECT item_key, item_name FROM dim_item;", conn
        )

        fact_df = (
            df.merge(dim_cust_map, on="customer_id", how="inner")
            .merge(dim_item_map, on="item_name", how="inner")
        )

        fact_rows = [
            {
                "order_id": row["order_id"],
                "customer_key": int(row["customer_key"]),
                "item_key": int(row["item_key"]),
                "date_key": int(row["date_key"]),
                "price": float(row["price"]),
                "quantity": int(row["quantity"]),
                "total_amount": float(row["total_amount"]),
                "status": row["status"],
                "order_timestamp": row["order_timestamp"].to_pydatetime(),
            }
            for _, row in fact_df.iterrows()
        ]

        conn.execute(
            text("""
                INSERT INTO fact_orders (
                    order_id, customer_key, item_key, date_key,
                    price, quantity, total_amount, status, order_timestamp
                )
                VALUES (
                    :order_id, :customer_key, :item_key, :date_key,
                    :price, :quantity, :total_amount, :status, :order_timestamp
                )
                ON CONFLICT (order_id)
                DO UPDATE SET
                    customer_key = EXCLUDED.customer_key,
                    item_key = EXCLUDED.item_key,
                    date_key = EXCLUDED.date_key,
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    total_amount = EXCLUDED.total_amount,
                    status = EXCLUDED.status,
                    order_timestamp = EXCLUDED.order_timestamp;
            """),
            fact_rows,
        )

        print(f"ETL hoàn tất! Đã xử lý {len(fact_rows)} bản ghi vào fact_orders.")

def run_etl():
    engine = get_db_engine()
    raw_df = extract_raw_orders(engine)
    print(f"Da trich xuat {len(raw_df)} dong")

    clean_df = tranform_orders(raw_df)
    print(f"Con {len(clean_df)} dong hop le.")

    load_dimensions_and_fact(engine, clean_df)

if __name__ == "__main__":
    run_etl()