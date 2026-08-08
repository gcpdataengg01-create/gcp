import pandas as pd
from sqlalchemy import create_engine, text


SOURCE_FILE = "data/online_retail_II.xlsx"

DATABASE_URL = (
    "postgresql+psycopg2://"
    "postgres:root@localhost:5433/retail"
)


def main():
    print("Reading Online Retail II workbook...")

    sheets = pd.read_excel(
        SOURCE_FILE,
        sheet_name=None
    )

    print("Sheets:", list(sheets.keys()))

    df = pd.concat(
        sheets.values(),
        ignore_index=True
    )

    print(f"Source rows: {len(df):,}")

    print("Source columns:")
    print(df.columns.tolist())

    # Database-friendly names only.
    # DO NOT cleanse source values here.
    df.columns = [
        "invoice",
        "stock_code",
        "description",
        "quantity",
        "invoice_date",
        "price",
        "customer_id",
        "country"
    ]

    engine = create_engine(DATABASE_URL)

    print("Writing temporary sales_txn_stg table...")

    df.to_sql(
        "sales_txn_stg",
        engine,
        if_exists="replace",
        index=False,
        chunksize=10000
    )

    print("Creating final sales_txn table...")

    with engine.begin() as connection:

        connection.execute(
            text("DROP TABLE IF EXISTS sales_txn")
        )

        connection.execute(
            text("""
                CREATE TABLE sales_txn AS
                SELECT *
                FROM sales_txn_stg
            """)
        )

        connection.execute(
            text("""
                ALTER TABLE sales_txn
                ADD COLUMN txn_id BIGSERIAL PRIMARY KEY
            """)
        )

        connection.execute(
            text("""
                CREATE INDEX idx_sales_txn_date
                ON sales_txn(invoice_date)
            """)
        )

        connection.execute(
            text("""
                DROP TABLE sales_txn_stg
            """)
        )

    print("sales_txn created successfully.")


if __name__ == "__main__":
    main()