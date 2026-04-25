
"""
Prerequisites:
- PostgreSQL with a default database (usually "postgres")
- Extract the downloaded outer archive first and keep this script in the same folder as airline_data.zip.
- If a default database password has been set, it must be provided in db_config.
  (The same password will be used for the new airline database.)

Install required libraries:
    pip install pandas sqlalchemy psycopg2

Notes:
- This script is written for PostgreSQL.
- Other SQL systems may require adjustments to this script and the included create_sql_tables script.
  (Can be done by prompting any popular LLM.)
- Large datasets may take significant time to import, especially multi-year versions.
"""

# -----------------------
# USER SETTINGS
# -----------------------

db_config = {
    "default_db": "postgres",
    "new_db": "airline_data",
    "user": "postgres",
    "password": "",
    "host": "localhost",
    "port": "5432",
    "driver": "postgresql"
}

# Use noisy customers data instead of clean customers table
customers_is_noisy = False

# Drop and recreate schema on rerun
is_rerun = False

# Select the right decimal separator for the downloaded csv-files
decimal_sep = "."

# -----------------------


def create_sql_database(
    dbc,
    decimal,
    zip_path,
    customers_has_noise=False,
    reset_database=False
):

    import zipfile
    import pandas as pd
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL

    sep = ";" if decimal == "," else ","

    datetime_cols = [
        "observation_time",
        "scheduled_departure",
        "scheduled_arrival",
        "actual_departure",
        "actual_arrival",
        "booking_time",
        "flight_date",
        "date_of_birth"
    ]

    # -----------------------
    # Create airline database
    # -----------------------

    admin_url = URL.create(
        drivername=dbc["driver"],
        username=dbc["user"],
        password=dbc["password"],
        host=dbc["host"],
        port=dbc["port"],
        database=dbc["default_db"]
    )

    admin_engine = create_engine(admin_url)

    with admin_engine.connect() as conn:

        conn = conn.execution_options(isolation_level="AUTOCOMMIT")

        exists = conn.execute(
            text(
                "SELECT 1 FROM pg_database "
                "WHERE datname = :db"
            ),
            {"db": dbc["new_db"]}
        ).scalar()

        if not exists:
            conn.execute(text(f'CREATE DATABASE "{dbc["new_db"]}"'))
            print(f"Database '{dbc['new_db']}' created.")

        else:
            print(f"Database '{dbc['new_db']}' already exists.")

    # -----------------------
    # Connect to airline db
    # -----------------------

    db_url = URL.create(
        drivername=dbc["driver"],
        username=dbc["user"],
        password=dbc["password"],
        host=dbc["host"],
        port=dbc["port"],
        database=dbc["new_db"]
    )

    engine = create_engine(db_url)

    # -----------------------
    # Optional reset
    # -----------------------

    if reset_database:

        with engine.connect() as conn:

            conn.execute(text("drop schema public cascade;"))
            conn.execute(text("create schema public;"))
            conn.commit()

        print("Database reset.")

    # -----------------------
    # Load data from inner zip
    # -----------------------

    dict_df = {}
    script_sql = None

    with zipfile.ZipFile(zip_path, "r") as zf:

        for file in zf.namelist():

            if file == "create_sql_tables.txt":

                script_sql = (zf.read(file).decode("utf-8"))

            elif file.startswith("tables/") and file.endswith(".csv"):

                table_name = (file.split("/")[-1].replace(".csv", ""))

                df = pd.read_csv(zf.open(file), sep=sep, decimal=decimal, engine="python")

                for col in datetime_cols:

                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors="coerce")

                if "phone" in df.columns:
                    df["phone"] = (df["phone"].astype("string"))

                dict_df[table_name] = df

    print("Dataframes loaded.")

    print("Row counts:", {k: len(v) for k, v in dict_df.items()})

    # -----------------------
    # Create tables
    # -----------------------

    with engine.connect() as conn:

        for stmt in script_sql.split(";"):

            if stmt.strip():
                conn.execute(text(stmt))

        conn.commit()

    print("Tables created.")

    # -----------------------
    # Insert data
    # -----------------------

    load_order = [
        "airports",
        "aircraft",
        "routes",
        "weather",
        "flights",
        "frequent_flyer_discounts",
        "class_discount_adjustments",
        "traveller_type_lookup",
        "customers",
        "bookings",
        "flight_capacity_by_class",
        "flight_class_cost_shares",
        "costs_per_flight"
    ]

    with engine.connect() as conn:

        for table in load_order:

            print(f"Loading {table}...")

            if table == "customers" and customers_has_noise:

                dict_df["customers_noisy"].to_sql(
                    table,con=conn, if_exists="append", index=False, chunksize=50000, method="multi"
                )

            else:

                dict_df[table].to_sql(
                    table, con=conn, if_exists="append", index=False, chunksize=50000, method="multi"
                )

        conn.commit()

    print("Database populated.")


create_sql_database(
    dbc=db_config,
    decimal=decimal_sep,
    zip_path="airline_data.zip",
    customers_has_noise=customers_is_noisy,
    reset_database=is_rerun
)