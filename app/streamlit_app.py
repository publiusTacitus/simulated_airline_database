# streamlit run C:\Users\Jan\sql\simulated_airline_database\app\streamlit_app.py

import streamlit as st

st.set_page_config(
    page_title="Simulated Airline Database",
    layout="wide"
)

# -------------------
# dataset links
# -------------------

DOWNLOADS = {
    ("1y","."): "https://github.com/publiusTacitus/simulated_airline_database/releases/download/1.0/airline_data_1y_decimal_point.zip",
    ("1y",","): "https://github.com/publiusTacitus/simulated_airline_database/releases/download/1.0/airline_data_1y_decimal_comma.zip",

    ("2y","."): "https://github.com/publiusTacitus/simulated_airline_database/releases/download/1.0/airline_data_2y_decimal_point.zip",
    ("2y",","): "https://github.com/publiusTacitus/simulated_airline_database/releases/download/1.0/airline_data_2y_decimal_comma.zip",

    ("3y","."): "https://github.com/publiusTacitus/simulated_airline_database/releases/download/1.0/airline_data_3y_decimal_point.zip",
    ("3y",","): "https://github.com/publiusTacitus/simulated_airline_database/releases/download/1.0/airline_data_3y_decimal_comma.zip",

    ("snap","."): "https://github.com/publiusTacitus/simulated_airline_database/releases/download/1.0/airline_data_snapshot_decimal_point.zip",
    ("snap",","): "https://github.com/publiusTacitus/simulated_airline_database/releases/download/1.0/airline_data_snapshot_decimal_comma.zip"
}

# -------------------
# Header
# -------------------

st.title("Simulated Airline Database Portal")

st.write("""
Download synthetic relational airline datasets for SQL practice,
analytics portfolios, and exploratory analysis.
""")

# -------------------
# Sidebar controls
# -------------------

st.sidebar.header("Choose Dataset")

dataset = st.sidebar.radio(
    "Simulation extent",
    ["1y","2y","3y","snap"],
    format_func=lambda x: {
        "1y":"1 Year",
        "2y":"2 Years",
        "3y":"3 Years",
        "snap":"Snapshot (3 Weeks)"
    }[x]
)

decimal = st.sidebar.radio(
    "Decimal separator",
    [".",","]
)

download_url = DOWNLOADS[(dataset, decimal)]

st.sidebar.link_button(
    "Download Package",
    download_url
)

# -------------------
# Tabs
# -------------------

tab1, tab2, tab3, tab4 = st.tabs(
    [
        "Overview",
        "Previews",
        "Schema",
        "Notes"
    ]
)

# -------------------
# Overview tab
# -------------------

with tab1:

    st.subheader("What's Included")

    st.markdown("""
- 14 relational tables  
- SQL schema creation script  
- Python PostgreSQL loader  
- Clean and noisy customer variants  
- Snapshot includes Excel workbook  
""")

# -------------------
# Previews tab
# -------------------

with tab2:
    st.subheader("Representative Samples")

    st.write("Featured table previews")

    pv1, pv2, pv3, pv4, pv5 = st.tabs(
        ["Routes", "Flights", "Bookings", "Customers", "Weather"]
    )

    with pv1:
        with open("previews/routes_preview.html", encoding="utf-8") as f:
            html = f.read()
        st.html(html)

    with pv2:
        with open("previews/flights_preview.html", encoding="utf-8") as f:
            html = f.read()
        st.html(html)

    with pv3:
        with open("previews/bookings_preview.html", encoding="utf-8") as f:
            html = f.read()
        st.html(html)

    with pv4:
        with open("previews/customers_preview.html", encoding="utf-8") as f:
            html = f.read()
        st.html(html)

    with pv5:
        with open("previews/weather_preview.html", encoding="utf-8") as f:
            html = f.read()
        st.html(html)

    table_choice = st.selectbox(
        label="Browse other table previews",
        options=[
            "airports",
            "aircraft",
            "flight_capacity_by_class",
            "costs_per_flight",
            "flight_class_cost_shares",
            "frequent_flyer_discounts",
            "class_discount_adjustments",
            "traveller_type_lookup",
        ],
        index=None,
        placeholder="Select a table",
        width=250
    )

    if table_choice:
        with open(f"previews/{table_choice}_preview.html", encoding="utf-8") as f:
            html = f.read()
        st.html(html)

# -------------------
# Schema tab
# -------------------

with tab3:

    st.subheader("Core Join Keys")

    st.code("""
flight_number
line_number
aircraft_id
customer_id
class_id
""")

# -------------------
# Notes tab
# -------------------

with tab4:

    st.subheader("Usage Notes")

    st.write("""
- All datetimes are UTC
- All currency values are EUR
- See included README for KPI suggestions
""")

