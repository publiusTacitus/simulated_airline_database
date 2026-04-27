
import streamlit as st
from asset_requests import get_release_assets

st.set_page_config(page_title="Simulated Airline Database", layout="wide")

# -------------------
# Header
# -------------------

st.title("Simulated Airline Database Portal")

st.write("""
Download synthetic relational dataset intended for data analysis practice and projects 
using SQL, Python, Excel, Power BI, Tableau, etc.
""")

# -------------------
# Asset selection
# -------------------

assets = get_release_assets()

asset_lookup = {}

for a in assets:

    name = a["name"]
    period = None

    if "1y" in name:
        period = "1y"

    elif "2y" in name:
        period = "2y"

    elif "3y" in name:
        period = "3y"

    elif "snapshot" in name:
        period = "snap"

    if "decimal_point" in name:
        dec = "."

    else:
        dec = ","

    asset_lookup[(period, dec)] = a

# -------------------
# Sidebar controls
# -------------------

st.sidebar.header("Choose Dataset")

dataset = st.sidebar.radio(
    "Simulation extent",
    ["1y","2y","3y","snap"],
    format_func=lambda x: {
        "1y":"1-Year",
        "2y":"2-Year",
        "3y":"3-Year",
        "snap":"Snapshot"
    }[x]
)

decimal = st.sidebar.radio(
    "Decimal separator",
    [".",","]
)

selected_asset = asset_lookup[(dataset, decimal)]

download_url = selected_asset["url"]
size_gb = selected_asset["size_gb"]

st.sidebar.caption(f"Package size: {size_gb} GB")
st.sidebar.link_button("Download Package", download_url)

# -------------------
# Tabs
# -------------------

tab1, tab2, tab3, tab4 = st.tabs(
    [
        "Simulation Features",
        "Package Contents",
        "Previews",
        "Resources"
    ]
)

# -------------------
# Simulation Features tab
# -------------------

with tab1:

    st.subheader( "Airline Simulation Features")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
**Network**
- 42 airports
- 142 routes
- 264 daily flights
- 3 distance categories

**Fleet**
- 68 aircraft
- Multi-model operations
""")

    with col2:
        st.markdown("""
**Demand Modeling**
- Route identity effects
- Sim year effects
- Seasonality effects
- Holiday spikes

**Financial Logic**
- Dynamic fares
- Flight cost structure
""")

    with col3:
        st.markdown("""
**Customer Behavior**
- Traveller types and loyalty tiers
- Demographics impact:
    - Booking patterns
    - Check-in reliability

**Weather**
- Delay and cancellation probs
- Delay duration impact
""")

# -------------------
# Package Contents tab
# -------------------

with tab2:

    st.subheader("What's Included")

    st.markdown("""
- 13 relational tables (CSV)
- Clean and noisy customers table variants 
- SQL schema creation script  
- Python to PostgreSQL loader  
- Readme with usage instructions 
- Snapshot includes Excel workbook  
""")

# -------------------
# Previews tab
# -------------------

with tab3:

    st.subheader("Representative Samples")

    st.write("Featured table previews")

    feat_tables = {
        "Flights": "flights",
        "Routes": "routes",
        "Bookings": "bookings",
        "Customers": "customers",
        "Aircraft": "aircraft",
        "Weather": "weather"
    }

    preview_descriptions = {
        "flights": "Operational records, delays, cancellations and capacity metrics.",
        "routes": "Airline routes, distances, and base prices per cabin class.",
        "bookings": "Pricing, revenue, lead times and cabin classes.",
        "customers": "Demographics, loyalty status, and traveller type.",
        "aircraft": "Aircraft models, seat capacities, and ranges.",
        "weather": "Weather conditions, intensities, and quantifiable observations."
    }

    preview_tabs = st.tabs(list(feat_tables.keys()))

    for tab, table, capt in zip(preview_tabs, feat_tables.values(), preview_descriptions.values()):
        with open(f"previews/{table}_preview.html", encoding="utf-8") as f:
            html = f.read()

        tab.caption(capt)
        tab.html(html)

    other_tables = {
        "Airports": "airports",
        "Flight Capacity by Class": "flight_capacity_by_class",
        "Costs per Flight": "costs_per_flight",
        "Flight Class Cost Shares": "flight_class_cost_shares",
        "Frequent Flyer Discounts": "frequent_flyer_discounts",
        "Class Discount Adjustments": "class_discount_adjustments",
        "Traveller Type Lookup": "traveller_type_lookup"
    }

    with st.expander("Browse additional table previews"):

        table_choice = st.selectbox(
            label="",
            options=list(other_tables.keys()),
            index=None,
            placeholder="Select a table",
            width=250
        )

        if table_choice:
            with open(f"previews/{other_tables[table_choice]}_preview.html", encoding="utf-8") as f:
                html = f.read()
            st.html(html)

with tab4:

    st.subheader("Additional Resources")

    st.link_button(
        "View Full Project on GitHub",
        "https://github.com/publiusTacitus/simulated_airline_database"
    )

    st.link_button(
        "Author LinkedIn",
        "https://linkedin.com/in/..."
    )