import streamlit as st

@st.cache_data(ttl=3600)
def get_release_assets():

    import requests

    owner = "publiusTacitus"
    repo = "simulated_airline_database"

    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"

    r = requests.get(url)

    data = r.json()

    assets = []

    for a in data["assets"]:

        size_gb = a["size"] / (1024**3)

        assets.append({
            "name": a["name"],
            "url": a["browser_download_url"],
            "size_gb": round(size_gb,2),
            "downloads": a["download_count"]
        })

    return assets