
def create_tables(
    only_snapshot=as_snapshot,
    month_snapshot=5,
    start_year=2024,
    n_years=1,
    rng_seed=seed_for_rng,
    show_diagnostics=False
):

    import pandas as pd
    import numpy as np
    import re
    from numpy.random import SeedSequence, default_rng
    from datetime import datetime, timedelta
    from faker import Faker
    from math import floor
    from collections import defaultdict
    from unidecode import unidecode

    seed_main, seed_alt = SeedSequence(rng_seed).spawn(2)
    np_rng_main, np_rng_alt = default_rng(seed_main), default_rng(seed_alt)

    # --- Data creation parameters ---

    def determine_simulation_parameters(
            is_snapshot=only_snapshot,
            snap_month=month_snapshot,
            default_year=start_year,
            years=n_years,
            run_diagnostics=show_diagnostics
        ):

            last_day = timedelta(days=1)

            if is_snapshot:
                start_date = datetime(default_year, snap_month, 1)
                end_date = datetime(default_year, snap_month, 21) + last_day

            else:
                if years == 3:
                    start_date = datetime(default_year, 1, 1)
                    end_date = datetime(default_year + 2, 12, 31) + last_day
                elif years == 2:
                    start_date = datetime(default_year, 1, 1)
                    end_date = datetime(default_year + 1, 12, 31) + last_day
                else:
                    start_date = datetime(default_year, 1, 1)
                    end_date = datetime(default_year, 12, 31) + last_day

            sim_days = (end_date - start_date).days

            if run_diagnostics:
                print(f"Simulation period: {start_date} to {end_date}")
                print(f"Simulation duration: {sim_days} days")

            return start_date, end_date, sim_days


    # --- Airports table ---

    def create_airports_df():

        # Airport data

        airports_data_eur = [
            {"airport_code": "FRA", "airport_name": "Frankfurt Airport", "city": "Frankfurt", "country": "Germany",
             "coordinates": (8.5622, 50.0379), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "LHR", "airport_name": "London Heathrow Airport", "city": "London", "country": "United Kingdom",
             "coordinates": (-0.4543, 51.4700), "climate_region": "Temperate", "is_maritime": True},
            {"airport_code": "BER", "airport_name": "Berlin Brandenburg Airport", "city": "Berlin", "country": "Germany",
             "coordinates": (13.5033, 52.3667), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "HAM", "airport_name": "Hamburg Airport", "city": "Hamburg", "country": "Germany",
             "coordinates": (9.9882, 53.6304), "climate_region": "Temperate", "is_maritime": True},
            {"airport_code": "VIE", "airport_name": "Vienna International Airport", "city": "Vienna", "country": "Austria",
             "coordinates": (16.5697, 48.1103), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "MUC", "airport_name": "Munich Airport", "city": "Munich", "country": "Germany",
             "coordinates": (11.7861, 48.3538), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "MAD", "airport_name": "Adolfo Suárez Madrid–Barajas Airport", "city": "Madrid",
             "country": "Spain", "coordinates": (-3.5608, 40.4722), "climate_region": "Mediterranean", "is_maritime": False},
            {"airport_code": "PMI", "airport_name": "Palma de Mallorca Airport", "city": "Palma", "country": "Spain",
             "coordinates": (2.7388, 39.5517), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "IST", "airport_name": "Istanbul Airport", "city": "Istanbul", "country": "Turkey",
             "coordinates": (28.7519, 41.2753), "climate_region": "Temperate", "is_maritime": True},
            {"airport_code": "BCN", "airport_name": "Barcelona–El Prat Airport", "city": "Barcelona", "country": "Spain",
             "coordinates": (2.0833, 41.2974), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "CDG", "airport_name": "Charles de Gaulle Airport", "city": "Paris", "country": "France",
             "coordinates": (2.5479, 49.0097), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "LIS", "airport_name": "Humberto Delgado Airport", "city": "Lisbon", "country": "Portugal",
             "coordinates": (-9.1342, 38.7742), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "AMS", "airport_name": "Amsterdam Schiphol Airport", "city": "Amsterdam", "country": "Netherlands",
             "coordinates": (4.7683, 52.3105), "climate_region": "Temperate", "is_maritime": True},
            {"airport_code": "ZRH", "airport_name": "Zurich Airport", "city": "Zurich", "country": "Switzerland",
             "coordinates": (8.5555, 47.4581), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "CPH", "airport_name": "Copenhagen Airport", "city": "Copenhagen", "country": "Denmark",
             "coordinates": (12.6561, 55.6181), "climate_region": "Temperate", "is_maritime": True},
            {"airport_code": "FCO", "airport_name": "Leonardo da Vinci–Fiumicino Airport", "city": "Rome", "country": "Italy",
             "coordinates": (12.2389, 41.8003), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "ARN", "airport_name": "Stockholm Arlanda Airport", "city": "Stockholm", "country": "Sweden",
             "coordinates": (17.9186, 59.6519), "climate_region": "Cold", "is_maritime": True},
            {"airport_code": "HEL", "airport_name": "Helsinki-Vantaa Airport", "city": "Helsinki", "country": "Finland",
             "coordinates": (24.9633, 60.3172), "climate_region": "Cold", "is_maritime": True},
            {"airport_code": "OSL", "airport_name": "Oslo Gardermoen Airport", "city": "Oslo", "country": "Norway",
             "coordinates": (11.1004, 60.1939), "climate_region": "Cold", "is_maritime": False},
            {"airport_code": "WAW", "airport_name": "Warsaw Chopin Airport", "city": "Warsaw", "country": "Poland",
             "coordinates": (20.9671, 52.1657), "climate_region": "Cold", "is_maritime": False},
            {"airport_code": "ATH", "airport_name": "Athens International Airport", "city": "Athens", "country": "Greece",
             "coordinates": (23.9475, 37.9364), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "BRU", "airport_name": "Brussels Airport", "city": "Brussels", "country": "Belgium",
             "coordinates": (4.4844, 50.9014), "climate_region": "Temperate", "is_maritime": True}
        ]

        airports_data_eur_mena = [
            {"airport_code": "FRA", "airport_name": "Frankfurt Airport", "city": "Frankfurt", "country": "Germany",
             "coordinates": (8.5622, 50.0379), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "LHR", "airport_name": "London Heathrow Airport", "city": "London", "country": "United Kingdom",
             "coordinates": (-0.4543, 51.4700), "climate_region": "Temperate", "is_maritime": True},
            {"airport_code": "CDG", "airport_name": "Charles de Gaulle Airport", "city": "Paris", "country": "France",
             "coordinates": (2.5479, 49.0097), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "DXB", "airport_name": "Dubai International Airport", "city": "Dubai",
             "country": "United Arab Emirates", "coordinates": (55.3657, 25.2532), "climate_region": "Desert",
             "is_maritime": True},
            {"airport_code": "DOH", "airport_name": "Hamad International Airport", "city": "Doha", "country": "Qatar",
             "coordinates": (51.6081, 25.2736), "climate_region": "Desert", "is_maritime": True},
            {"airport_code": "CAI", "airport_name": "Cairo International Airport", "city": "Cairo", "country": "Egypt",
             "coordinates": (31.4056, 30.1219), "climate_region": "Desert", "is_maritime": True},
            {"airport_code": "TLV", "airport_name": "Ben Gurion Airport", "city": "Tel Aviv", "country": "Israel",
             "coordinates": (34.8867, 32.0114), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "BEY", "airport_name": "Beirut–Rafic Hariri International Airport", "city": "Beirut",
             "country": "Lebanon", "coordinates": (35.4884, 33.8209), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "AMM", "airport_name": "Queen Alia International Airport", "city": "Amman", "country": "Jordan",
             "coordinates": (35.9932, 31.7226), "climate_region": "Desert", "is_maritime": False},
            {"airport_code": "ALG", "airport_name": "Houari Boumediene Airport", "city": "Algiers", "country": "Algeria",
             "coordinates": (3.2154, 36.6910), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "CMN", "airport_name": "Mohammed V International Airport", "city": "Casablanca",
             "country": "Morocco", "coordinates": (-7.5899, 33.3675), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "TUN", "airport_name": "Tunis–Carthage International Airport", "city": "Tunis",
             "country": "Tunisia", "coordinates": (10.2272, 36.8510), "climate_region": "Mediterranean", "is_maritime": True},
            {"airport_code": "LCA", "airport_name": "Larnaca International Airport", "city": "Larnaca", "country": "Cyprus",
             "coordinates": (33.6249, 34.8751), "climate_region": "Mediterranean", "is_maritime": True}
        ]

        airports_data_glob = [
            {"airport_code": "FRA", "airport_name": "Frankfurt Airport", "city": "Frankfurt", "country": "Germany",
             "coordinates": (8.5622, 50.0379), "climate_region": "Temperate", "is_maritime": False},
            {"airport_code": "JFK", "airport_name": "John F. Kennedy International Airport", "city": "New York",
             "country": "United States", "coordinates": (-73.7781, 40.6413), "climate_region": "Temperate",
             "is_maritime": True},
            {"airport_code": "SFO", "airport_name": "San Francisco International Airport", "city": "San Francisco",
             "country": "United States", "coordinates": (-122.3790, 37.6213), "climate_region": "Mediterranean",
             "is_maritime": True},
            {"airport_code": "YYZ", "airport_name": "Toronto Pearson International Airport", "city": "Toronto",
             "country": "Canada", "coordinates": (-79.6248, 43.6777), "climate_region": "Cold", "is_maritime": False},
            {"airport_code": "HKG", "airport_name": "Hong Kong International Airport", "city": "Hong Kong", "country": "China",
             "coordinates": (113.9185, 22.3080), "climate_region": "Subtropical", "is_maritime": True},
            {"airport_code": "SIN", "airport_name": "Singapore Changi Airport", "city": "Singapore", "country": "Singapore",
             "coordinates": (103.9915, 1.3644), "climate_region": "Tropical", "is_maritime": True},
            {"airport_code": "ICN", "airport_name": "Incheon International Airport", "city": "Seoul", "country": "South Korea",
             "coordinates": (126.4407, 37.4602), "climate_region": "Temperate", "is_maritime": True},
            {"airport_code": "GRU", "airport_name": "São Paulo International Airport",
             "city": "São Paulo", "country": "Brazil", "coordinates": (-46.4731, -23.4356), "climate_region": "Subtropical",
             "is_maritime": False},
            {"airport_code": "NRT", "airport_name": "Narita International Airport", "city": "Tokyo", "country": "Japan",
             "coordinates": (140.3929, 35.7720), "climate_region": "Temperate", "is_maritime": True},
            {"airport_code": "JNB", "airport_name": "O. R. Tambo International Airport", "city": "Johannesburg",
             "country": "South Africa", "coordinates": (28.2420, -26.1337), "climate_region": "Subtropical",
             "is_maritime": False},
            {"airport_code": "SYD", "airport_name": "Sydney Kingsford Smith Airport", "city": "Sydney", "country": "Australia",
             "coordinates": (151.1753, -33.9399), "climate_region": "Subtropical", "is_maritime": True}
        ]

        airports_df = (
            pd.DataFrame(airports_data_eur + airports_data_eur_mena + airports_data_glob)
            .drop_duplicates().sort_values("airport_code").reset_index(drop=True)
        )

        # split coordinates (input is: (lon, lat))
        airports_df["latitude"] = airports_df["coordinates"].apply(
            lambda coord: coord[1] if isinstance(coord, tuple) else None
        )
        airports_df["longitude"] = airports_df["coordinates"].apply(
            lambda coord: coord[0] if isinstance(coord, tuple) else None
        )

        airports_df = airports_df.drop(columns=["coordinates"])

        airports_df["hub_status"] = np.select(
            [
                airports_df["airport_code"] == "FRA",
                airports_df["airport_code"] == "LHR",
                airports_df["airport_code"] == "CDG"
            ],
            ["Primary", "Secondary", "Secondary"],
            default="No hub"
        )

        return airports_df


    # --- Aircraft table ---

    def create_aircraft_dfs(output, np_rng=np_rng_main, run_diagnostics=show_diagnostics):

        aircraft_specs_short = [
            {"model": "MC-21-300", "manufacturer": "Irkut", "seat_capacity": 211, "range_km": 6000},
            {"model": "A320neo", "manufacturer": "Airbus", "seat_capacity": 180, "range_km": 6300},
            {"model": "E195-E2", "manufacturer": "Embraer", "seat_capacity": 132, "range_km": 4815},
            {"model": "CRJ900", "manufacturer": "Bombardier", "seat_capacity": 90, "range_km": 2956},
            {"model": "A220-300", "manufacturer": "Airbus", "seat_capacity": 145, "range_km": 6200}
        ]
        manufacture_year_range_short = range(2016, 2021)
        aircraft_distribution_short = [
            (aircraft_specs_short[0], 9),  # MC-21-300
            (aircraft_specs_short[1], 10),  # A320neo
            (aircraft_specs_short[2], 5),  # E195-E2
            (aircraft_specs_short[3], 4),  # CRJ900
            (aircraft_specs_short[4], 8)  # A220-300
        ]

        aircraft_specs_medium = [
            {"model": "737 MAX 8", "manufacturer": "Boeing", "seat_capacity": 189, "range_km": 6570},
            {"model": "737 MAX 9", "manufacturer": "Boeing", "seat_capacity": 193, "range_km": 6575},
            {"model": "A321neo", "manufacturer": "Airbus", "seat_capacity": 206, "range_km": 7400}
        ]
        manufacture_year_range_medium = range(2016, 2024)
        aircraft_distribution_medium = [
            (aircraft_specs_medium[0], 6),  # 737 MAX 8
            (aircraft_specs_medium[1], 7),  # 737 MAX 9
            (aircraft_specs_medium[2], 7),  # A321neo
        ]

        aircraft_specs_long = [
            {"model": "A340-500", "manufacturer": "Airbus", "seat_capacity": 270, "range_km": 16670},
            {"model": "777-200LR", "manufacturer": "Boeing", "seat_capacity": 317, "range_km": 17395}
        ]
        manufacture_year_range_long = range(2007, 2011)
        aircraft_distribution_long = [
            (aircraft_specs_long[0], 5),  # A340-500
            (aircraft_specs_long[1], 7),  # 777-200LR
        ]

        aircraft_dfs = {}
        for name, dist, rng, id_base in zip(
                ["aircraft_short_df", "aircraft_medium_df", "aircraft_long_df"],
                [aircraft_distribution_short, aircraft_distribution_medium, aircraft_distribution_long],
                [manufacture_year_range_short, manufacture_year_range_medium, manufacture_year_range_long],
                [1000, 2000, 3000]):

            df = pd.DataFrame([spec for spec, num in dist for _ in range(num)])
            df["manufacture_year"] = np_rng.choice(list(rng), size=len(df), replace=True)
            df.insert(0, "aircraft_id", "AC" + (df.index + id_base).astype(str))
            aircraft_dfs[name] = df
        aircraft_df = (
            pd.concat(
                [aircraft_dfs["aircraft_short_df"], aircraft_dfs["aircraft_medium_df"], aircraft_dfs["aircraft_long_df"]])
            .sort_values("aircraft_id").reset_index(drop=True)
        )

        if run_diagnostics:
            print("\naircraft diagnostics:\n")
            for test in {
                'aircraft_medium_df["range_km"].min() > aircraft_short_df["range_km"].max()':
                    aircraft_dfs["aircraft_medium_df"]["range_km"].min() > aircraft_dfs["aircraft_short_df"][
                        "range_km"].max(),
                'aircraft_long_df["range_km"].min() > aircraft_medium_df["range_km"].max()':
                    aircraft_dfs["aircraft_long_df"]["range_km"].min() > aircraft_dfs["aircraft_medium_df"][
                        "range_km"].max()
            }.items():
                print(test[0], "\n", test[1], "\n")

        if output == "full":
            return aircraft_df
        elif output == "parts":
            return aircraft_dfs["aircraft_short_df"], aircraft_dfs["aircraft_medium_df"], aircraft_dfs["aircraft_long_df"]
        else:
            raise ValueError(f"Invalid output type: {output}")


    # --- Routes table ---

    def create_route_dfs(output, start_date, end_date, np_rng=np_rng_main, run_diagnostics=show_diagnostics):

        # primary hub: FRA, secondary hubs: LHR, CDG
        ln, dep, arr = "line_number", "departure_airport_code", "arrival_airport_code"
        tyd = "demand_tier"                                         # default year demand tier
        tya = "demand_tier_a"                                       # alt year a demand tier
        tyb = "demand_tier_b"                                       # alt year b demand tier
        t1, t2, t3, t4, t5, t6 = "1_90–100", "2_80–90", "3_70–80", "4_60–70", "5_50–60", "6_<50"
        vl, sn = "volatility", "season_sensitivity"
        vll, vlm, vlh = 0.9, 1.0, 1.15                              # volatility
        snb, snl, snm, snh, snt = 0.85, 0.9, 1.0, 1.15, 1.3         # seasonality

        # short-haul < 1900, 2 flights per day
        short_routes_tiered = [
            {ln: 101, dep: "FRA", arr: "LHR", tyd: t1, vl: vll, sn: snb, tya: t1, tyb: t1},
            {ln: 103, dep: "FRA", arr: "CDG", tyd: t1, vl: vll, sn: snb, tya: t1, tyb: t1},
            {ln: 105, dep: "FRA", arr: "ZRH", tyd: t1, vl: vll, sn: snb, tya: t1, tyb: t1},
            {ln: 143, dep: "LHR", arr: "CDG", tyd: t1, vl: vll, sn: snb, tya: t1, tyb: t1},
            {ln: 145, dep: "LHR", arr: "BER", tyd: t1, vl: vll, sn: snb, tya: t2, tyb: t2},
            {ln: 157, dep: "CDG", arr: "BER", tyd: t1, vl: vll, sn: snb, tya: t2, tyb: t1},

            {ln: 107, dep: "FRA", arr: "BER", tyd: t2, vl: vlm, sn: snl, tya: t2, tyb: t2},
            {ln: 109, dep: "FRA", arr: "VIE", tyd: t2, vl: vlm, sn: snl, tya: t3, tyb: t2},
            {ln: 111, dep: "FRA", arr: "AMS", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t1},
            {ln: 113, dep: "FRA", arr: "MUC", tyd: t2, vl: vll, sn: snl, tya: t3, tyb: t2},
            {ln: 147, dep: "LHR", arr: "VIE", tyd: t2, vl: vlm, sn: snl, tya: t2, tyb: t2},
            {ln: 149, dep: "LHR", arr: "ZRH", tyd: t2, vl: vll, sn: snl, tya: t1, tyb: t2},
            {ln: 159, dep: "CDG", arr: "AMS", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t1},
            {ln: 161, dep: "CDG", arr: "ZRH", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t2},
            {ln: 181, dep: "HAM", arr: "MUC", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t2},
            {ln: 185, dep: "MUC", arr: "AMS", tyd: t2, vl: vlm, sn: snl, tya: t4, tyb: t3},
            {ln: 165, dep: "BER", arr: "HAM", tyd: t2, vl: vll, sn: snl, tya: t3, tyb: t2},
            {ln: 167, dep: "BER", arr: "MUC", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t3},

            {ln: 115, dep: "FRA", arr: "HAM", tyd: t3, vl: vlm, sn: snl, tya: t3, tyb: t2},
            {ln: 117, dep: "FRA", arr: "MAD", tyd: t3, vl: vlm, sn: snm, tya: t3, tyb: t3},
            {ln: 119, dep: "FRA", arr: "IST", tyd: t3, vl: vlm, sn: snm, tya: t3, tyb: t3},
            {ln: 121, dep: "FRA", arr: "LIS", tyd: t3, vl: vlm, sn: snh, tya: t4, tyb: t3},
            {ln: 123, dep: "FRA", arr: "FCO", tyd: t3, vl: vlm, sn: snh, tya: t2, tyb: t2},
            {ln: 125, dep: "FRA", arr: "ARN", tyd: t3, vl: vlm, sn: snm, tya: t3, tyb: t3},
            {ln: 127, dep: "FRA", arr: "WAW", tyd: t3, vl: vlm, sn: snm, tya: t3, tyb: t2},
            {ln: 129, dep: "FRA", arr: "BRU", tyd: t3, vl: vlm, sn: snl, tya: t3, tyb: t3},
            {ln: 151, dep: "LHR", arr: "HAM", tyd: t3, vl: vlm, sn: snl, tya: t4, tyb: t3},
            {ln: 153, dep: "LHR", arr: "MAD", tyd: t3, vl: vlm, sn: snm, tya: t3, tyb: t2},
            {ln: 155, dep: "LHR", arr: "BRU", tyd: t3, vl: vlm, sn: snl, tya: t3, tyb: t3},
            {ln: 169, dep: "BER", arr: "VIE", tyd: t3, vl: vlm, sn: snl, tya: t3, tyb: t2},
            {ln: 173, dep: "BER", arr: "ARN", tyd: t3, vl: vlm, sn: snm, tya: t3, tyb: t3},
            {ln: 177, dep: "VIE", arr: "IST", tyd: t3, vl: vlm, sn: snh, tya: t3, tyb: t3},

            {ln: 131, dep: "FRA", arr: "BCN", tyd: t4, vl: vlm, sn: snh, tya: t5, tyb: t4},
            {ln: 133, dep: "FRA", arr: "CPH", tyd: t4, vl: vlm, sn: snm, tya: t4, tyb: t3},
            {ln: 135, dep: "FRA", arr: "HEL", tyd: t4, vl: vlm, sn: snm, tya: t5, tyb: t5},
            {ln: 137, dep: "FRA", arr: "OSL", tyd: t4, vl: vlm, sn: snm, tya: t4, tyb: t4},
            {ln: 163, dep: "CDG", arr: "HAM", tyd: t4, vl: vlh, sn: snm, tya: t4, tyb: t2},
            {ln: 171, dep: "BER", arr: "MAD", tyd: t4, vl: vlh, sn: snh, tya: t4, tyb: t4},
            {ln: 175, dep: "VIE", arr: "HAM", tyd: t4, vl: vlh, sn: snm, tya: t3, tyb: t3},
            {ln: 179, dep: "VIE", arr: "BRU", tyd: t4, vl: vlh, sn: snm, tya: t4, tyb: t4},

            {ln: 139, dep: "FRA", arr: "ATH", tyd: t5, vl: vlh, sn: snh, tya: t5, tyb: t4},
            {ln: 141, dep: "FRA", arr: "PMI", tyd: t5, vl: vlh, sn: snt, tya: t5, tyb: t5},
            {ln: 183, dep: "BRU", arr: "MAD", tyd: t5, vl: vlh, sn: snh, tya: t4, tyb: t4},
            {ln: 187, dep: "FRA", arr: "TUN", tyd: t5, vl: vlh, sn: snt, tya: t5, tyb: t4},
            {ln: 189, dep: "FRA", arr: "ALG", tyd: t5, vl: vlh, sn: snt, tya: t5, tyb: t5},
        ]

        # medium-haul, 2 flights per day
        medium_routes_tiered = [
            {ln: 191, dep: "FRA", arr: "DXB", tyd: t1, vl: vll, sn: snl, tya: t1, tyb: t1},
            {ln: 207, dep: "LHR", arr: "DXB", tyd: t1, vl: vll, sn: snl, tya: t1, tyb: t1},
            {ln: 193, dep: "FRA", arr: "DOH", tyd: t2, vl: vll, sn: snl, tya: t1, tyb: t2},
            {ln: 195, dep: "FRA", arr: "CAI", tyd: t2, vl: vll, sn: snl, tya: t3, tyb: t2},
            {ln: 209, dep: "LHR", arr: "DOH", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t1},
            {ln: 215, dep: "CDG", arr: "DOH", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t2},
            {ln: 197, dep: "FRA", arr: "CMN", tyd: t3, vl: vlm, sn: snh, tya: t4, tyb: t3},
            {ln: 211, dep: "LHR", arr: "IST", tyd: t3, vl: vlm, sn: snm, tya: t3, tyb: t3},
            {ln: 213, dep: "LHR", arr: "CAI", tyd: t3, vl: vlm, sn: snh, tya: t3, tyb: t3},
            {ln: 217, dep: "CDG", arr: "IST", tyd: t3, vl: vlm, sn: snm, tya: t3, tyb: t3},
            {ln: 199, dep: "FRA", arr: "TLV", tyd: t4, vl: vlm, sn: snm, tya: t4, tyb: t4},
            {ln: 201, dep: "FRA", arr: "LCA", tyd: t4, vl: vlm, sn: snh, tya: t4, tyb: t4},
            {ln: 219, dep: "CDG", arr: "TLV", tyd: t4, vl: vlm, sn: snm, tya: t5, tyb: t5},
            {ln: 203, dep: "FRA", arr: "AMM", tyd: t5, vl: vlh, sn: snm, tya: t5, tyb: t5},
            {ln: 205, dep: "FRA", arr: "BEY", tyd: t6, vl: vlh, sn: snt, tya: t6, tyb: t6},
            {ln: 221, dep: "CDG", arr: "AMM", tyd: t6, vl: vlh, sn: snt, tya: t6, tyb: t5},
        ]

        # long-haul >= 5500, 1 flight per day
        long_routes_tiered = [
            {ln: 223, dep: "FRA", arr: "JFK", tyd: t1, vl: vll, sn: snb, tya: t1, tyb: t1},
            {ln: 225, dep: "FRA", arr: "SIN", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t2},
            {ln: 227, dep: "FRA", arr: "NRT", tyd: t2, vl: vll, sn: snl, tya: t3, tyb: t2},
            {ln: 229, dep: "FRA", arr: "SFO", tyd: t2, vl: vll, sn: snl, tya: t2, tyb: t2},
            {ln: 231, dep: "FRA", arr: "YYZ", tyd: t3, vl: vlm, sn: snm, tya: t4, tyb: t3},
            {ln: 233, dep: "FRA", arr: "HKG", tyd: t3, vl: vlm, sn: snl, tya: t3, tyb: t3},
            {ln: 235, dep: "FRA", arr: "ICN", tyd: t3, vl: vlm, sn: snl, tya: t3, tyb: t3},
            {ln: 237, dep: "FRA", arr: "SYD", tyd: t4, vl: vlm, sn: snm, tya: t4, tyb: t4},
            {ln: 239, dep: "FRA", arr: "GRU", tyd: t4, vl: vlh, sn: snm, tya: t4, tyb: t5},
            {ln: 241, dep: "FRA", arr: "JNB", tyd: t5, vl: vlh, sn: snm, tya: t5, tyb: t4},
        ]


        def create_base_routes(route_records, date_start, date_end, np_rng_=np_rng):

            routes = pd.DataFrame(route_records)

            tier_map = {
                "1_90–100": (0.00, 0.02),
                "2_80–90":  (0.00, 0.025),
                "3_70–80":  (0.00, 0.03),
                "4_60–70":  (0.00, 0.035),
                "5_50–60":  (0.00, 0.04),
                "6_<50":    (0.00, 0.045),
            }

            sim_years = round(((date_end - date_start) / 365).days)

            if sim_years >= 2:
                routes.rename(columns={
                    "demand_tier_a": f"demand_tier_{date_start.year}",
                    "demand_tier":   f"demand_tier_{date_start.year + 1}",
                    "demand_tier_b": f"demand_tier_{date_start.year + 2}",
                }, inplace=True)

                routes[f"base_demand_bias_{date_start.year + 1}"] = [
                    np_rng_.normal(mean, std)
                    for mean, std in (tier_map[t] for t in routes[f"demand_tier_{date_start.year + 1}"])
                ]

                y2_ranks = [int(r[0]) for r in routes[f"demand_tier_{date_start.year + 1}"]]
                y1_ranks = [int(r[0]) for r in routes[f"demand_tier_{date_start.year}"]]
                y3_ranks = [int(r[0]) for r in routes[f"demand_tier_{date_start.year + 2}"]]

                routes[f"base_demand_bias_{date_start.year}"] = np.clip(
                    routes[f"base_demand_bias_{date_start.year + 1}"]
                    * [1 + 0.2 * (y1_ranks[i] - y2_ranks[i]) for i in range(len(routes))]
                    + np_rng_.normal(0, 0.002, len(routes)),
                    -0.08, 0.08
                )

                routes[f"base_demand_bias_{date_start.year + 2}"] = np.clip(
                    routes[f"base_demand_bias_{date_start.year + 1}"]
                    * [1 + 0.2 * (y3_ranks[i] - y2_ranks[i]) for i in range(len(routes))]
                    + np_rng_.normal(0, 0.002, len(routes)),
                    -0.08, 0.08
                )

                if sim_years > 3:
                    for y in range(3, sim_years):
                        routes[f"demand_tier_{date_start.year + y}"] = routes[f"demand_tier_{date_start.year + 1}"]
                        routes[f"base_demand_bias_{date_start.year + y}"] = routes[f"base_demand_bias_{date_start.year + 1}"]


            else:
                routes.rename(columns={"demand_tier": f"demand_tier_{date_start.year}"}, inplace=True)
                routes = routes.drop(columns=["demand_tier_a", "demand_tier_b"])

                routes[f"base_demand_bias_{date_start.year}"] = [
                    np_rng_.normal(mean, std)
                    for mean, std in (tier_map[t] for t in routes[f"demand_tier_{date_start.year}"])
                ]

            return routes


        def create_reverse_routes(routes, date_start, sn_order, np_rng_=np_rng):

            routes_rev = routes.copy()

            routes_rev["line_number"] = routes["line_number"] + 1
            routes_rev["departure_airport_code"] = routes["arrival_airport_code"]
            routes_rev["arrival_airport_code"] = routes["departure_airport_code"]

            for col in [
                f"base_demand_bias_{date_start.year}",
                f"base_demand_bias_{date_start.year + 1}",
                f"base_demand_bias_{date_start.year + 2}"
            ]:
                if col in routes.columns:
                    routes_rev[col] = routes[col] * np_rng_.uniform(0.97, 1.03, len(routes))

            if "volatility" in routes.columns:
                routes_rev["volatility"] = routes["volatility"] * np_rng_.uniform(0.92, 1.08, len(routes))

            if "season_sensitivity" in routes.columns:
                shift_prob = 0.15

                order_map = {k: i for i, k in enumerate(sn_order)}

                def maybe_shift(sn_):
                    if np_rng_.random() > shift_prob:
                        return sn_

                    idx = order_map[sn_]
                    shift = np_rng_.choice([-1, 1])

                    new_idx = np.clip(idx + shift, 0, len(sn_order) - 1)
                    return sn_order[new_idx]

                routes_rev["season_sensitivity"] = routes["season_sensitivity"].apply(maybe_shift)

            return (
                pd.concat([routes, routes_rev]).sort_values("line_number").reset_index(drop=True)
            )


        def assign_distance(routes, airports):

            coord_map = airports.set_index("airport_code")[["latitude", "longitude"]].to_dict("index")

            routes["dep_lat"] = routes["departure_airport_code"].map(lambda x: coord_map[x]["latitude"])
            routes["dep_lon"] = routes["departure_airport_code"].map(lambda x: coord_map[x]["longitude"])
            routes["arr_lat"] = routes["arrival_airport_code"].map(lambda x: coord_map[x]["latitude"])
            routes["arr_lon"] = routes["arrival_airport_code"].map(lambda x: coord_map[x]["longitude"])

            # vectorized haversine
            radius = 6371
            lat1 = np.radians(routes["dep_lat"].to_numpy())
            lon1 = np.radians(routes["dep_lon"].to_numpy())
            lat2 = np.radians(routes["arr_lat"].to_numpy())
            lon2 = np.radians(routes["arr_lon"].to_numpy())

            d_lat = lat2 - lat1
            d_lon = lon2 - lon1

            a_ = np.sin(d_lat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(d_lon / 2)**2
            c_ = 2 * np.arcsin(np.sqrt(a_))

            routes["distance_km"] = (radius * c_).round().astype("int64")

            return routes.drop(columns=["dep_lat", "dep_lon", "arr_lat", "arr_lon"])


        routes_short_df = create_base_routes(short_routes_tiered, start_date, end_date)
        routes_medium_df = create_base_routes(medium_routes_tiered, start_date, end_date)
        routes_long_df = create_base_routes(long_routes_tiered, start_date, end_date)

        routes_short_df = create_reverse_routes(routes_short_df, start_date, [snb, snl, snm, snh, snt])
        routes_medium_df = create_reverse_routes(routes_medium_df, start_date, [snb, snl, snm, snh, snt])
        routes_long_df = create_reverse_routes(routes_long_df, start_date, [snb, snl, snm, snh, snt])

        routes_short_df = assign_distance(routes_short_df, df_airports)
        routes_medium_df = assign_distance(routes_medium_df, df_airports)
        routes_long_df = assign_distance(routes_long_df, df_airports)

        routes_short_df["flights_per_day"] = 2
        routes_medium_df["flights_per_day"] = 2
        routes_long_df["flights_per_day"] = 1

        routes_df = (
                pd.concat([routes_short_df, routes_medium_df, routes_long_df]).sort_values("line_number").reset_index(drop=True)
            )

        routes_df.insert(
            routes_df.columns.get_loc("line_number") + 1, "route_name",
            (routes_df.departure_airport_code + " -> " + routes_df.arrival_airport_code)
        )

        if run_diagnostics:
            print("\nroute diagnostics:\n")
            print("total flights per day: ", routes_df["flights_per_day"].sum(), "\n")
            print("line numbers unique:",
                (len(routes_df.line_number.unique()) ==
                (len(short_routes_tiered) + len(medium_routes_tiered) + len(long_routes_tiered)) * 2)
            )

            routes_ = set(zip(routes_df["departure_airport_code"], routes_df["arrival_airport_code"]))
            reverse_routes = set((arr, dep) for dep, arr in routes_)
            missing_reverse_routes = routes_ - reverse_routes
            if missing_reverse_routes:
                print("routes missing reverse counterparts:")
                for route in missing_reverse_routes:
                    print(f"{route[0]} → {route[1]}")
            else:
                print("\nall routes have reverse counterparts")

        if output == "full":
            return routes_df
        elif output == "parts":
            return routes_short_df, routes_medium_df, routes_long_df
        else:
            raise ValueError(f"Invalid output type: {output}")


    # --- Weather table ---

    def create_weather_df(airports_df, start_date, end_date, np_rng=np_rng_main, run_diagnostics=show_diagnostics):

        """
        Create weather blocks with conditions persisting for multiple hours. Take hourly observation samples.
        Cancellation and delay probabilities used for flight generation, dropped afterward.
        Possible improvements:
            Make weather blocks impact each other, creating gradual transitions.
            Combine cancellation and delay probabilities into a single hazard severity score.
        """

        weather_probs = {
            "Temperate": {
                "Spring": (["Clear", "Cloudy", "Rain", "Thunderstorm", "Fog"], [0.25, 0.30, 0.25, 0.15, 0.05]),
                "Summer": (["Clear", "Cloudy", "Rain", "Thunderstorm"], [0.45, 0.30, 0.15, 0.10]),
                "Autumn": (["Cloudy", "Rain", "Clear", "Fog", "Thunderstorm"], [0.30, 0.30, 0.20, 0.15, 0.05]),
                "Winter": (["Cloudy", "Rain", "Snow", "Fog", "Clear"], [0.30, 0.25, 0.15, 0.20, 0.10]),
            },
            "Mediterranean": {
                "Spring": (["Clear", "Cloudy", "Rain", "Thunderstorm"], [0.50, 0.25, 0.15, 0.10]),
                "Summer": (["Clear", "Cloudy", "Haze", "Thunderstorm"], [0.65, 0.20, 0.10, 0.05]),
                "Autumn": (["Clear", "Cloudy", "Rain", "Thunderstorm"], [0.40, 0.25, 0.25, 0.10]),
                "Winter": (["Clear", "Cloudy", "Rain", "Thunderstorm"], [0.35, 0.30, 0.25, 0.10]),
            },
            "Cold": {
                "Spring": (["Snow", "Rain", "Cloudy", "Clear", "Fog"], [0.25, 0.25, 0.25, 0.15, 0.10]),
                "Summer": (["Clear", "Cloudy", "Rain", "Fog", "Thunderstorm"], [0.45, 0.30, 0.15, 0.05, 0.05]),
                "Autumn": (["Cloudy", "Rain", "Clear", "Fog", "Snow"], [0.30, 0.25, 0.20, 0.15, 0.10]),
                "Winter": (["Snow", "Cloudy", "Clear", "Blizzard", "Fog"], [0.40, 0.25, 0.15, 0.10, 0.10]),
            },
            "Desert": {
                "Spring": (["Clear", "Haze", "Dust", "Sandstorm"], [0.50, 0.20, 0.20, 0.10]),
                "Summer": (["Clear", "Haze", "Dust", "Sandstorm"], [0.40, 0.25, 0.20, 0.15]),
                "Autumn": (["Clear", "Haze", "Dust"], [0.65, 0.25, 0.10]),
                "Winter": (["Clear", "Haze", "Dust"], [0.70, 0.20, 0.10]),
            },
            "Tropical": {
                "Spring": (["Rain", "Thunderstorm", "Clear"], [0.5, 0.35, 0.15]),
                "Summer": (["Thunderstorm", "Rain", "Haze", "Fog"], [0.4, 0.35, 0.15, 0.10]),
                "Autumn": (["Rain", "Thunderstorm", "Clear"], [0.5, 0.35, 0.15]),
                "Winter": (["Rain", "Thunderstorm", "Clear", "Haze"], [0.45, 0.25, 0.20, 0.10]),
            },
            "Subtropical": {
                "Spring": (["Clear", "Cloudy", "Rain", "Thunderstorm"], [0.35, 0.30, 0.20, 0.15]),
                "Summer": (["Rain", "Thunderstorm", "Clear"], [0.40, 0.35, 0.25]),
                "Autumn": (["Clear", "Cloudy", "Rain", "Thunderstorm"], [0.40, 0.25, 0.25, 0.10]),
                "Winter": (["Clear", "Cloudy", "Rain"], [0.45, 0.30, 0.25]),
            }
        }

        intensity_probs = {
            "Clear": ["light"],
            "Cloudy": ["light"],
            "Rain": ["light", "moderate", "heavy"],
            "Snow": ["light", "moderate", "heavy"],
            "Thunderstorm": ["moderate", "heavy"],
            "Fog": ["moderate", "heavy"],
            "Blizzard": ["heavy"],
            "Sandstorm": ["heavy"],
            "Haze": ["light", "moderate"],
            "Dust": ["light", "moderate"]
        }

        def get_season(dt):
            m = dt.month
            if m in [12, 1, 2]:
                return "Winter"
            elif m in [3, 4, 5]:
                return "Spring"
            elif m in [6, 7, 8]:
                return "Summer"
            else:
                return "Autumn"

        def generate_metrics(condition_, intensity_, is_maritime_, np_rng_):

            if condition_ == "Fog":
                visibility_ = np_rng_.uniform(0.1, 2)
            elif condition_ in ["Rain", "Snow"]:
                visibility_ = np_rng_.uniform(1, 8)
            elif condition_ == "Dust":
                visibility_ = np_rng_.uniform(2, 10)
            else:
                visibility_ = np_rng_.uniform(8, 20)

            wind_ = np_rng_.uniform(5, 20)
            if condition_ in ["Thunderstorm", "Sandstorm", "Blizzard"]:
                wind_ += np_rng_.uniform(10, 25)

            if condition_ == "Dust":
                wind_ += np_rng_.uniform(5, 15)

            if is_maritime_:
                wind_ *= 1.2

            if condition_ in ["Rain", "Thunderstorm"]:
                precip_ = {"light": 1, "moderate": 5, "heavy": 15}[intensity_]
            elif condition_ in ["Snow"]:
                precip_ = {"light": 0.5, "moderate": 2, "heavy": 5}[intensity_]
            else:
                precip_ = 0

            base_delay = {
                "Clear": 0.01, "Cloudy": 0.02, "Rain": 0.08, "Snow": 0.15,
                "Fog": 0.2, "Thunderstorm": 0.25, "Blizzard": 0.4,
                "Sandstorm": 0.35, "Haze": 0.05, "Dust": 0.12
            }

            base_cancel = {
                "Clear": 0.001, "Cloudy": 0.005, "Rain": 0.02, "Snow": 0.08,
                "Fog": 0.05, "Thunderstorm": 0.12, "Blizzard": 0.3,
                "Sandstorm": 0.25, "Haze": 0.02, "Dust": 0.08
            }

            delay_prob_ = base_delay.get(condition_, 0.05) * 1.3
            cancel_prob_ = base_cancel.get(condition_, 0.02)

            if intensity_ == "heavy":
                delay_prob_ *= 1.7
                cancel_prob_ *= 1.8

            return visibility_, wind_, precip_, delay_prob_, cancel_prob_


        records = []
        weather_id = 0
        block_id = 0

        for _, airport in airports_df.iterrows():

            current_time = pd.Timestamp(start_date)

            while current_time < pd.Timestamp(end_date):

                season = get_season(current_time)
                region = airport["climate_region"]
                is_maritime = airport["is_maritime"]

                conditions, weights = weather_probs.get(region, {}).get(season)
                condition = np_rng.choice(conditions, p=weights)

                intensity = np_rng.choice(intensity_probs[condition])

                # block duration
                if condition == "Thunderstorm":
                    duration = np_rng.integers(1, 4)
                elif condition == "Fog":
                    duration = np_rng.integers(6, 18)
                elif condition in ["Snow", "Blizzard"]:
                    duration = np_rng.integers(6, 18)
                elif condition == "Dust":
                    duration = np_rng.integers(2, 6)
                else:
                    duration = np_rng.integers(2, 8)

                # generate metrics once per block
                visibility, wind, precip, delay_prob, cancel_prob = generate_metrics(
                    condition, intensity, is_maritime, np_rng
                )

                block_id += 1

                for h in range(duration):

                    obs_time = current_time + pd.Timedelta(hours=h)
                    if obs_time >= pd.Timestamp(end_date):
                        break

                    records.append({
                        "weather_id": weather_id,
                        "airport_code": airport["airport_code"],
                        "observation_time": obs_time,
                        "season": season,
                        "condition": condition,
                        "intensity": intensity,
                        "visibility_km": visibility,
                        "wind_speed_kts": wind,
                        "precip_mm_per_hr": precip,
                        "delay_prob": delay_prob,
                        "cancel_prob": cancel_prob,
                        "weather_block_id": block_id,
                        "climate_region": region,
                        "is_maritime": is_maritime,
                    })

                    weather_id += 1

                current_time += pd.Timedelta(hours=duration)

        if run_diagnostics:
            print("\nweather diagnostics:\n")
            diagnosis_weather = pd.DataFrame(records)
            for test in {
                "observations per block": diagnosis_weather.groupby(["weather_block_id"])[
                    "observation_time"].count().describe(),
                "unique dates": diagnosis_weather["observation_time"].dt.date.nunique(),
                "combined probabilities": diagnosis_weather.groupby(["is_maritime", "climate_region", "season", "condition"]
                    )["weather_block_id"].nunique().groupby(level=[0, 1, 2]).apply(lambda x: x / x.sum()),
            }.items():
                print(test[0], "\n", test[1], "\n")

        return pd.DataFrame(records).drop(columns=["climate_region", "is_maritime"])


    # --- Flights table ---

    def create_flights_df(create_aircraft_function, create_routes_function, start_date, end_date, days,
                          rng_np_main=np_rng_main, rng_np_alt=np_rng_alt, run_diagnostics=show_diagnostics
    ):

        """
        Creates flights in portions based on flights per day per route.
        Currently: short- and medium-haul routes: 2 flights per day, long-haul routes: 1 flight per day,
            meaning short- and medium-haul routes and the corresponding aircraft pools are divided into 2 sections.
            This allows for a minimum offset between the departure times of two flights on the same route.

        Consistent flight scheduling is enabled by tracking each aircraft's location and making departures dependent
        on actual aircraft availability at a given time and place.
            Upside: Aircraft travel paths are fully accounted for, no 'teleportation' effects.
            Downside: Departures tend to be densely clustered around certain times of the day. Increasing the offset
                between sections will lead to cancelled flights due to unavailable aircraft.
        """

        def assign_initial_locations(routes_df, aircraft_df, np_rng_=rng_np_main):

            long = routes_df["distance_km"].min() >= 6000

            if long:
                loc_long = {}
                for n in range(len(aircraft_df)):
                    aircraft = aircraft_df.iloc[n]["aircraft_id"]
                    loc_long[aircraft] = "FRA"
                loc_long[aircraft_df["aircraft_id"][0]] = "SYD"
                loc_long[aircraft_df["aircraft_id"][1]] = "SIN"

                return loc_long

            else:
                departure_counts = routes_df["departure_airport_code"].value_counts().to_dict()
                num_aircraft = len(aircraft_df)
                total_routes = sum(departure_counts.values())

                allocations = {}
                remainders = {}

                for airport_, count in departure_counts.items():
                    proportion = (count / total_routes) * num_aircraft
                    allocations[airport_] = floor(proportion)
                    remainders[airport_] = proportion - allocations[airport_]

                assigned_total = sum(allocations.values())
                remaining = num_aircraft - assigned_total
                for airport_ in sorted(remainders, key=remainders.get, reverse=True)[:remaining]:
                    allocations[airport_] += 1

                airport_list = []
                for airport_, count in allocations.items():
                    airport_list.extend([airport_] * count)

                np_rng_.shuffle(airport_list)
                aircraft_ids = aircraft_df["aircraft_id"].tolist()

                return dict(zip(aircraft_ids, airport_list))


        def separate_aircraft_pools(aircraft_df):
            df_parts = []
            for model, group in aircraft_df.groupby("model"):
                group_shuffled = group.sample(frac=1, random_state=50)
                half = len(group_shuffled) // 2
                df_parts.append((group_shuffled.iloc[:half], group_shuffled.iloc[half:]))
            df_a = pd.concat([part[0] for part in df_parts]).reset_index(drop=True)
            df_b = pd.concat([part[1] for part in df_parts]).reset_index(drop=True)
            return df_a, df_b


        df_aircraft_short, df_aircraft_medium, df_aircraft_long = create_aircraft_function("parts")
        df_routes_short, df_routes_medium, df_routes_long = create_routes_function("parts", start_date, end_date)

        df_aircraft_short_a, df_aircraft_short_b = separate_aircraft_pools(df_aircraft_short)
        df_aircraft_medium_a, df_aircraft_medium_b = separate_aircraft_pools(df_aircraft_medium)

        initial_locations_short_a = assign_initial_locations(df_routes_short, df_aircraft_short_a)
        initial_locations_short_b = assign_initial_locations(df_routes_short, df_aircraft_short_b)
        initial_locations_medium_a = assign_initial_locations(df_routes_medium, df_aircraft_medium_a)
        initial_locations_medium_b = assign_initial_locations(df_routes_medium, df_aircraft_medium_b)
        initial_locations_long = assign_initial_locations(df_routes_long, df_aircraft_long)

        initial_loc_short_med_a = {**initial_locations_short_a, **initial_locations_medium_a}
        initial_loc_short_med_b = {**initial_locations_short_b, **initial_locations_medium_b}


        def create_flights(section, np_rng, date_start=start_date, sim_days=days):

            def generate_bookings(
                flight_date_, demand_tier_, seat_capacity_, base_demand_bias_, volatility_,
                seasonality_, np_rng_=np_rng
            ):

                weekday = flight_date_.weekday()
                month = flight_date_.month

                tier_config = {
                    "1_90–100": {"base": 0.93, "amp": 0.50, "noise": 0.020, "floor": 0.80, "ceiling": 0.995},
                    "2_80–90":  {"base": 0.85, "amp": 0.65, "noise": 0.025, "floor": 0.70, "ceiling": 0.95},
                    "3_70–80":  {"base": 0.75, "amp": 0.80, "noise": 0.035, "floor": 0.60, "ceiling": 0.90},
                    "4_60–70":  {"base": 0.65, "amp": 0.95, "noise": 0.045, "floor": 0.55, "ceiling": 0.85},
                    "5_50–60":  {"base": 0.55, "amp": 1.10, "noise": 0.060, "floor": 0.48, "ceiling": 0.80},
                    "6_<50":    {"base": 0.45, "amp": 1.20, "noise": 0.070, "floor": 0.38, "ceiling": 0.75},
                }

                cfg = tier_config[demand_tier_]

                base = (
                    cfg["base"] * (1 - abs(base_demand_bias_)) if demand_tier_ == "6_<50"
                    else cfg["base"] * (1 + base_demand_bias_)
                )

                weekday_base = {
                    0: 0.00, 1: -0.06, 2: 0.00, 3: 0.00,
                    4: 0.08, 5: -0.06, 6: 0.08
                }

                weekday_micro = {
                    0: -0.005, 1: -0.002, 2:  0.000, 3:  0.003,
                    4:  0.006, 5: -0.004, 6:  0.004
                }

                weekday_adj = weekday_base[weekday] + weekday_micro[weekday]

                if weekday in [1, 5] and demand_tier_ == "4_low":
                    weekday_adj = -0.04

                seasonal_base_map = {
                    1: -0.055, 2: -0.050, 3:  -0.005, 4:  0.000,
                    5:  0.005, 6:  0.065, 7:  0.075, 8:  0.070,
                    9:  0.003, 10: -0.003, 11: -0.045, 12: 0.060
                }

                seasonal_base = seasonal_base_map[month]

                seasonal_adj = seasonal_base * seasonality_

                holiday_base = (
                    0.06 if (
                        (month == 12 and flight_date_.day >= 20) or
                        (month == 1 and flight_date_.day <= 3)
                    )
                    else 0.0
                )

                holiday_adj = holiday_base * seasonality_
                holiday_adj *= np_rng_.normal(1.0, 0.15)

                total_adj = weekday_adj + seasonal_adj + holiday_adj

                # apply tier amplitude
                if total_adj > 0:
                    total_adj *= cfg["amp"]
                else:
                    total_adj *= (cfg["amp"] + 0.2)

                total_adj *= (0.7 + 0.3 * volatility_)

                noise = np_rng_.normal(0, cfg["noise"] * volatility_)

                booked_rate_ = base + total_adj + noise

                # soft bounds
                min_floor = cfg["floor"]
                max_ceiling = cfg["ceiling"]

                if booked_rate_ < min_floor:
                    booked_rate_ = min_floor + abs((booked_rate_ - min_floor) * 0.15)

                if booked_rate_ > max_ceiling:
                    booked_rate_ = max_ceiling - abs((booked_rate_ - max_ceiling) * 0.15)

                # micro jitter
                booked_rate_ += np_rng_.normal(0, 0.003)

                # final safety clamp
                booked_rate_ = np.clip(booked_rate_, 0.0, 0.998)

                return round(booked_rate_ * seat_capacity_)


            def check_weather_cancellation(
                    scheduled_departure, dep_airport_code, distance_km_, weather_df, np_rng_=np_rng
            ):

                if distance_km_ >= 5500:
                    hours_ahead = 4
                elif distance_km_ >= 1900:
                    hours_ahead = 3
                else:
                    hours_ahead = 2

                probs = []

                for h in range(hours_ahead):
                    t = scheduled_departure + pd.Timedelta(hours=h)
                    t = pd.Timestamp(t).floor("h")

                    try:
                        probs.append(weather_df.loc[(dep_airport_code, t), "cancel_prob"])
                    except KeyError:
                        continue

                if not probs:
                    return False

                combined_prob = 1 - float(np.prod([1 - p for p in probs]))

                # compress
                combined_prob = 1 - (1 - combined_prob) ** 0.5

                # global scaling
                combined_prob *= 0.55

                # optional safety cap
                combined_prob = min(combined_prob, 0.15)

                return np_rng_.random() < combined_prob


            def calculate_weather_delay(
                    scheduled_time, airport_code_, distance_km_, weather_df, phase, np_rng_=np_rng
            ):

                combined_prob = 1.0
                best_row = None
                best_prob = -1

                for h in range(2):
                    t = scheduled_time + pd.Timedelta(hours=h)
                    t = pd.Timestamp(t).floor("h")

                    try:
                        r = weather_df.loc[(airport_code_, t), :]
                    except KeyError:
                        continue

                    p = r["delay_prob"]

                    combined_prob *= (1 - p)

                    if p > best_prob:
                        best_prob = p
                        best_row = r

                if best_row is None:
                    return 0

                combined_prob = 1 - combined_prob
                combined_prob *= 0.65

                if phase == "arrival":
                    combined_prob *= 0.75

                condition = best_row["condition"]
                intensity = best_row["intensity"]

                if np_rng_.random() > combined_prob:
                    return 0

                base_delay = {
                    "Clear": 0, "Cloudy": 5, "Rain": 10, "Snow": 20, "Fog": 25,
                    "Thunderstorm": 30, "Blizzard": 60, "Sandstorm": 50,
                    "Dust": 20, "Haze": 10
                }

                delay = base_delay.get(condition, 10)

                # intensity
                intensity_mult = {"light": 0.8, "moderate": 1.0, "heavy": 1.4}
                delay *= intensity_mult.get(intensity, 1.0)

                # distance
                if distance_km_ >= 5500:
                    delay *= 1.15
                elif distance_km_ < 1900:
                    delay *= 0.85

                # skewed randomness
                delay *= (0.5 + np_rng_.beta(2, 4))

                # heavy delay tail
                if phase == "arrival":
                    if np_rng_.random() < 0.07:
                        delay += np_rng_.uniform(60, 200)
                else:
                    if np_rng_.random() < 0.06:
                        delay += np_rng_.uniform(50, 180)

                return int(max(0, delay))


            def calculate_turnaround_minutes(seat_capacity_, distance_km_, np_rng_=np_rng):
                base = 40

                if seat_capacity_ >= 300:
                    base += 20
                elif seat_capacity_ >= 220:
                    base += 15
                elif seat_capacity_ >= 170:
                    base += 10
                elif seat_capacity_ >= 120:
                    base += 5

                if distance_km_ > 8000:
                    base += 10
                elif distance_km_ > 3000:
                    base += 5
                elif distance_km_ < 800:
                    base -= 5

                base += int(np_rng_.integers(-3, 7))

                return max(base, 30)


            if section == "short_medium_a":
                aircraft_ = (
                    pd.concat([df_aircraft_short_a.copy(), df_aircraft_medium_a.copy()]).sort_values("aircraft_id").reset_index(drop=True))
                short_dummies = df_aircraft_short_a["aircraft_id"].tolist()
                medium_dummies = df_aircraft_medium_a["aircraft_id"].tolist()
                routes_ = (
                    pd.concat([df_routes_short.copy(), df_routes_medium.copy()]).sort_values("line_number").reset_index(drop=True))
                initial_locations = initial_loc_short_med_a
                departure_offset = timedelta(hours=1)

            elif section == "short_medium_b":
                aircraft_ = (
                    pd.concat([df_aircraft_short_b.copy(), df_aircraft_medium_b.copy()]).sort_values("aircraft_id").reset_index(drop=True))
                short_dummies = df_aircraft_short_b["aircraft_id"].tolist()
                medium_dummies = df_aircraft_medium_b["aircraft_id"].tolist()
                routes_ = (
                    pd.concat([df_routes_short.copy(), df_routes_medium.copy()]).sort_values("line_number").reset_index(drop=True))
                initial_locations = initial_loc_short_med_b
                departure_offset = timedelta(hours=6.5)

            elif section == "long":
                aircraft_ = df_aircraft_long.copy()
                short_dummies = df_aircraft_short["aircraft_id"].tolist()
                medium_dummies = df_aircraft_medium["aircraft_id"].tolist()
                routes_ = df_routes_long
                initial_locations = initial_locations_long
                departure_offset = timedelta(hours=0)

            else:
                raise ValueError(f"Invalid section: {section}")

            df_weather_multi_index = df_weather.copy()
            df_weather_multi_index.set_index(["airport_code", "observation_time"], inplace=True)

            active_aircraft = aircraft_.copy()

            aircraft_status = {}
            for ac_id, ap in initial_locations.items():
                aircraft_status[ac_id] = {
                    "available_from": date_start,
                    "location": ap,
                    "flight_attempts": 0,
                    "grounded": False
                }

            route_numbers = routes_["line_number"].tolist()

            delay_reasons_dep = ["Technical issue", "Crew delay", "Air traffic control"]
            delay_weights_dep = [0.45, 0.35, 0.2]
            delay_reasons_arr = ["Airspace congestion", "Ground handling"]
            delay_weights_arr = [0.6, 0.4]

            cancellation_reasons = ["Technical failure", "Crew unavailable", "Airport closure"]
            cancellation_weights = [0.6, 0.3, 0.1]

            flight_records = []
            flight_record_keys = ["flight_date", "line_number", "aircraft_id", "bookings_total", "scheduled_departure",
                                  "scheduled_arrival", "actual_departure", "actual_arrival", "is_cancelled", "cancellation_reason",
                                  "delay_reason_dep", "delay_reason_arr", "seat_capacity", "distance_km", "demand_tier", "base_demand_bias"]

            for day_offset in range(sim_days):
                flight_date = date_start + timedelta(days=day_offset)
                day_end = datetime.combine(flight_date.date(), datetime.max.time())
                active_this_day = set()
                route_usage_today = defaultdict(int)
                max_flights_per_day = 8
                total_aircraft_grounded = 0

                for ac_id in aircraft_status:
                    aircraft_status[ac_id]["flight_attempts"] = 0
                    aircraft_status[ac_id]["grounded"] = False

                while total_aircraft_grounded < len(initial_locations) + 1:
                    if total_aircraft_grounded == len(initial_locations):
                        for line_number in route_numbers:
                            if route_usage_today[line_number] < 1:
                                rnd_seconds = int(np_rng.integers(0, int((day_end - flight_date).total_seconds())))
                                scheduled_dep = flight_date + timedelta(seconds=rnd_seconds) + departure_offset
                                distance_km = routes_[routes_["line_number"] == line_number]["distance_km"].values[0]
                                demand_tier = routes_[routes_["line_number"] == line_number][f"demand_tier_{flight_date.year}"].values[0]
                                base_demand_bias = routes_[routes_["line_number"] == line_number][f"base_demand_bias_{flight_date.year}"].values[0]
                                volatility = routes_[routes_["line_number"] == line_number]["volatility"].values[0]
                                seasonality = routes_[routes_["line_number"] == line_number]["season_sensitivity"].values[0]
                                duration = timedelta(hours=distance_km / 900 + np_rng.uniform(0.1, 0.3))
                                scheduled_arr = scheduled_dep + duration

                                if line_number in df_routes_short["line_number"].tolist():
                                    dummy_aircraft = short_dummies
                                elif line_number in df_routes_medium["line_number"].tolist():
                                    dummy_aircraft = medium_dummies
                                else:
                                    dummy_aircraft = df_aircraft_long["aircraft_id"].tolist()

                                ac_id = np_rng.choice(dummy_aircraft)

                                seat_capacity = aircraft_[aircraft_["aircraft_id"] == ac_id]["seat_capacity"].values[0]

                                bookings_total = generate_bookings(
                                    flight_date, demand_tier, seat_capacity, base_demand_bias,
                                    volatility, seasonality
                                )

                                flight_records.append({
                                flight_record_keys[0]: flight_date.date(),
                                flight_record_keys[1]: line_number,
                                flight_record_keys[2]: ac_id,
                                flight_record_keys[3]: bookings_total,
                                flight_record_keys[4]: scheduled_dep,
                                flight_record_keys[5]: scheduled_arr,
                                flight_record_keys[6]: pd.NaT,
                                flight_record_keys[7]: pd.NaT,
                                flight_record_keys[8]: True,
                                flight_record_keys[9]: "Aircraft unavailable",
                                flight_record_keys[10]: None,
                                flight_record_keys[11]: None,
                                flight_record_keys[12]: seat_capacity,
                                flight_record_keys[13]: distance_km,
                                flight_record_keys[14]: demand_tier,
                                flight_record_keys[15]: base_demand_bias,
                            })

                                route_usage_today[line_number] += 1 ###

                        total_aircraft_grounded += 1

                    for ac_id, status in aircraft_status.items():

                        if status["grounded"]:
                            continue

                        if status["available_from"] > day_end:
                            status["grounded"] = True
                            total_aircraft_grounded += 1
                            continue

                        if status["flight_attempts"] == max_flights_per_day:
                            status["grounded"] = True
                            total_aircraft_grounded += 1
                            continue

                        status["available_from"] = max(status["available_from"], datetime.combine(flight_date.date(), datetime.min.time()) + departure_offset)
                        location = status["location"]
                        scheduled = False

                        aircraft_row = active_aircraft[active_aircraft["aircraft_id"] == ac_id]

                        if aircraft_row.empty:
                            print(f"AIRCRAFT {ac_id} IS NOT AT {location}!")
                            status["grounded"] = True
                            total_aircraft_grounded += 1
                            continue

                        aircraft_range = aircraft_row["range_km"].values[0]
                        seat_capacity = aircraft_row["seat_capacity"].values[0]

                        flag = False

                        if aircraft_range >= df_aircraft_long["range_km"].min():
                            eligible_routes = df_routes_long[df_routes_long["departure_airport_code"] == location]
                            flag = True

                        elif aircraft_range >= df_aircraft_medium["range_km"].min() and location in df_routes_medium["departure_airport_code"].tolist():
                            for ln in df_routes_medium["line_number"].tolist():
                                if route_usage_today[ln] == 0:
                                    eligible_routes = df_routes_medium[df_routes_medium["departure_airport_code"] == location]
                                    flag = True
                                    break
                                else:
                                    continue

                        if not flag:
                            eligible_routes = routes_[routes_["departure_airport_code"] == location]
                            eligible_routes = eligible_routes[eligible_routes["distance_km"] <= aircraft_range]

                        # noinspection PyUnboundLocalVariable
                        if eligible_routes.empty:
                            print(f"AIRCRAFT {ac_id} IS STUCK IN {location}!")
                            status["grounded"] = True
                            total_aircraft_grounded += 1
                            continue


                        for _, route_ in eligible_routes.iterrows():
                            line_number = route_["line_number"]
                            if route_usage_today[line_number] == 1:
                                continue
                            distance_km = route_["distance_km"]
                            demand_tier = route_[f"demand_tier_{flight_date.year}"]
                            base_demand_bias = route_[f"base_demand_bias_{flight_date.year}"]
                            volatility = route_["volatility"]
                            seasonality = route_["season_sensitivity"]
                            dest_airport = route_["arrival_airport_code"]

                            duration = timedelta(hours=distance_km / 900 + np_rng.uniform(0.1, 0.3))
                            scheduled_dep = status["available_from"]
                            scheduled_arr = scheduled_dep + duration

                            bookings_total = generate_bookings(
                                    flight_date, demand_tier, seat_capacity, base_demand_bias,
                                    volatility, seasonality
                                )

                            weather_cancellation = check_weather_cancellation(
                                scheduled_departure=scheduled_dep,
                                dep_airport_code=location,
                                distance_km_=distance_km,
                                weather_df=df_weather_multi_index,
                            )

                            if weather_cancellation:
                                is_cancelled = True
                                route_usage_today[line_number] += 1
                                cancellation_reason = "Weather"
                                actual_dep = actual_arr = None
                                delay_reason_dep = delay_reason_arr = None
                                status["location"] = dest_airport
                                status["available_from"] = scheduled_arr + timedelta(minutes=90)
                            else:
                                is_cancelled = np_rng.random() < 0.017
                                cancellation_reason = (np_rng.choice(cancellation_reasons, p=cancellation_weights) if is_cancelled else None)

                                if is_cancelled:
                                    route_usage_today[line_number] += 1
                                    actual_dep = actual_arr = None
                                    delay_reason_dep = delay_reason_arr = None
                                    status["location"] = dest_airport
                                    status["available_from"] = scheduled_arr + timedelta(minutes=90)
                                else:
                                    delay_dep_min = calculate_weather_delay(
                                        scheduled_time=scheduled_dep,
                                        airport_code_=location,
                                        distance_km_=distance_km,
                                        weather_df=df_weather_multi_index,
                                        phase="departure"
                                    )

                                    if delay_dep_min <= 15:
                                        delay_dep_min = max(0, int(np_rng.exponential(scale=9)))
                                        delay_reason_dep = np_rng.choice(delay_reasons_dep, p=delay_weights_dep)[0] if delay_dep_min > 15 else None
                                    else:
                                        delay_reason_dep = "Weather"

                                    scheduled_arr_with_dep_delay = scheduled_arr + timedelta(minutes=delay_dep_min)
                                    delay_arr_min = calculate_weather_delay(
                                        scheduled_time=scheduled_arr_with_dep_delay,
                                        airport_code_=dest_airport,
                                        distance_km_=distance_km,
                                        weather_df=df_weather_multi_index,
                                        phase="arrival"
                                    )

                                    if delay_arr_min <= 15:
                                        delta = int(np_rng.integers(-5, 11))
                                        delay_arr_min = max(0, delay_dep_min + delta)

                                        if delay_dep_min > 15:
                                            delay_reason_arr = "Late departure" if delay_arr_min > 15 else None
                                        else:
                                            delay_reason_arr = np_rng.choice(delay_reasons_arr, p=delay_weights_arr)[0] if delay_arr_min > 15 else None
                                    else:
                                        delay_reason_arr = "Weather"
                                        delay_arr_min = delay_dep_min + delay_arr_min

                                    actual_dep = scheduled_dep + timedelta(minutes=delay_dep_min)
                                    actual_arr = scheduled_arr + timedelta(minutes=delay_arr_min)

                                    turnaround = calculate_turnaround_minutes(seat_capacity, distance_km)
                                    status["available_from"] = actual_arr + timedelta(minutes=turnaround)
                                    status["location"] = dest_airport
                                    route_usage_today[line_number] += 1
                                    status["flight_attempts"] += 1

                            flight_records.append({
                                flight_record_keys[0]: flight_date.date(),
                                flight_record_keys[1]: line_number,
                                flight_record_keys[2]: ac_id,
                                flight_record_keys[3]: bookings_total,
                                flight_record_keys[4]: scheduled_dep,
                                flight_record_keys[5]: scheduled_arr,
                                flight_record_keys[6]: actual_dep,
                                flight_record_keys[7]: actual_arr,
                                flight_record_keys[8]: is_cancelled,
                                flight_record_keys[9]: cancellation_reason,
                                flight_record_keys[10]: delay_reason_dep,
                                flight_record_keys[11]: delay_reason_arr,
                                flight_record_keys[12]: seat_capacity,
                                flight_record_keys[13]: distance_km,
                                flight_record_keys[14]: demand_tier,
                                flight_record_keys[15]: base_demand_bias
                            })

                            active_this_day.add(ac_id)
                            scheduled = True
                            break

                        if not scheduled:
                            status["grounded"] = True
                            total_aircraft_grounded += 1

            flights = pd.DataFrame(flight_records)
            flights["flight_date"] = pd.to_datetime(flights["flight_date"])
            flights["booked_rate_pct"] = flights["bookings_total"] * 100 / flights["seat_capacity"]

            return flights


        def investigate_flights_data(flights):

            print("\n flights diagnostics:\n")

            flights["weekday"] = flights["flight_date"].dt.weekday
            flights["month"] = flights["flight_date"].dt.month
            flights["month_group"] = np.select(
            [flights["flight_date"].dt.month.isin([6, 7, 8, 12]), flights["flight_date"].dt.month.isin([1, 2, 11])],
                ["months 6, 7, 8, 12", "months 1, 2, 11"], default="months other"
            )
            flights["distance_category"] = np.select(
                [flights["distance_km"] < 1900, flights["distance_km"] >= 5500],
                ["short-haul", "long-haul"], default="medium-haul"
            )

            flight_routes = flights.groupby("line_number")["booked_rate_pct"].mean().reset_index()

            booked_rate_bins = [0, 50, 60, 70, 80, 90, 100]
            bin_labels = ["(6) <50%", "(5) 50–60%", "(4) 60–70%", "(3) 70–80%", "(2) 80–90%", "(1) 90–100%"]
            flights["booked_rate_bin"] = pd.cut(flights["booked_rate_pct"], booked_rate_bins, labels=bin_labels)
            flight_routes["booked_rate_bin"] = pd.cut(flight_routes["booked_rate_pct"], booked_rate_bins, labels=bin_labels)

            dep_dly_weather = flights[flights["delay_reason_dep"] == "Weather"]
            arr_dly_weather = flights[flights["delay_reason_arr"] == "Weather"]

            for year in flights["flight_date"].dt.year.unique():
                print(f"avg booked rate year: {year}:",
                      flights[flights["flight_date"].dt.year == year]["booked_rate_pct"].mean()
                )

            for test in {
                "flight share (pct) by booked rate bin":
                    flights["booked_rate_bin"].value_counts(normalize=True).sort_index() * 100,
                "route share (pct averages) by booked rate bin":
                    flight_routes["booked_rate_bin"].value_counts(normalize=True).sort_index() * 100,
                "booked rate by demand_tier":
                    flights.groupby("demand_tier", observed=False)["booked_rate_pct"].describe(),
                "booked rate by distance category":
                    flights.groupby("distance_category")["booked_rate_pct"].describe(),
                "booked rate by month_group":
                    flights.groupby("month_group")["booked_rate_pct"].describe(),
                "avg booked rate by month":
                    flights.groupby("month")["booked_rate_pct"].mean(),
                "booked rate by weekday":
                    flights.groupby("weekday")["booked_rate_pct"].describe(),

                "num flights with bookings exceeding capacity":
                    flights[flights["bookings_total"] > flights["seat_capacity"]].shape[0],

                "num flights without available aircraft": flights[flights["cancellation_reason"] == "Aircraft unavailable"].shape[0],
                "daily flights per aircraft": flights["aircraft_id"].value_counts() / flights["flight_date"].nunique(),

                "pct flights cancelled": flights[flights["is_cancelled"] == True].shape[0] / flights.shape[0] * 100,
                "pct of flights cancelled by reason": flights["cancellation_reason"].value_counts() / flights.shape[0] * 100,

                "pct departures delayed": flights[flights["delay_reason_dep"].notna()].shape[0] / flights.shape[0] * 100,
                "pct of departures delayed by reason": flights["delay_reason_dep"].value_counts() / flights.shape[0] * 100,
                "min weather delay minutes for flights with delay_reason_dep":
                    ((dep_dly_weather["actual_departure"] - dep_dly_weather["scheduled_departure"]).dt.total_seconds() / 60).min(),
                "avg weather delay minutes for flights with delay_reason_dep":
                    ((dep_dly_weather["actual_departure"] - dep_dly_weather["scheduled_departure"]).dt.total_seconds() / 60).mean(),
                "max weather delay minutes for flights with delay_reason_dep":
                    ((dep_dly_weather["actual_departure"] - dep_dly_weather["scheduled_departure"]).dt.total_seconds() / 60).max(),

                "pct of arrivals delayed by reason": flights["delay_reason_arr"].value_counts() / flights.shape[0] * 100,
                "min weather delay minutes for flights with delay_reason_arr":
                    ((arr_dly_weather["actual_arrival"] - arr_dly_weather["scheduled_arrival"]).dt.total_seconds() / 60).min(),
                "avg weather delay minutes for flights with delay_reason_arr":
                    ((arr_dly_weather["actual_arrival"] - arr_dly_weather["scheduled_arrival"]).dt.total_seconds() / 60).mean(),
                "max weather delay minutes for flights with delay_reason_arr":
                    ((arr_dly_weather["actual_arrival"] - arr_dly_weather["scheduled_arrival"]).dt.total_seconds() / 60).max(),
            }.items():
                print(test[0], "\n", test[1], "\n")


        flights_short_medium_a = create_flights("short_medium_a", rng_np_main)
        flights_short_medium_b = create_flights("short_medium_b", rng_np_alt)
        flights_long = create_flights("long", rng_np_main)
        flights_df = pd.concat([flights_short_medium_a, flights_short_medium_b, flights_long]).reset_index(drop=True)
        flights_df.insert(0, "flight_number", "FL1" + flights_df.index.astype(str).str.zfill(len(str(len(flights_df)))))

        if run_diagnostics:
            investigate_flights_data(flights_df.copy())

        return flights_df


    # --- Frequent flyer status and class discount adjustment lookup tables ---

    frequent_flyer_specs = [
        {"frequent_flyer_status_id": "(0) No Status", "frequent_flyer_status": "No status", "min_flights_yearly": 0,
         "base_discount": 0.0},
        {"frequent_flyer_status_id": "(1) Silver", "frequent_flyer_status": "Silver", "min_flights_yearly": 3,
         "base_discount": 0.05},
        {"frequent_flyer_status_id": "(2) Gold", "frequent_flyer_status": "Gold", "min_flights_yearly": 6,
         "base_discount": 0.10},
        {"frequent_flyer_status_id": "(3) Platinum", "frequent_flyer_status": "Platinum", "min_flights_yearly": 18,
         "base_discount": 0.18},
    ]
    class_discount_modifiers = [
        {"class_id": "(01) Economy", "class_name": "Economy", "discount_factor": 1,
         "description": "full frequent flyer discount applies"},
        {"class_id": "(02) Business", "class_name": "Business", "discount_factor": 0.7,
         "description": "reduced frequent flyer discount in premium cabin"},
        {"class_id": "(03) First", "class_name": "First", "discount_factor": 0.5,
         "description": "heavily reduced frequent flyer discount in premium cabin"}
    ]


    # --- Flight capacity by class and class cost share tables ---

    def create_class_cap_and_cost_dfs(flights_df, class_discount_adjustments_df, np_rng=np_rng_main, run_diagnostics=show_diagnostics):

        def allocate_class_capacities(total_capacity_, route_distance_km,
            class_discount_adjustments=class_discount_adjustments_df
        ):

            class_ids_ = class_discount_adjustments["class_id"].tolist()  # Economy, Business, First

            if total_capacity_ < 120:
                return {class_ids_[0]: total_capacity, class_ids_[1]: 0, class_ids_[2]: 0}
            elif route_distance_km < 2000:
                econ = int(total_capacity_ * 0.9)
                bus = total_capacity_ - econ
                return {class_ids_[0]: econ, class_ids_[1]: bus, class_ids_[2]: 0}
            elif total_capacity_ >= 250:
                econ = int(total_capacity_ * 0.75)
                bus = int(total_capacity_ * 0.2)
                first = total_capacity_ - econ - bus
                return {class_ids_[0]: econ, class_ids_[1]: bus, class_ids_[2]: first}
            else:
                econ = int(total_capacity_ * 0.85)
                bus = total_capacity_ - econ
                return {class_ids_[0]: econ, class_ids_[1]: bus, class_ids_[2]: 0}


        def allocate_bookings_by_class(class_capacities, total_passengers_,
            class_discount_adjustments=class_discount_adjustments_df, np_rng_=np_rng
        ):

            class_ids_ = class_discount_adjustments["class_id"].tolist()  # Economy, Business, First
            caps = class_capacities.copy()
            p_first = min(np_rng_.binomial(caps.get(class_ids_[2], 0), 0.9), caps.get(class_ids_[2], 0))
            p_business = min(np_rng_.binomial(caps.get(class_ids_[1], 0), 0.85), caps.get(class_ids_[1], 0))
            allocations = {class_ids_[2]: p_first, class_ids_[1]: p_business, class_ids_[0]: 0}
            allocated = p_first + p_business
            remaining = total_passengers_ - allocated
            eco_cap = caps.get(class_ids_[0], 0)
            p_economy = min(remaining, eco_cap)
            allocations[class_ids_[0]] = p_economy
            remaining -= p_economy

            if remaining > 0:
                for cls_ in [class_ids_[0], class_ids_[1], class_ids_[2]]:  # Economy-first fill
                    space = caps[cls_] - allocations[cls_]
                    add = min(space, remaining)
                    allocations[cls_] += add
                    remaining -= add
                    if remaining == 0:
                        break

            total_assigned = sum(allocations.values())
            overflow = total_assigned - total_passengers_
            if overflow > 0:
                for cls_ in [class_ids_[2], class_ids_[1], class_ids_[0]]:
                    if allocations[cls_] > 0:
                        reduction = int(round((allocations[cls_] / total_assigned) * overflow))
                        allocations[cls_] = max(0, allocations[cls_] - reduction)
                diff = sum(allocations.values()) - total_passengers_
                if diff > 0:
                    for cls_ in sorted(allocations, key=lambda x: -allocations[x]):
                        while allocations[cls_] > 0 and diff > 0:
                            allocations[cls_] -= 1
                            diff -= 1
                            if diff == 0:
                                break

            return allocations


        def compute_cost_shares(class_caps_, cost_weights_):
            weighted = {cls_: seats * cost_weights_[cls_] for cls_, seats in class_caps_.items() if seats > 0}
            total_weight = sum(weighted.values())
            return {cls_: round(weight / total_weight, 2) for cls_, weight in weighted.items()}


        class_ids = class_discount_adjustments_df["class_id"].tolist()  # Economy, Business, First
        flight_class_records = []

        cost_weights = {class_ids[0]: 1.0, class_ids[1]: 2.25, class_ids[2]: 3.5}
        cost_share_records = []

        for _, row in flights_df.iterrows():
            flight_number = row["flight_number"]
            flight_date = row["flight_date"]
            total_bookings = row["bookings_total"]
            route_distance = row["distance_km"]
            total_capacity = row["seat_capacity"]

            class_caps = allocate_class_capacities(total_capacity, route_distance)
            class_passengers = allocate_bookings_by_class(class_caps, total_bookings)
            cost_shares = compute_cost_shares(class_caps, cost_weights)

            for cls in class_caps:
                flight_class_records.append({
                    "flight_number": flight_number,
                    "flight_date": flight_date,
                    "class_id": cls,
                    "capacity": class_caps[cls],
                    "class_bookings": class_passengers[cls], })

                if class_caps[cls] > 0:
                    cost_share_records.append({
                        "flight_number": flight_number,
                        "flight_date": flight_date,
                        "class_id": cls,
                        "cost_share": cost_shares[cls]})

        flight_capacity_by_class_df = pd.DataFrame(flight_class_records)

        flight_class_cost_shares_df = pd.DataFrame(cost_share_records)

        classes = [
            flight_class_cost_shares_df["class_id"].str.lower().str.contains("economy"),
            flight_class_cost_shares_df["class_id"].str.lower().str.contains("business"),
            flight_class_cost_shares_df["class_id"].str.lower().str.contains("first"),
        ]
        suffixes = ["1E", "2B", "3F"]

        flight_class_cost_shares_df.insert(
            0, "cost_share_id", "CS_" + flight_class_cost_shares_df["flight_number"].str[2:] +
                                "_" + np.select(classes, suffixes, default=flight_class_cost_shares_df.index.astype(str))
        )

        if run_diagnostics:
            print("\nflight_capacity_by_class diagnostics:\n")
            capacity_grp = flight_capacity_by_class_df.groupby(["flight_number", "flight_date"])[
                "class_bookings"].sum().reset_index()
            comparison = flights_df[["flight_number", "flight_date", "bookings_total"]].merge(capacity_grp,
                         on=["flight_number", "flight_date"])
            for test in {
                "rows with class bookings exceeding capacity":
                    flight_capacity_by_class_df.query("capacity < class_bookings").shape[0],
                "for each flight sum of class bookings matches total bookings in flights":
                    comparison.shape[0] == flights_df.shape[0],
                "non-matching rows": comparison.query("bookings_total != class_bookings").shape[0]
            }.items():
                print(test[0], "\n", test[1], "\n")

        return flight_capacity_by_class_df, flight_class_cost_shares_df


    # --- Base customers and bookings tables, traveller type lookup table ---

    # Created with assistance from Claude Sonnet 4.6
    def create_bookings_and_customers_dfs(
        flight_capacity_by_class_df, frequent_flyer_discounts_df,
        np_rng=np_rng_main, run_diagnostics=show_diagnostics,
    ):

        """
        Function optimized for simulation duration of one to three years.
        Path for creating simulation snapshots with duration < 6 months included
        with separate flyer status assignment logic.
        """

        # -------------------------------------------------------------------------
        # TRAVELLER TYPE LOOKUP TABLE
        # -------------------------------------------------------------------------

        traveller_type_lookup_df = pd.DataFrame([
            {"traveller_type_id": "(01) Leisure", "traveller_type": "Leisure",
             "annual_bookings_expd_min": 1, "annual_bookings_expd_max": 3},
            {"traveller_type_id": "(02) Family", "traveller_type": "Family",
             "annual_bookings_expd_min": 1, "annual_bookings_expd_max": 3},
            {"traveller_type_id": "(03) Corporate", "traveller_type": "Corporate",
             "annual_bookings_expd_min": 6, "annual_bookings_expd_max": 13},
            {"traveller_type_id": "(04) Road warrior", "traveller_type": "Road warrior",
             "annual_bookings_expd_min": 20, "annual_bookings_expd_max": 40},
        ])

        ttype_ids = traveller_type_lookup_df["traveller_type_id"].values
        ttype_mins = traveller_type_lookup_df["annual_bookings_expd_min"].values
        ttype_maxs = traveller_type_lookup_df["annual_bookings_expd_max"].values

        # -------------------------------------------------------------------------
        # SHARED DISTRIBUTIONS
        # -------------------------------------------------------------------------

        gender_dist = {"Female": 0.48, "Male": 0.52}

        age_dist = {
            "AG1": 0.12, "AG2": 0.23, "AG3": 0.22, "AG4": 0.27, "AG5": 0.16
        }

        # Base class preference per traveller type [Economy, Business, First]
        traveller_type_class_prefs = {
            ttype_ids[0]: np.array([0.96, 0.04, 0.00]),
            ttype_ids[1]: np.array([0.98, 0.02, 0.00]),
            ttype_ids[2]: np.array([0.65, 0.32, 0.03]),
            ttype_ids[3]: np.array([0.35, 0.50, 0.15]),
        }

        # Traveller type probabilities conditioned on gender and age group
        # Columns: Leisure, Family, Corporate, Road warrior
        traveller_type_dist_matrix = {
            ("Female", "AG1"): [0.70, 0.10, 0.18, 0.02],
            ("Female", "AG2"): [0.54, 0.22, 0.20, 0.04],
            ("Female", "AG3"): [0.50, 0.19, 0.24, 0.07],
            ("Female", "AG4"): [0.48, 0.16, 0.27, 0.09],
            ("Female", "AG5"): [0.65, 0.12, 0.18, 0.05],
            ("Male",   "AG1"): [0.68, 0.08, 0.20, 0.04],
            ("Male",   "AG2"): [0.55, 0.18, 0.22, 0.05],
            ("Male",   "AG3"): [0.47, 0.18, 0.27, 0.08],
            ("Male",   "AG4"): [0.45, 0.15, 0.29, 0.11],
            ("Male",   "AG5"): [0.60, 0.12, 0.22, 0.06],
        }

        age_groups      = list(age_dist.keys())
        genders         = list(gender_dist.keys())
        traveller_types = list(traveller_type_class_prefs.keys())
        class_ids       = sorted(flight_capacity_by_class_df["class_id"].unique())
        tier_ids        = frequent_flyer_discounts_df["frequent_flyer_status_id"].values

        total_bookings = int(flight_capacity_by_class_df["class_bookings"].sum())
        n_unique_dates = flight_capacity_by_class_df["flight_date"].nunique()
        scale_factor   = n_unique_dates / 365
        is_snapshot    = scale_factor < 0.5

        # -------------------------------------------------------------------------
        # SHARED POOL GENERATION
        # -------------------------------------------------------------------------

        def generate_pool(n_pool):
            gender_arr_ = np_rng.choice(genders,    size=n_pool, p=list(gender_dist.values()))
            age_arr_    = np_rng.choice(age_groups, size=n_pool, p=list(age_dist.values()))
            ttype_arr_  = np.empty(n_pool, dtype=object)
            for (gender, age), probs_tt in traveller_type_dist_matrix.items():
                mask = (gender_arr_ == gender) & (age_arr_ == age)
                n = mask.sum()
                if n > 0:
                    ttype_arr_[mask] = np_rng.choice(traveller_types, size=n, p=probs_tt)
            return gender_arr_, age_arr_, ttype_arr_

        # -------------------------------------------------------------------------
        # SHARED SEAT ASSIGNMENT
        # -------------------------------------------------------------------------

        def assign_seats(gender_arr_, age_arr_, ttype_arr_, ff_status_, booking_counts_):

            n_customers_ = len(gender_arr_)

            ttype_idx_  = pd.Categorical(ttype_arr_, categories=traveller_types).codes
            base_probs_ = np.array([traveller_type_class_prefs[t] for t in traveller_types])[ttype_idx_]
            probs_      = base_probs_ / base_probs_.sum(axis=1, keepdims=True)

            flights_ = (
                flight_capacity_by_class_df[flight_capacity_by_class_df["class_bookings"] > 0]
                .copy().reset_index(drop=True)
            )

            class_to_idx_ = {c: i for i, c in enumerate(class_ids)}

            repeat_list_ = np.repeat(np.arange(n_customers_, dtype=np.int32), booking_counts_)
            np_rng.shuffle(repeat_list_)

            def build_class_queue(class_col):
                affinity      = probs_[repeat_list_, class_col]
                affinity_norm = (affinity - affinity.min()) / (affinity.max() - affinity.min())
                # Noise ratio controls correlation strength vs. demographic spread:
                    # lower = stronger correlations but more concentration
                    # higher = more spread but weaker correlations
                noise  = np_rng.uniform(0, 5.0, len(repeat_list_))
                scores = affinity_norm + noise
                return repeat_list_[np.argsort(-scores)].copy()

            class_queues_   = {c: build_class_queue(i) for c, i in class_to_idx_.items()}
            queue_pointers_ = {c: 0 for c in class_ids}

            all_rows_          = []
            booking_id_        = 0
            booked_per_flight_ = {}
            scan_limit         = 200 if is_snapshot else 50

            for flight_number, flight_date, class_id, _, n_seats in flights_.itertuples(index=False):

                n_seats_   = int(n_seats)
                queue_     = class_queues_[class_id]
                ptr_       = queue_pointers_[class_id]
                booked_    = booked_per_flight_.get(flight_number, set())
                chosen_    = []
                checked_   = 0
                max_check_ = min(n_seats_ * scan_limit, len(queue_) - ptr_)

                while len(chosen_) < n_seats_ and checked_ < max_check_:
                    if ptr_ >= len(queue_):
                        break
                    cust_id_ = int(queue_[ptr_])
                    ptr_    += 1
                    checked_ += 1
                    if cust_id_ not in booked_:
                        chosen_.append(cust_id_)
                        booked_.add(cust_id_)

                queue_pointers_[class_id]          = ptr_
                booked_per_flight_[flight_number]  = booked_

                for cust_id_ in chosen_:
                    all_rows_.append((booking_id_, cust_id_, flight_number, flight_date, class_id))
                    booking_id_ += 1

            customers = pd.DataFrame({
                "customer_id":              np.arange(n_customers_),
                "gender":                   gender_arr_,
                "age_group":                age_arr_,
                "traveller_type_id":           ttype_arr_,
                "frequent_flyer_status_id": ff_status_,
            })

            bookings = pd.DataFrame(
                all_rows_,
                columns=["booking_id", "customer_id", "flight_number", "flight_date", "class_id"]
            )
            bookings = bookings.merge(
                customers[["customer_id", "frequent_flyer_status_id"]],
                on="customer_id", how="left"
            )

            return customers, bookings

        # -------------------------------------------------------------------------
        # SNAPSHOT PATH  (< ~6 months)
        # -------------------------------------------------------------------------

        def run_snapshot():

            # Flyer status assigned directly from traveller_type
            traveller_type_status_prefs = {
                #         No Status Silver Gold Platinum
                ttype_ids[0]: [0.92, 0.06, 0.02, 0.00],
                ttype_ids[1]: [0.92, 0.06, 0.02, 0.00],
                ttype_ids[2]: [0.60, 0.27, 0.11, 0.02],
                ttype_ids[3]: [0.12, 0.28, 0.38, 0.22],
            }

            # Snapshot booking counts per traveller type
            traveller_type_booking_ranges = {
                ttype_ids[0]: (1, 1),
                ttype_ids[1]: (1, 1),
                ttype_ids[2]: (1, 2),
                ttype_ids[3]: (1, 3),
            }

            # Slight oversize — trimmed after seat assignment
            n_pool = int(total_bookings * 1.1)
            gender_arr, age_arr, ttype_arr = generate_pool(n_pool)

            ff_status = np.empty(n_pool, dtype=object)
            for ttype, status_probs in traveller_type_status_prefs.items():
                mask = ttype_arr == ttype
                n = mask.sum()
                if n > 0:
                    ff_status[mask] = np_rng.choice(tier_ids, size=n, p=status_probs)

            booking_counts = np.zeros(n_pool, dtype=np.int32)
            for ttype, (lo, hi) in traveller_type_booking_ranges.items():
                mask = ttype_arr == ttype
                booking_counts[mask] = np_rng.integers(lo, hi + 1, mask.sum())

            booking_counts = np.minimum(booking_counts, n_unique_dates)
            scale = total_bookings / booking_counts.sum()
            booking_counts = np.maximum(1, np.floor(booking_counts * scale).astype(np.int32))
            diff = total_bookings - booking_counts.sum()
            idx  = np_rng.choice(n_pool, size=abs(int(diff)), replace=False)
            booking_counts[idx] += np.sign(diff).astype(np.int32)

            customers, bookings = assign_seats(
                gender_arr, age_arr, ttype_arr, ff_status, booking_counts
            )

            # Trim to customers who actually appear in bookings
            active_ids = bookings["customer_id"].unique()
            customers = customers[
                customers["customer_id"].isin(active_ids)
            ].reset_index(drop=True)

            return customers, bookings

        # -------------------------------------------------------------------------
        # FULL-LENGTH PATH  (>= ~6 months)
        # -------------------------------------------------------------------------

        def run_full_length():

            # Annual booking ranges — scaled to simulation period
            traveller_type_booking_ranges = {
                ttype_ids[0]: (ttype_mins[0], ttype_maxs[0]),
                ttype_ids[1]: (ttype_mins[1], ttype_maxs[1]),
                ttype_ids[2]: (ttype_mins[2], ttype_maxs[2]),
                ttype_ids[3]: (ttype_mins[3], ttype_maxs[3]),
            }

            n_pool = int(total_bookings / (3.0 * scale_factor))
            gender_arr, age_arr, ttype_arr = generate_pool(n_pool)

            booking_counts = np.zeros(n_pool, dtype=np.int32)
            for ttype, (lo, hi) in traveller_type_booking_ranges.items():
                mask      = ttype_arr == ttype
                scaled_lo = max(1, round(lo * scale_factor))
                scaled_hi = max(scaled_lo + 1, round(hi * scale_factor))
                booking_counts[mask] = np_rng.integers(scaled_lo, scaled_hi + 1, mask.sum())

            booking_counts = np.minimum(booking_counts, n_unique_dates)
            scale = total_bookings / booking_counts.sum()
            booking_counts = np.maximum(1, np.floor(booking_counts * scale).astype(np.int32))
            diff = total_bookings - booking_counts.sum()
            idx  = np_rng.choice(n_pool, size=abs(int(diff)), replace=False)
            booking_counts[idx] += np.sign(diff).astype(np.int32)

            scaled_thresholds = frequent_flyer_discounts_df["min_flights_yearly"].values * scale_factor
            ff_status = np.select(
                [booking_counts >= scaled_thresholds[3],
                 booking_counts >= scaled_thresholds[2],
                 booking_counts >= scaled_thresholds[1]],
                [tier_ids[3], tier_ids[2], tier_ids[1]],
                default=tier_ids[0]
            )

            return assign_seats(gender_arr, age_arr, ttype_arr, ff_status, booking_counts)

        # -------------------------------------------------------------------------
        # DIAGNOSTICS
        # -------------------------------------------------------------------------

        def investigate_customer_and_booking_data(customers, bookings, flight_capacity_by_class):

            bookings_per_customer = bookings.groupby("customer_id").size()
            customers_by_date = bookings.groupby("flight_date")["customer_id"].value_counts().reset_index()

            tests = {
                "num customers": customers.shape[0],
                "num bookings": bookings.shape[0],
                "customer ids unique": customers["customer_id"].value_counts().sum() == len(customers),
                "rows without flight number:": bookings.flight_number.isna().sum(),
                "original class assignments retained":
                    flight_capacity_by_class[flight_capacity_by_class["class_bookings"] != 0]["class_bookings"].tolist() ==
                    bookings.groupby(["flight_number", "class_id"])["customer_id"].count().reset_index()["customer_id"].tolist(),
                "missing bookings (sum of class_bookings minus len bookings)":
                    flight_capacity_by_class.class_bookings.sum() - len(bookings),
                "no customer used more than once per flight":
                    (bookings.groupby("flight_number")["customer_id"].value_counts() > 1).sum() == 0,
                "num customers used more than once per flight date":
                    (bookings.groupby("flight_date")["customer_id"].value_counts() > 1).sum(),
                "max occurrences of same customer per flight date":
                    customers_by_date["count"].max(),
                "mean occurrences of same customer per flight date if more than 1":
                    customers_by_date[customers_by_date["count"] > 1]["count"].mean(),

                "median bookings per customer": bookings_per_customer.median(),
                "mean bookings per customer": bookings_per_customer.mean(),
                "top 5% of customers account for the following share of bookings":
                    bookings_per_customer.nlargest(int(len(bookings_per_customer)*0.05)).sum() / len(bookings) * 100,

                "gender distribution overall:": customers["gender"].value_counts(normalize=True) * 100,
                "age group distribution overall:": (customers["age_group"].value_counts(normalize=True) * 100).sort_index(),
                "flyer status distribution overall:":
                    (customers["frequent_flyer_status_id"].value_counts(normalize=True) * 100).sort_index(),

                "gender distribution by cabin class:":
                    bookings.groupby("class_id")["gender"].value_counts(normalize=True).sort_index() * 100,
                "cabin class distribution by gender:":
                    bookings.groupby("gender")["class_id"].value_counts(normalize=True).sort_index() * 100,
                "age group distribution by cabin class:":
                    bookings.groupby("class_id")["age_group"].value_counts(normalize=True).sort_index() * 100,
                "cabin class distribution by age group":
                    bookings.groupby("age_group")["class_id"].value_counts(normalize=True).sort_index() * 100,

                "gender distribution by flyer status:":
                    customers.groupby("frequent_flyer_status_id")["gender"].value_counts(normalize=True).sort_index() * 100,
                "flyer status distribution by gender:":
                    customers.groupby("gender")["frequent_flyer_status_id"].value_counts(normalize=True).sort_index() * 100,
                "age group distribution by flyer status:":
                    customers.groupby("frequent_flyer_status_id")["age_group"].value_counts(normalize=True).sort_index() * 100,
                "flyer status distribution by age group":
                    customers.groupby("age_group")["frequent_flyer_status_id"].value_counts(normalize=True).sort_index() * 100,

                "traveller type distribution by gender:":
                    customers.groupby("gender")["traveller_type_id"].value_counts(normalize=True).sort_index() * 100,
                "gender distribution by traveller type:":
                    customers.groupby("traveller_type_id")["gender"].value_counts(normalize=True).sort_index() * 100,
                "traveller type distribution by age group":
                    customers.groupby("age_group")["traveller_type_id"].value_counts(normalize=True).sort_index() * 100,
                "age group distribution by traveller type:":
                    customers.groupby("traveller_type_id")["age_group"].value_counts(normalize=True).sort_index() * 100,
            }

            for test in tests.items():
                print(test[0], "\n", test[1], "\n")

        # -------------------------------------------------------------------------
        # DISPATCH
        # -------------------------------------------------------------------------

        if is_snapshot:
            customers_df, bookings_df = run_snapshot()
        else:
            customers_df, bookings_df = run_full_length()

        if run_diagnostics:
            diagnosis_bookings = (
                bookings_df.merge(customers_df[["customer_id", "traveller_type_id", "gender", "age_group"]], on="customer_id", how="left")
            )
            investigate_customer_and_booking_data(customers_df, diagnosis_bookings, flight_capacity_by_class_df)

        return customers_df, bookings_df, traveller_type_lookup_df


    # --- Customer attributes ---

    def assign_customer_attributes(customers_df, routes, airports, start_date, np_rng=np_rng_main):

        locale_map = {
            # Core Europe
            "Germany": "de_DE",
            "Austria": "de_AT",
            "Switzerland": "de_CH",
            "United Kingdom": "en_GB",
            "France": "fr_FR",
            "Belgium": "fr_BE",
            "Netherlands": "nl_NL",
            "Spain": "es_ES",
            "Portugal": "pt_PT",
            "Italy": "it_IT",
            "Greece": "el_GR",
            "Poland": "pl_PL",
            "Sweden": "sv_SE",
            "Denmark": "da_DK",
            "Norway": "no_NO",
            "Finland": "fi_FI",
            "Cyprus": "el_GR",

            # Middle East / North Africa
            "Turkey": "tr_TR",
            "Israel": "en_US",
            "Jordan": "en_US",
            "Lebanon": "en_US",
            "Egypt": "ar_EG",
            "Morocco": "fr_FR",
            "Tunisia": "fr_FR",
            "Algeria": "fr_FR",
            "United Arab Emirates": "en_GB",
            "Qatar": "en_GB",

            # Americas
            "United States": "en_US",
            "Canada": "en_CA",
            "Brazil": "pt_BR",

            # Asia-Pacific
            "India": "en_IN",
            "China": "zh_CN",
            "Japan": "ja_JP",
            "South Korea": "ko_KR",
            "Singapore": "en_GB",
            "Australia": "en_AU",
            "South Africa": "en_GB"
        }


        def assign_date_of_birth(customers, reference_date=start_date, np_rng_=np_rng):

            # define age ranges
            age_ranges = {
                "AG1": (18, 24),
                "AG2": (25, 34),
                "AG3": (35, 44),
                "AG4": (45, 60),
                "AG5": (61, 85)
            }

            # map age group to min/max arrays
            age_min = customers["age_group"].map(lambda x: age_ranges[x][0]).to_numpy()
            age_max = customers["age_group"].map(lambda x: age_ranges[x][1]).to_numpy()

            # sample integer ages
            ages = np_rng_.integers(age_min, age_max + 1)

            # convert age -> date of birth
            # randomize the birthday within the year (avoids everyone born Jan 1)
            random_days = np_rng_.integers(0, 365, size=len(customers))

            dob_ = reference_date - pd.to_timedelta(ages * 365.25 + random_days, unit="D")
            customers["date_of_birth"] = dob_

            return customers


        def build_name_pools(locale_fakers_, pool_size=5000, np_rng_=np_rng):

            name_pools_ = {}

            for locale_, faker in locale_fakers_.items():
                faker_seed = int(np_rng_.integers(0, 2**32 - 1))
                faker.seed_instance(faker_seed)

                first_male = faker.first_name_male
                first_female = faker.first_name_female
                last_name = faker.last_name

                male_first = [unidecode(first_male()).strip() for _ in range(pool_size)]
                female_first = [unidecode(first_female()).strip() for _ in range(pool_size)]
                last = [unidecode(last_name()).strip() for _ in range(10000)]

                name_pools_[locale_] = {
                    "male_first": np.array(male_first),
                    "female_first": np.array(female_first),
                    "last": np.array(last)
                }

            return name_pools_


        def sanitize(s):
            return re.sub(r"[^a-z0-9]", "", s.lower())

        # DOB
        customers_df = assign_date_of_birth(customers_df)

        # nationality
        routes_airports_merged = (
            routes[["departure_airport_code", "flights_per_day"]]
            .merge(airports[["airport_code", "country"]], left_on="departure_airport_code", right_on="airport_code")
        )

        route_weights = routes_airports_merged.groupby("country")["flights_per_day"].sum()
        route_weights = round(np.sqrt(route_weights), 1)  # soften dominance

        # large diaspora / travel corridors, global heavy hitters, aviation hubs / high travel intensity
        manual_boost = {"India": 6, "China": 5, "United States": 5, "Brazil": 3,  "United Arab Emirates": 3, "Qatar": 2,
                        "Singapore": 2, "Turkey": 4, "Morocco": 3, "Egypt": 3}

        country_weights_dict = route_weights.to_dict()

        for c, w in manual_boost.items():
            country_weights_dict[c] = country_weights_dict.get(c, 0) + w

        countries = np.array(list(country_weights_dict.keys()))
        weights = np.array(list(country_weights_dict.values()))
        probs = weights / weights.sum()

        customers_df["nationality"] = np_rng.choice(countries, size=len(customers_df), p=probs)
        customers_df["locale"] = customers_df["nationality"].map(locale_map).fillna("en_US")

        # faker setup
        locale_fakers = {loc: Faker(loc) for loc in set(locale_map.values())}

        # build name pools
        name_pools = build_name_pools(locale_fakers)

        # Generate fake data

        genders = customers_df["gender"].to_numpy()
        n = len(customers_df)

        domains = np_rng.choice(
            ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"],
            size=n,
            p=[0.5, 0.2, 0.2, 0.1]
        )

        phones = np_rng.integers(10**9, 10**10, size=n).astype(str)

        first_names = np.empty(n, dtype=object)
        last_names = np.empty(n, dtype=object)

        for locale, idx in customers_df.groupby("locale").groups.items():

            pool = name_pools.get(locale, name_pools["en_US"])
            size = len(idx)

            fi = np_rng.integers(0, len(pool["male_first"]), size=size)
            li = np_rng.integers(0, len(pool["last"]), size=size)

            male_mask = (genders[idx] == "male")

            first_names[idx] = np.where(male_mask, pool["male_first"][fi], pool["female_first"][fi])

            last_names[idx] = pool["last"][li]

        # assemble
        full_names = [f"{f} {l}" for f, l in zip(first_names, last_names)]

        base_emails = [f"{sanitize(f)}.{sanitize(l)}@{d}"
                      for f, l, d in zip(first_names, last_names, domains)]

        # find duplicated emails and resolve with a simple incrementing counter per base
        seen = defaultdict(int)
        emails = []
        for base in base_emails:
            count = seen[base]
            seen[base] += 1
            if count == 0:
                emails.append(base)
            else:
                name, domain = base.split("@")
                emails.append(f"{name}{count}@{domain}")

        customers_df["full_name"] = full_names
        customers_df["email"] = emails
        customers_df["phone"] = phones

        return customers_df[[
            "customer_id", "full_name", "email", "phone", "date_of_birth", "nationality", "gender",
            "frequent_flyer_status_id", "traveller_type_id", "age_group"
        ]]


    # --- Enrich bookings-table: booking times and check-ins ---

    def add_columns_to_bookings_df(
            bookings_df, customers_df, flights_df, traveller_type_lookup_df, run_diagnostics=show_diagnostics, np_rng=np_rng_main
    ):

        def assign_booking_times(bookings_merged, traveller_type_lookup, np_rng_=np_rng):

            """
            Booking lead times are affected by traveller type (segment), age group, and flight distance.
            Nationality affects booking time of day.
            """

            country_to_cluster = {
                # Central/Northern Europe
                "Germany": "central_europe", "Austria": "central_europe", "Switzerland": "central_europe",
                "Netherlands": "central_europe", "Belgium": "central_europe",
                "Denmark": "central_europe", "Sweden": "central_europe",
                "Norway": "central_europe", "Finland": "central_europe",
                "Poland": "central_europe",

                # Southern Europe
                "Spain": "southern_europe", "Italy": "southern_europe",
                "Portugal": "southern_europe", "Greece": "southern_europe",
                "Cyprus": "southern_europe",

                # Anglosphere
                "United States": "anglosphere", "Canada": "anglosphere",
                "United Kingdom": "anglosphere", "Australia": "anglosphere",

                # MENA
                "United Arab Emirates": "mena", "Qatar": "mena",
                "Israel": "mena", "Jordan": "mena", "Lebanon": "mena",
                "Egypt": "mena", "Morocco": "mena",
                "Algeria": "mena", "Tunisia": "mena",
                "Turkey": "mena",

                # South Asia
                "India": "south_asia", "Singapore": "south_asia",

                # East Asia
                "Japan": "east_asia", "South Korea": "east_asia", "China": "east_asia",

                # LATAM
                "Brazil": "latam"
            }

            time_profiles = {
                "central_europe": [0.45, 0.30, 0.20, 0.05],
                "southern_europe": [0.30, 0.30, 0.30, 0.10],
                "anglosphere": [0.30, 0.30, 0.30, 0.10],
                "mena": [0.20, 0.35, 0.30, 0.15],
                "south_asia": [0.25, 0.30, 0.35, 0.10],
                "east_asia": [0.35, 0.30, 0.25, 0.10],
                "latam": [0.25, 0.30, 0.35, 0.10],
                "default": [0.25, 0.25, 0.25, 0.25]
            }

            n = len(bookings_merged)
            ttype_ids = traveller_type_lookup["traveller_type_id"].values   # Leisure, Family, Corporate, Road warrior

            # traveller-driven base (gamma distribution -> skewed)
            traveller_params = {
                ttype_ids[0]: (4.5, 10),   # mean ~45
                ttype_ids[1]: (5.5, 10),   # mean ~55
                ttype_ids[2]: (2.0, 7),    # mean ~14
                ttype_ids[3]: (1.5, 5)     # mean ~7.5
            }

            shapes = bookings_merged["traveller_type_id"].map(lambda x: traveller_params[x][0]).to_numpy()
            scales = bookings_merged["traveller_type_id"].map(lambda x: traveller_params[x][1]).to_numpy()

            days_before = np_rng_.gamma(shape=shapes, scale=scales)

            # age adjustment (small effect)
            age_shift = bookings_merged["age_group"].map({
                "AG1": -8,
                "AG2": -4,
                "AG3":  2,
                "AG4":  6,
                "AG5":  3
            }).to_numpy()

            days_before += age_shift

            # distance category adjustment
            distance = bookings_merged["distance_km"].to_numpy()

            distance_mod = np.where(
                distance >= 5500, 1.25,               # long-haul -> earlier booking
                np.where(distance < 1900, 0.80, 1.0)  # short-haul -> later booking
            )
            days_before *= distance_mod

            # clip + same-day probability
            days_before = np.clip(days_before, 0, np_rng_.integers(90, 180, size=n)).astype(int)

            same_day_probs = {
                ttype_ids[0]: 0.01,
                ttype_ids[1]: 0.005,
                ttype_ids[2]: 0.05,
                ttype_ids[3]: 0.08
            }

            probs = bookings_merged["traveller_type_id"].map(same_day_probs).to_numpy()

            same_day_mask = np_rng_.random(n) < probs
            days_before[same_day_mask] = 0

            # booking date
            bookings_merged["flight_date"] = pd.to_datetime(bookings_merged["flight_date"])
            booking_dates = bookings_merged["flight_date"] - pd.to_timedelta(days_before, unit="D")

            # time-of-day

            clusters = bookings_merged["nationality"].map(country_to_cluster).fillna("default")
            probs = clusters.map(time_profiles)
            probs_matrix = np.vstack(probs.to_numpy())

            # add noise
            probs_matrix += np_rng_.normal(0, 0.02, probs_matrix.shape)
            probs_matrix = np.clip(probs_matrix, 0, None)
            probs_matrix /= probs_matrix.sum(axis=1, keepdims=True)

            cum_probs = probs_matrix.cumsum(axis=1)
            rand = np_rng_.random((n, 1))
            choices = (rand < cum_probs).argmax(axis=1)

            time_windows = {
                0: (6, 12),
                1: (12, 17),
                2: (17, 22),
                3: (22, 6)
            }

            starts = np.array([time_windows[c][0] for c in choices])
            ends   = np.array([time_windows[c][1] for c in choices])

            hours = np.array([
                np_rng_.integers(start, end) if start < end else np_rng_.integers(0, 24)
                for start, end in zip(starts, ends)
            ])

            minutes = np_rng_.integers(0, 60, n)
            seconds = np_rng_.integers(0, 60, n)

            time_offsets = (
                pd.to_timedelta(hours, unit="h") +
                pd.to_timedelta(minutes, unit="m") +
                pd.to_timedelta(seconds, unit="s")
            )

            bookings_merged["booking_time"] = booking_dates + time_offsets

            # same-day override
            mask_same_day = days_before == 0
            offsets = pd.to_timedelta(np_rng_.integers(0, 6 * 3600, mask_same_day.sum()), unit="s")

            bookings_merged.loc[mask_same_day, "booking_time"] = (
                bookings_merged.loc[mask_same_day, "flight_date"] - offsets
            )

            return bookings_merged


        def apply_check_in_behavior_and_cxl_refunds(bookings_merged, traveller_type_lookup, np_rng_=np_rng):

            """
            Notes:
            - The simulation assumes that customers who fail to check in for their flights
              do not receive refunds for their ticket payments.
            - The simulation assumes that customers are fully refunded for their ticket
              payments if a flight is cancelled.
            """

            ttype_ids = traveller_type_lookup["traveller_type_id"].values   # Leisure, Family, Corporate, Road warrior

            class_ids = bookings_merged["class_id"].unique().tolist()      # Economy, Business, First

            n_ = len(bookings_merged)
            booked_rate_pct = bookings_merged["booked_rate_pct"].values

            base_rate = np.select(
                [booked_rate_pct >= 90, booked_rate_pct >= 75, booked_rate_pct >= 70, booked_rate_pct >= 60],
                [
                    np_rng_.uniform(0.95, 0.98, size=n_),
                    np_rng_.uniform(0.92, 0.96, size=n_),
                    np_rng_.uniform(0.90, 0.94, size=n_),
                    np_rng_.uniform(0.88, 0.92, size=n_)
                ],
                default=np_rng_.uniform(0.80, 0.90, size=n_)
            )

            weekday = bookings_merged["flight_date"].dt.weekday.values
            weekday_mod = np.select(
                [weekday == 0, weekday == 4, weekday == 6],
                [-0.006, 0.009, -0.009],
                default=0.0
            )

            traveller_mod = bookings_merged["traveller_type_id"].map({ttype_ids[0]: -0.01, ttype_ids[1]: -0.015, ttype_ids[2]: 0.01, ttype_ids[3]: 0.02}).values

            age_mod = bookings_merged["age_group"].map({"AG1": -0.02, "AG2": -0.01, "AG3": 0.01, "AG4": 0.015, "AG5": -0.005}).values

            class_mod = bookings_merged["class_id"].map({class_ids[0]: -0.005, class_ids[1]: 0.005, class_ids[2]: 0.01}).values

            rand_drop = np_rng_.beta(2, 8, size=n_) * 0.08

            check_in_rate = base_rate * (1 + weekday_mod + traveller_mod + age_mod + class_mod - rand_drop)
            check_in_rate = np.clip(check_in_rate, 0.75, 0.995)

            bookings_merged["check_in_rate"] = check_in_rate
            bookings_merged["is_checked_in"] = np_rng_.random(n_) < check_in_rate
            bookings_merged["is_checked_in"] = np.where(bookings_merged["is_cancelled"] == True, False, bookings_merged["is_checked_in"])

            bookings_merged["has_cancellation_refund"] = bookings_merged["is_cancelled"]

            return bookings_merged


        def investigate_added_columns(bookings_merged):

            bookings_merged["booking_lead_time"] = (bookings_merged["flight_date"] - bookings_merged["booking_time"]).dt.days
            bookings_merged["booking_hour"] = bookings_merged["booking_time"].dt.hour

            lead_time_bins = [0, 3, 7, 14, 30, 60, 120, 200]
            bookings_merged["lead_time_bin"] = pd.cut(bookings_merged["booking_lead_time"], bins=lead_time_bins)

            not_cancelled = bookings_merged[bookings_merged["has_cancellation_refund"] == False].copy()
            not_cancelled["day_of_week"] = not_cancelled["flight_date"].dt.weekday

            for test in [
                "--- Booking Times ---",
                "lead time in days:",
                bookings_merged.groupby("traveller_type_id")["booking_lead_time"].describe(),
                bookings_merged.groupby("age_group")["booking_lead_time"].describe(),
                bookings_merged.groupby("gender")["booking_lead_time"].describe(),
                "booking hour of day:",
                bookings_merged.groupby("nationality")["booking_hour"].describe(),

                "--- Check-ins ---",
                "check-in percentages:",
                not_cancelled["is_checked_in"].value_counts(normalize=True) * 100,
                not_cancelled.groupby("class_id")["is_checked_in"].value_counts(normalize=True).sort_index() * 100,
                not_cancelled.groupby("day_of_week")["is_checked_in"].value_counts(normalize=True).sort_index() * 100,
                not_cancelled.groupby("traveller_type_id")["is_checked_in"].value_counts(normalize=True).sort_index() * 100,
                not_cancelled.groupby("age_group")["is_checked_in"].value_counts(normalize=True).sort_index() * 100,

            ]:
                print(test, "\n")


        bookings_merged_df = (bookings_df
            .merge(customers_df[["customer_id", "gender", "nationality", "traveller_type_id", "age_group"]],
                   on="customer_id", how="left")
            .merge(flights_df[["flight_number", "line_number", "is_cancelled", "booked_rate_pct", "distance_km"]],
                   on="flight_number", how="left")
        )

        bookings_merged_df = assign_booking_times(bookings_merged_df, traveller_type_lookup_df)
        bookings_merged_df = apply_check_in_behavior_and_cxl_refunds(bookings_merged_df, traveller_type_lookup_df)

        bookings_df = bookings_merged_df[[
            "booking_id", "customer_id", "flight_number", "flight_date", "booking_time", "class_id",
            "frequent_flyer_status_id", "is_checked_in", "has_cancellation_refund"
        ]]

        if run_diagnostics:
            investigate_added_columns(bookings_merged_df.copy())

        return bookings_df


    # --- Pricing and Revenue ---

    def apply_pricing_model(bookings_df, routes_df, flights_df, frequent_flyer_discounts_df, customers_df,
        class_discount_adjustments_df, traveller_type_lookup_df, np_rng=np_rng_main, run_diagnostics=show_diagnostics
    ):

        """
        Notes:
        - Frequent flyer tiers reduce price conditional on behavior, but higher-tier customers exhibit systematically
          more expensive booking behavior, which is reflected in the final ticket prices paid per booking.
        - The net_revenue is set to 0 where price_paid has been refunded due to flight cancellation.
            - Aggregating net_revenue without filtering for has_cancellation_refund == False produces stats for scheduled flights.
            - Aggregating net_revenue with filtering for has_cancellation_refund == False produces stats for actualized flights.
        """

        def calculate_prices_pairwise_with_variation(dep_, arr_, distance_km_, np_rng_=np_rng):

            route_key = tuple(sorted([dep_, arr_]))

            if route_key not in route_price_cache:

                # base rate by distance
                if distance_km_ < 1900:
                    base_rate = np_rng_.uniform(0.125, 0.150)
                elif distance_km_ < 5500:
                    base_rate = np_rng_.uniform(0.080, 0.100)
                elif distance_km_ < 8000:
                    base_rate = np_rng_.uniform(0.070, 0.080)
                elif distance_km_ < 10000:
                    base_rate = np_rng_.uniform(0.060, 0.070)
                elif distance_km_ < 14000:
                    base_rate = np_rng_.uniform(0.050, 0.060)
                else:
                    base_rate = np_rng_.uniform(0.040, 0.050)

                # cabin multipliers
                if distance_km_ < 1900:
                    business_mult = np_rng_.uniform(2.0, 2.4)
                    first_mult = np_rng_.uniform(3.5, 4.2)
                else:
                    business_mult = np_rng_.uniform(2.2, 3.4)
                    first_mult = np_rng_.uniform(4.4, 5.8)

                price_economy = distance_km_ * base_rate
                price_business = price_economy * business_mult
                price_first = price_economy * first_mult

                route_price_cache[route_key] = (price_economy, price_business, price_first)

            base_prices_ = route_price_cache[route_key]

            # small directional variation
            directional_multiplier = np_rng_.uniform(0.99, 1.01)

            return tuple(round(p * directional_multiplier, 2) for p in base_prices_)


        route_price_cache = {}
        routes_df[["expd_avg_price_economy", "expd_avg_price_business", "expd_avg_price_first"]] = routes_df.apply(
            lambda row_: pd.Series(
                calculate_prices_pairwise_with_variation(
                    row_["departure_airport_code"],
                    row_["arrival_airport_code"],
                    row_["distance_km"]
                )), axis=1
        )

        bookings_merged = (bookings_df.copy()
            .merge(flights_df [["flight_number", "line_number", "booked_rate_pct", "bookings_total", "seat_capacity", "demand_tier"]],
                on="flight_number", how="left")
            .merge(frequent_flyer_discounts_df[["frequent_flyer_status_id", "base_discount"]],
                on="frequent_flyer_status_id", how="left")
            .merge(customers_df[["customer_id", "traveller_type_id"]], on="customer_id", how="left")
            .merge(routes_df[["line_number", "distance_km", "expd_avg_price_economy", "expd_avg_price_business", "expd_avg_price_first"]],
                on="line_number", how="left")
        )

        bookings_merged = bookings_merged

        ttype_ids = traveller_type_lookup_df["traveller_type_id"].values   # Leisure, Family, Corporate, Road warrior

        class_ids = class_discount_adjustments_df["class_id"].tolist()     # Economy, Business, First
        class_factors = class_discount_adjustments_df["discount_factor"].tolist()

        bookings_merged["booking_lead_time"] = (bookings_merged["flight_date"] - bookings_merged["booking_time"]).dt.days

        # class discount modifier
        class_discount_factor = {class_ids[0]: class_factors[0], class_ids[1]: class_factors[1], class_ids[2]: class_factors[2]}
        bookings_merged["effective_discount"] = bookings_merged["base_discount"] * bookings_merged["class_id"].map(class_discount_factor)

        # base price selection
        price_matrix = bookings_merged[["expd_avg_price_economy", "expd_avg_price_business", "expd_avg_price_first"]].to_numpy()
        class_to_col_idx = {class_ids[0]: 0, class_ids[1]: 1, class_ids[2]: 2}
        class_indices = bookings_merged["class_id"].map(class_to_col_idx).to_numpy()
        base_prices = price_matrix[np.arange(len(bookings_merged)), class_indices]

        # traveller type and lead time multiplier
        days = bookings_merged["booking_lead_time"].to_numpy()
        lead_time_multiplier = np.select(
            [days <= 3, days <= 7, days <= 14, days <= 30, days <= 60],
            [1.2, 1.15, 1.10, 1.05, 0.95],
            default=0.9  # very early booking discount
        )

        traveller_price_multiplier = {ttype_ids[0]: 0.97, ttype_ids[1]: 0.93, ttype_ids[2]: 1.02, ttype_ids[3]: 1.07}
        adjusted_base_prices = (
            base_prices
            * bookings_merged["traveller_type_id"].map(traveller_price_multiplier).to_numpy()
            * lead_time_multiplier
        )

        # demand multiplier

        tier_base_multiplier = {
            "1_90–100": 1.15,
            "2_80–90": 1.07,
            "3_70–80": 1.05,
            "4_60–70": 0.97,
            "5_50–60": 0.95,
            "6_<50":   0.85
        }

        tier_price_sensitivity = {
            "1_90–100": 1.28,
            "2_80–90": 1.10,
            "3_70–80": 1.05,
            "4_60–70": 0.99,
            "5_50–60": 0.90,
            "6_<50":   0.83
        }

        sensitivity = bookings_merged[f"demand_tier"].map(tier_price_sensitivity)
        base_mult = bookings_merged[f"demand_tier"].map(tier_base_multiplier)

        cabin_sensitivity = {
            class_ids[0]: 1.00,
            class_ids[1]: 0.90,
            class_ids[2]: 0.80
        }

        effective_sensitivity = (sensitivity * bookings_merged["class_id"].map(cabin_sensitivity))

        booked_rates_pct = bookings_merged["booked_rate_pct"].to_numpy()
        load_factor = booked_rates_pct / 100
        lf_center = 0.70

        lf_scaled = np.clip((load_factor - lf_center) / (1 - lf_center), -1, 1)
        exp = 0.6 + 0.4 * effective_sensitivity
        lf_component = np.sign(lf_scaled) * (np.abs(lf_scaled) ** exp)

        lf_impact = np.where(
            lf_component >= 0,
            0.85 * lf_component,
            1.30 * lf_component
        )

        demand_factor = (base_mult * (1 + lf_impact))

        demand_factor = np.clip(demand_factor, 0.50, 1.75)

        adjusted_base_prices *= demand_factor

        # add minor price dispersion (+/- ~3%)
        adjusted_base_prices *= np_rng.normal(loc=1.0, scale=0.03, size=len(bookings_merged))

        # apply discount
        discount_strength = 0.8
        effective_discounts = bookings_merged["effective_discount"].to_numpy()
        price_paid = adjusted_base_prices * (1 - effective_discounts * discount_strength)

        # prevent negative prices (very rare but safe)
        price_paid = np.clip(price_paid, base_prices * 0.40, None)

        bookings_merged["price_paid"] = price_paid.round(2)
        bookings_merged["net_revenue"] = np.where(
            bookings_merged["has_cancellation_refund"], 0, bookings_merged["price_paid"]
        )

        if run_diagnostics:

            bookings_merged["distance_category"] = np.select(
                [bookings_merged["distance_km"] < 1900, bookings_merged["distance_km"] >= 5500],
                ["short-haul", "long-haul"], default="medium-haul"
            )
            avg_dist, avg_rev = bookings_merged["distance_km"].mean(), bookings_merged["price_paid"].mean()
            avg_rev_per_km = avg_rev / avg_dist
            avg_dist_by_dist_cat = bookings_merged.groupby("distance_category")["distance_km"].mean()
            avg_rev_by_dist_cat = bookings_merged.groupby("distance_category")["price_paid"].mean()
            avg_rev_per_km_by_dist_cat = avg_rev_by_dist_cat / avg_dist_by_dist_cat

            bookings_merged["expected_price"] = np.select(
                [
                    bookings_merged["class_id"] == class_ids[0],
                    bookings_merged["class_id"] == class_ids[1],
                    bookings_merged["class_id"] == class_ids[2]
                ],
                [
                    bookings_merged["expd_avg_price_economy"],
                    bookings_merged["expd_avg_price_business"],
                    bookings_merged["expd_avg_price_first"]
                ]
            )

            bookings_merged["price_vs_expected"] = (
                bookings_merged["price_paid"] / bookings_merged["expected_price"]
            )

            booked_rate_bins = [0, 50, 60, 70, 80, 90, 100]
            bin_labels = ["(6) <50%", "(5) 50–60%", "(4) 60–70%", "(3) 70–80%", "(2) 80–90%", "(1) 90–100%"]
            bookings_merged["booked_rate_bin"] = pd.cut(bookings_merged["booked_rate_pct"], booked_rate_bins, labels=bin_labels)

            flight_rev = bookings_merged.groupby("flight_number").agg({
                "price_paid": "sum",
                "expected_price": "sum",
                "bookings_total": "first",
                "seat_capacity": "first",
                "booked_rate_pct": "first",
                "demand_tier": "first",
                "distance_category": "first"
            }).reset_index()

            flight_rev["rev_per_seat"] = (flight_rev["price_paid"] / flight_rev["seat_capacity"])
            flight_rev["rev_per_pax"] = (flight_rev["price_paid"] / flight_rev["bookings_total"])
            flight_rev["exp_rev_per_pax"] = (flight_rev["expected_price"] / flight_rev["bookings_total"])

            bookings_merged["discount_impact"] = (
                bookings_merged["price_paid"] /
                (bookings_merged["price_paid"] / (1 - bookings_merged["effective_discount"]))
            )

            pivot_prices = bookings_merged.pivot_table(index="line_number", columns="class_id", values="price_paid", aggfunc="mean")
            pivot_prices["bus_to_eco"] = pivot_prices["(02) Business"] / pivot_prices["(01) Economy"]
            pivot_prices["first_to_bus"] = pivot_prices["(03) First"] / pivot_prices["(02) Business"]

            route_diag = bookings_merged.groupby("line_number").agg({
                "price_paid": "mean", "expected_price": "mean", "booked_rate_pct": "mean"
            }).round(2)

            print(f"\navg distance: {avg_dist} km, avg revenue: {avg_rev} EUR,"
                  f" avg revenue per km: {avg_rev_per_km} EUR")
            print("\navg_dist_by_dist_cat")
            print(avg_dist_by_dist_cat)
            print("\navg_rev_by_dist_cat")
            print(avg_rev_by_dist_cat)
            print("\navg_rev_per_km_by_dist_cat")
            print(avg_rev_per_km_by_dist_cat)

            print("\n=== PRICE VS EXPECTED (by distance category) ===")
            print(bookings_merged.groupby("distance_category")["price_vs_expected"].mean())

            print("\n=== PRICE VS EXPECTED (by tier & load) ===")
            print(bookings_merged.groupby(["demand_tier", "booked_rate_bin"],
                    observed=False)["price_vs_expected"].mean().round(3).head(50))

            print("\n=== AVG PRICE PAID (by tier) ===")
            print(bookings_merged.groupby("demand_tier")["price_paid"].mean().round(2))

            print("\n=== REV PER PAX AND SEAT (by tier) ===")
            print(flight_rev.groupby("demand_tier")[
                      ["booked_rate_pct", "exp_rev_per_pax", "rev_per_pax", "rev_per_seat"]
                  ].mean().round(2))

            print("\n=== AVG EFFECTIVE DISCOUNT ===")
            print(bookings_merged.groupby(["demand_tier", "class_id"])["effective_discount"].mean().round(3))

            print("\n=== CABIN PRICE RATIOS ===")
            print(pivot_prices[["bus_to_eco", "first_to_bus"]].mean().round(2))

            print("\n=== PRICE VS EXPECTED BY TIER ===")
            print(bookings_merged.groupby("demand_tier")["price_vs_expected"].mean().round(3))

            print("\n=== ROUTE DIAGNOSTIC SAMPLE ===")
            print(route_diag.sample(10, random_state=50))

        return routes_df, bookings_merged[[
            "booking_id", "customer_id", "flight_number", "flight_date", "booking_time", "class_id",
            "frequent_flyer_status_id", "price_paid", "is_checked_in", "has_cancellation_refund", "net_revenue"
        ]]


    # --- Costs per flight table ---

    def create_route_cost_per_flight_df(
            bookings_df, flights_df, aircraft_df, start_date, end_date, np_rng=np_rng_main, run_diagnostics=show_diagnostics
    ):

        """
        While the output of this function is a cost table, it's main purpose is to create plausible
        profit margins relative to satisfied demand. Costs are therefore 'reverse-engineered' from a target
        profit margin average and booked rate comparisons. When multiple years are simulated, a revenue
        ratio is introduced to shift explanatory power regarding changed profit margins from differences
        between costs to differences between revenues.
        """

        sim_years = round(((end_date - start_date) / 365).days)

        financials_df = (
            bookings_df.groupby("flight_number").agg(net_revenue=("net_revenue", "sum"), price_paid=("price_paid", "sum"))
            .merge(flights_df[["flight_number", "flight_date", "line_number", "aircraft_id", "booked_rate_pct",
                "is_cancelled", "distance_km", "demand_tier", "base_demand_bias"]], on="flight_number", how="left")
            .merge(aircraft_df[["aircraft_id", "model"]], on="aircraft_id", how="left")
        )

        if sim_years >= 2:
            prices_paid_routes = (
                bookings_df.merge(flights_df[["flight_number", "line_number"]], on="flight_number", how="left")
                .groupby("line_number")["price_paid"].mean().rename("avg_route_price_paid").reset_index()
            )

            reference_prices_paid = (
                bookings_df[bookings_df["flight_date"].dt.year == start_date.year + 1]
                .merge(flights_df[["flight_number", "line_number"]], on="flight_number", how="left")
                .groupby("line_number")["price_paid"].mean().rename("reference_price_paid").reset_index()
            )

            financials_df = (
                financials_df.merge(prices_paid_routes, on="line_number", how="left")
                .merge(reference_prices_paid, on="line_number", how="left")
            )

        # aircraft-specific cask
        cask_lookup = {
            # regional / small
            "CRJ900": 0.09,
            "E195-E2": 0.08,

            # efficient narrowbody
            "A220-300": 0.075,
            "A320neo": 0.073,
            "737 MAX 8": 0.073,
            "737 MAX 9": 0.074,
            "A321neo": 0.077,
            "MC-21-300": 0.077,

            # widebody (long-haul)
            "A340-500": 0.07,
            "777-200LR": 0.065,
        }

        financials_df["cask"] = financials_df["model"].map(cask_lookup)
        financials_df["cask"] = financials_df["cask"].fillna(0.08)

        # create base distribution dependent on flight booked rate relative to global mean
        reference_booked_rate = financials_df["booked_rate_pct"].mean() / 100
        flight_booked_rate = financials_df["booked_rate_pct"] / 100
        br_diff_weight = 1.77

        tanh_adj = 1.05 if sim_years >= 3 else 1.08
        booked_rate_diff = np.tanh((reference_booked_rate - flight_booked_rate) * tanh_adj)

        # overall target profit margin for year with average booking performance
        avg_profit_margin_target = 0.165
        reverse_margin = 1 - avg_profit_margin_target

        financials_df["flight_cost_total"] = financials_df["price_paid"] * (reverse_margin + br_diff_weight * booked_rate_diff)

        # route distribution adjustments
        financials_df["flight_cost_total"] *= 1 + 0.05 * financials_df["base_demand_bias"]

        if sim_years <= 1:
            choices = [0.98, 1.05, 0.98, 1.00, 1.00]
        elif sim_years == 2:
            choices = [1.04, 1.05, 1.01, 0.99, 1.01]
        else:
            choices = [1.00, 1.05, 1.02, 0.98, 1.00]

        financials_df["flight_cost_total"] *= np.select(
            [financials_df["demand_tier"] == "1_90–100",
             financials_df["demand_tier"] == "2_80–90",
             financials_df["demand_tier"] == "3_70–80",
             financials_df["demand_tier"] == "4_60–70",
             financials_df["demand_tier"] == "5_50–60"],
            choices, default=1
        )

        # distance category adjustment
        if sim_years <= 1:
            long_haul_adj = 1.05
        elif sim_years == 2:
            long_haul_adj = 1.035
        else:
            long_haul_adj = 1.025

        financials_df["flight_cost_total"] *= np.where(financials_df["distance_km"] >= 5500, long_haul_adj, 1)

        # noise
        financials_df["flight_cost_total"] *= np_rng.normal(1.0, 0.03, len(financials_df))

        # cask
        cask_rel = (financials_df["cask"] / financials_df["cask"].mean()) - 1
        cask_strength = 0.15
        financials_df["flight_cost_total"] *= (1 + cask_strength * cask_rel)

        # revenue comparison adjustment
        if sim_years >= 2:
            rev_ratio = financials_df["avg_route_price_paid"] / financials_df["reference_price_paid"]
            rev_diff = np.clip(1 - rev_ratio, -0.1, 0.1)

            financials_df["flight_cost_total"] *= (1 + 0.2 * rev_diff)

        # cost breakdown
        def split_costs(total):
            fuel = total * np_rng.uniform(0.30, 0.38)
            crew = total * np_rng.uniform(0.12, 0.16)
            maintenance = total * np_rng.uniform(0.10, 0.14)
            landing = total * np_rng.uniform(0.06, 0.09)
            catering = total * np_rng.uniform(0.04, 0.06)

            assigned = fuel + crew + maintenance + landing + catering
            other = total - assigned

            return pd.Series([
                round(fuel, 2),
                round(crew, 2),
                round(maintenance, 2),
                round(landing, 2),
                round(catering, 2),
                round(other, 2)
            ])

        cost_cols = [
            "fuel_cost",
            "crew_cost",
            "maintenance_cost",
            "landing_fees",
            "catering_cost",
            "other_costs"
        ]

        financials_df[cost_cols] = financials_df["flight_cost_total"].apply(split_costs)

        # cancellation adjustments (partial costs only)
        cancel_multiplier = {
            "fuel_cost": 0.05,
            "crew_cost": 0.6,
            "maintenance_cost": 0.75,
            "landing_fees": 0.2,
            "catering_cost": 0.1,
            "other_costs": 0.7
        }

        distance_impact = np.clip(financials_df["distance_km"] / 5500, 0.5, 2.0)

        cancel_mask = financials_df["is_cancelled"]

        for col, base_factor in cancel_multiplier.items():
            adjusted_factor = base_factor + (distance_impact - 1) * 0.1
            financials_df[col] *= np.where(
                cancel_mask, np.clip(adjusted_factor, 0.05, 0.9), 1.0
            )

        financials_df["flight_cost_total"] = financials_df[cost_cols].sum(axis=1)

        # table creation
        costs_per_flight_df = financials_df[
            ["flight_number", "flight_date", "flight_cost_total", *cost_cols]
        ].copy()

        costs_per_flight_df.insert(0, "flight_cost_id", "C_" + costs_per_flight_df["flight_number"].str[2:])

        if run_diagnostics:

            financials_df["distance_category"] = np.select(
                [financials_df["distance_km"] < 1900, financials_df["distance_km"] >= 5500],
                ["short-haul", "long-haul"], default="medium-haul"
            )

            # profit calculation
            financials_df["flight_profit_total"] = (
                financials_df["net_revenue"] - financials_df["flight_cost_total"]
            ).round(2)

            financials_df["profit_margin_pct"] = (np.where(
                financials_df["net_revenue"] == 0, 0,
                financials_df["flight_profit_total"] / financials_df["net_revenue"]) * 100
            ).round(2)

            profit_margin_bins = [-200, -100, -50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 100, 200]
            bin_labels = ["< -100%", "-100% to -50%", "-50% to -40%", "-40% to -30%", "-30% to -20%", "-20% to -10%", "-10% to 0%",
                          "0% to 10%", "10% to 20%", "20% to 30%", "30% to 40%", "40% to 50%", "50% to 100%", "> 100%"]
            financials_df["profit_margin_bin"] = pd.cut(financials_df["profit_margin_pct"], bins=profit_margin_bins, labels=bin_labels)

            for yr in financials_df["flight_date"].dt.year.unique():
                print(f"\nYear {yr}:\n")
                scheduled_flights = financials_df[financials_df["flight_date"].dt.year == yr]
                operated_flights = financials_df[financials_df["flight_date"].dt.year == yr].query("is_cancelled == False")

                operated_routes = operated_flights.groupby("line_number")["profit_margin_pct"].mean().rename("avg_profit_margin_pct").reset_index()
                operated_routes["avg_profit_margin_bin"] = pd.cut(
                    operated_routes["avg_profit_margin_pct"], bins=profit_margin_bins, labels=bin_labels
                )

                for test in {
                    "profit margins:": operated_flights["profit_margin_pct"].describe(),
                    "profit margins by distance":
                        operated_flights.groupby("distance_category")["profit_margin_pct"].describe(),
                    "flight profit margin bins": operated_flights.groupby("profit_margin_bin", observed=False)["flight_number"].nunique(),

                    "average route profit margin bins": operated_routes.groupby("avg_profit_margin_bin", observed=False)["line_number"].count(),
                    "min and max route profit margins": (operated_routes["avg_profit_margin_pct"].min(), operated_routes["avg_profit_margin_pct"].max()),

                    "flight profits": scheduled_flights["flight_profit_total"].describe(),
                    "full vs partial costs": scheduled_flights.groupby("is_cancelled")[cost_cols].mean(),
                }.items():
                    print(test[0], "\n", test[1], "\n")

        return costs_per_flight_df


    # --- Cleanup ---

    def finalize_tables(
            weather_df, customers_df, flights_df, bookings_df, routes_df, run_diagnostics=show_diagnostics
    ):

        weather_df = weather_df.drop(columns=["delay_prob", "cancel_prob"])
        customers_df = customers_df.drop(columns="age_group")

        flights_df = flights_df.drop(
            columns=["seat_capacity", "distance_km", "booked_rate_pct", "demand_tier", "base_demand_bias"]
        )

        if run_diagnostics:
            print("\ntotal bookings correct:", flights_df["bookings_total"].sum() == bookings_df.shape[0])

        passengers_map = bookings_df.groupby("flight_number")["is_checked_in"].sum()
        flights_df.insert(
            flights_df.columns.get_loc("bookings_total") + 1, "passengers_total",
            flights_df["flight_number"].map(passengers_map).fillna(0).astype(int)
        )

        routes_df = routes_df.drop(columns=["volatility", "season_sensitivity"])

        demand_tier_cols = [col for col in routes_df.columns if col.startswith("demand_tier")]
        demand_bias_cols = [col for col in routes_df.columns if col.startswith("base_demand_bias")]
        routes_df = routes_df.drop(columns=demand_tier_cols + demand_bias_cols)

        if run_diagnostics:
            print("\nmissed check-in rate (pct):",
                  (bookings_df.query("has_cancellation_refund == False").shape[0] -
                   flights_df.query("is_cancelled == False")["passengers_total"].sum())
                  / bookings_df.query("has_cancellation_refund == False").shape[0] * 100,
            )

        return weather_df, customers_df, flights_df, routes_df


    # --- Optional: noise introduction enabling data completeness assessments and cleaning tasks ---

    def add_noise_to_customers(customers, np_rng=np_rng_main, run_diagnostics=show_diagnostics):


        def add_email_typo(email_, np_rng_=np_rng):
            username, sep, domain = email_.partition("@")
            if not sep:
                return email_

            typo_options = [
                lambda u, d: u + "@gnail.com",
                lambda u, d: u + "@gmal.com",
                lambda u, d: u + "@yaho.com",
                lambda u, d: u + "@hotmial.com",
                lambda u, d: u + "@gmail..com",
                lambda u, d: u + "@yahoo..com",
                lambda u, d: u + "@hotmail..com",
                lambda u, d: u + "@@gmail.com",
                lambda u, d: u + "@@yahoo.com",
                lambda u, d: u + "@@hotmail.com",
                lambda u, d: u + "@gmail.comcom",
                lambda u, d: u + "@yahoo.comcom",
                lambda u, d: u + "@hotmail.comcom",
                lambda u, d: u + "@gmai.com",
                lambda u, d: u.replace(".", ""),
                lambda u, d: u + d
            ]
            typo_func = np_rng_.choice(typo_options)
            return typo_func(username, domain)


        missing_rate = 0.01
        typo_rate = 0.02

        noisy_data = []
        for row in customers.itertuples(index=False):
            customer_id, full_name, email, phone, dob, nationality, gender, status_, ttype = row

            if np_rng.random() < typo_rate:
                if np_rng.random() < 0.5:
                    email = add_email_typo(email)
                else:
                    full_name = full_name.replace(" ", "  ")

            if np_rng.random() < typo_rate:
                nationality = nationality.lower()

            if np_rng.random() < missing_rate:
                email = None
            if np_rng.random() < missing_rate:
                phone = None
            if np_rng.random() < missing_rate:
                dob = None

            noisy_data.append(
                (customer_id, full_name, email, phone, dob, nationality, gender, status_, ttype)
            )

        customers_noisy = pd.DataFrame(noisy_data, columns=[
            "customer_id", "full_name", "email", "phone", "date_of_birth", "nationality",
            "gender", "frequent_flyer_status_id", "traveller_type_id"
        ])

        if run_diagnostics:
            g_nail_mask = [str(x).endswith("@gnail.com") for x in customers_noisy["email"]]
            print("noisy email sample\n", customers_noisy.loc[g_nail_mask, "email"].sample(5))

        return customers_noisy



    start_date_global, end_date_global, sim_days_global = determine_simulation_parameters()

    df_airports = create_airports_df()
    df_aircraft = create_aircraft_dfs("full")
    df_routes = create_route_dfs("full", start_date_global, end_date_global)
    df_weather = create_weather_df(df_airports, start_date_global, end_date_global)

    df_flights = create_flights_df(
        create_aircraft_dfs, create_route_dfs, start_date_global, end_date_global, sim_days_global
    )

    df_frequent_flyer_discounts = pd.DataFrame(frequent_flyer_specs)
    df_class_discount_adjustments = pd.DataFrame(class_discount_modifiers)

    df_flight_capacity_by_class, df_flight_class_cost_shares = (
        create_class_cap_and_cost_dfs(df_flights, df_class_discount_adjustments)
    )

    df_customers, df_bookings, df_traveller_type_lookup = (
        create_bookings_and_customers_dfs(df_flight_capacity_by_class, df_frequent_flyer_discounts)
    )

    df_customers = assign_customer_attributes(df_customers, df_routes, df_airports, start_date_global)
    df_bookings = add_columns_to_bookings_df(df_bookings, df_customers, df_flights, df_traveller_type_lookup)

    df_routes, df_bookings = apply_pricing_model(
        df_bookings, df_routes, df_flights, df_frequent_flyer_discounts, df_customers,
        df_class_discount_adjustments, df_traveller_type_lookup
    )

    df_costs_per_flight = create_route_cost_per_flight_df(
        df_bookings, df_flights, df_aircraft, start_date_global, end_date_global
    )

    df_weather, df_customers, df_flights, df_routes = finalize_tables(
        df_weather, df_customers, df_flights, df_bookings, df_routes
    )

    df_customers_noisy = add_noise_to_customers(df_customers)

    return {
        "airports": df_airports,
        "aircraft": df_aircraft,
        "routes": df_routes,
        "weather": df_weather,
        "frequent_flyer_discounts": df_frequent_flyer_discounts,
        "class_discount_adjustments": df_class_discount_adjustments,
        "flights": df_flights,
        "flight_capacity_by_class": df_flight_capacity_by_class,
        "flight_class_cost_shares": df_flight_class_cost_shares,
        "bookings": df_bookings,
        "traveller_type_lookup": df_traveller_type_lookup,
        "costs_per_flight": df_costs_per_flight,
        "customers": df_customers,
        "customers_noisy": df_customers_noisy
    }
