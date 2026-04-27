
def save_previews(dict_df):

    previews = dict()

    previews["airports"] = dict_df["airports"].sample(10, random_state=50).sort_index()

    previews["aircraft"] = (
        dict_df["aircraft"].groupby("model", as_index=False).first()[df_aircraft.columns].sort_values("aircraft_id")
    )

    route_distance_mask = [
        dict_df["routes"]["distance_km"].min(),
        dict_df["routes"]["distance_km"].quantile(0.25, interpolation="nearest"),
        dict_df["routes"]["distance_km"].quantile(0.50, interpolation="nearest"),
        dict_df["routes"]["distance_km"].quantile(0.75, interpolation="nearest"),
        dict_df["routes"]["distance_km"].max()
    ]
    previews["routes"] = dict_df["routes"][dict_df["routes"]["distance_km"].isin(route_distance_mask)]

    previews["weather"] = dict_df["weather"].sample(10, random_state=43).sort_index()

    previews["flights"] = dict_df["flights"].sample(10, random_state=45).sort_values("flight_date")

    fn_fcc_sample = dict_df["flight_capacity_by_class"]["flight_number"].sample(3, random_state=50)
    previews["flight_capacity_by_class"] = (
        dict_df["flight_capacity_by_class"][dict_df["flight_capacity_by_class"]["flight_number"].isin(fn_fcc_sample)]
    )

    previews["costs_per_flight"] = dict_df["costs_per_flight"].sample(10, random_state=50).sort_index()

    fn_fcc_sample = dict_df["flight_class_cost_shares"]["flight_number"].sample(5, random_state=50)
    previews["flight_class_cost_shares"] = (
        dict_df["flight_class_cost_shares"][dict_df["flight_class_cost_shares"]["flight_number"].isin(fn_fcc_sample)]
    )

    previews["frequent_flyer_discounts"] = dict_df["frequent_flyer_discounts"]

    previews["class_discount_adjustments"] = dict_df["class_discount_adjustments"]

    previews["traveller_type_lookup"] = dict_df["traveller_type_lookup"]

    previews["bookings"] = dict_df["bookings"].sample(10, random_state=43).sort_index()

    customers_sample = dict_df["customers"].sample(10, random_state=42).sort_index().copy()
    customers_sample["date_of_birth"] = customers_sample["date_of_birth"].dt.date
    previews["customers"] = customers_sample

    for table, preview in previews.items():
        preview.to_html(f"previews/{table}_preview.html", index=False)
