
AIRLINE DATABASE USAGE NOTES

-------------
General notes
-------------
- All datetime fields are stored in UTC.
- All monetary values are expressed in EUR.
- This is a simulated relational dataset intended for data analysis practice and projects
  using SQL, Python, Excel, Power BI, Tableau, etc.
- Data contains designed patterns intended to support analysis.

------------------------------
Core joins and derived metrics
------------------------------

1. Capacity and demand analysis

Join:
- flights -> aircraft via aircraft_id

Use seat_capacity to derive:

- Booked rate
  bookings_total / seat_capacity

  Indicates demand relative to available capacity.

- Load factor
  passengers_total / seat_capacity

  Indicates operational utilization.

Note:
Booked rate and load factor differ because bookings_total includes cancelled flights and missed check-ins,
while passengers_total reflects actual transported passengers.


2. Route analysis

Join:
- flights -> routes via line_number

Useful fields:
- route_name
- distance_km

Distance categories assumed during generation:

- Short-haul:  < 1900 km
- Medium-haul: 1900 km to < 5500 km
- Long-haul:   >= 5500 km

Supports:
- Per-route aggregations
- Distance-based segmentation
- Route performance comparisons


3. Revenue analysis

Revenue per scheduled flight:
- Sum net_revenue from bookings
- Group by flight_number

Revenue per operated flight:
- Same, excluding bookings where has_cancellation_refund is True

Derived metrics:
- Revenue per km
- Revenue per seat
- Revenue per passenger


4. Profitability analysis

Join:
- Revenue per flight (from bookings)
- costs_per_flight -> flights via flight_number

Metrics:
- Profit:
  revenue - flight_cost_total

- Profit margin:
  profit / revenue
  (use 0 where revenue = 0)


5. Cabin-class analysis

Capacity:
- flight_capacity_by_class

Metrics by flight_number and class_id:
- Booked rate:
  class_bookings / capacity

- Load factor:
  passengers_total / capacity

- Revenue:
  Sum net_revenue from bookings

- Estimated class-level cost:
  flight_cost_total * cost_share
  using flight_class_cost_shares


-----------------------
Weather impact analysis
-----------------------

Filter flights where:
- cancellation_reason = 'Weather'
- delay_reason_dep = 'Weather'
- delay_reason_arr = 'Weather'

Join:
- flights -> routes via line_number
- routes -> weather via airport_code

Use:
- departure_airport_code for departure disruptions
- arrival_airport_code for arrival disruptions

Investigate weather records where:
- observation_time overlaps with scheduled_departure
- or observation_time overlaps with scheduled_arrival


-----------------------------------
Route-level performance comparisons
-----------------------------------

Aggregate KPIs by:
- line_number or route_name

Suggested statistics:
- Average
- Quantiles
- Minimum / Maximum
- Standard deviation

Compare by:

- Distance category
- Year (if multiple years simulated)
- Day of the week
- Travel season

Seasonality assumptions:
- Strong months: 6, 7, 8, 12
- Weak months: 1, 2, 11
- Holiday spike: Dec 20 to Jan 3

Additional segmentation:
- Volatility (standard deviation of KPIs)
- Performance tiers based on:
  - booked rate
  - load factor
  - profit margin


-----------------------------
Customer and booking analysis
-----------------------------

Join:
- bookings -> customers via customer_id

Compare customer and booking shares by:
- traveller_type
- frequent_flyer_status_id
- age_group
- gender
- nationality

Age may be derived using:
latest flight_date - date_of_birth

Further comparisons:
- Booking lead times
- Check-in rates
- Cabin-class preferences

Expected vs. effective pricing:
- bookings -> flights via flight_number
- flights -> routes via line_number

Compare:
- expected average class prices
- effective price_paid

Notes:
- Higher frequent flyer tiers may receive discounts while still showing higher effective prices due to systematically
  more expensive booking behavior (e.g. shorter lead times, premium cabin preference, high-demand routes).
- When deriving age, missing values for date_of_birth must be handled if the noisy customers table is used.


------------------------------
Data completeness and cleaning
------------------------------

Relevant if noisy customers table is included.

Check for:
- Missing values
  - date_of_birth
  - email
  - phone

- Formatting inconsistencies
  - Missing capitalization (nationality)
  - Redundant spaces in full_name

- Data quality issues
  - Common email domain typos
  - Invalid email formats
