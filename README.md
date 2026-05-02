# Simulated Airline Database

## Overview

This project simulates a realistic airline database using Python (Pandas) to support data analytics across operations, 
finance, weather, and customer behavior.
It represents a medium-to-large European airline and allows export to SQL, CSV collections, or an Excel snapshot.  
Variations of the database with differing simulation periods and decimal separators are made available through a
download portal implemented via [Streamlit](https://simulatedairlinedatabase.streamlit.app/).

## Simulation Features

- **Network**: 42 airports, 142 routes, 264 daily flights
    - Primary hub: Frankfurt (FRA)
    - Secondary hubs: London (LHR), Paris (CDG)
    - ~68% short-haul, ~24% medium-haul, ~8% long-haul
- **Fleet**: 68 aircraft across multiple models and manufacturers
- **Scheduling**: Aircraft assigned based on real availability; multi-day loops and weather effects
- **Weather Simulation**: Localized conditions influence delays and cancellations
- **Demand Modeling**: Affected by route identity, year, season, weekday, holidays, and randomness
- **Operational Realism**: Turnaround time varies by aircraft size and route distance
- **Financial Logic**:
  - Base fares determined by distance and cabin class
  - Effective fares adjusted by demand, booking lead times, discounts, and price dispersion
  - Flight costs split into fuel, crew, maintenance, and class-level cost shares
- **Customer Behavior**:
  - Loyalty tier and traveller type based on customer booking frequencies and demographics
  - Traveller type, age, gender, and demand affect
    - Cabin class preferences
    - Check-in likelihoods
    - Booking lead times

## Downloadable Package Options

### Simulation Extent

- One year
- Two years
- Three years
- Snapshot (3 weeks)

### Decimal Separator

- Period (.)
- Comma (,)
 
## Project Structure

| Folder / File            | Description                                                                                         |
|--------------------------|-----------------------------------------------------------------------------------------------------|
| `core/`                  | Table generation, archive creation, release assets upload, and PostgreSQL insertion                 |
| `assets/`                | SQL table creation script, database usage notes, and license for inclusion in downloadable archives |
| `app/`                   | Streamlit app for download portal and procurement of required assets                                |
| `previews/`              | Representative table-samples for display in download portal                                         |
| `generate_and_export.py` | Combined execution of core functions                                                                |
| `core/workshop.ipynb`    | Developement and diagnostics environment                                                            |

## Core Database Tables (Key Structure)

| Table                      | Description                                     | Primary Key(s)                           |
|----------------------------|-------------------------------------------------|------------------------------------------|
| `airports`                 | Airport info, coordinates, climate data         | `airport_code`                           |
| `aircraft`                 | Fleet info (capacity, range, manufacturer)      | `aircraft_id`                            |
| `routes`                   | Routes, distances, pricing, frequencies         | `line_number`                            |
| `weather`                  | Hourly airport weather observations             | `weather_id`                             |
| `flights`                  | Core fact table (schedule, delay, cancellation) | `(flight_number, flight_date)`           |
| `flight_capacity_by_class` | Seats & bookings per class                      | `(flight_number, flight_date, class_id)` |
| `costs_per_flight`         | Total & component flight costs                  | `(flight_number, flight_date)`           |
| `bookings`                 | Customer bookings, payments, refunds            | `booking_id`                             |
| `customers`                | Demographics, nationality, flyer status         | `customer_id`                            |

## Format Conventions

- Dates: `YYYY-MM-DD`
- Timestamps: UTC
- Currency: Euro (€)
- Week start: Monday

## Future Improvements

- Avoid clustering of scheduled flights around certain times of day
- Implement connecting flights (currently nonstop only)
- Move from year-based loyalty tier assignment to dynamic point-based system
- Implement nationality-weighted distribution of customers to flights
- Potential extensions: `payments` and `maintenance_records` tables

## Implementation Notes

- Data generation logic created with ChatGPT- and Claude Sonnet-assisted iteration
- Route network and flight assignment logic manually refined for realism and loop stability
- Extensive debugging was required to ensure valid multi-day aircraft scheduling
- Significant performance improvements compared to earlier version of the project

## Reference

[Earlier version of this project](https://github.com/data-analysis-colab/database_creation_airline_python) by Jan H. Schüttler & Behzad Nematipour

## Author
Jan H. Schüttler ([LinkedIn](https://www.linkedin.com/in/jan-heinrich-sch%C3%BCttler-64b872396/))
