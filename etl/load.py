import json
import snowflake.connector

def create_tables(cur):
    # Line Status
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TFL_STATUS (
            line_id STRING,
            line_name STRING,
            mode_name STRING,
            severity STRING,
            status_description STRING,
            created_at TIMESTAMP
        )
    """)

    # Disruptions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TFL_DISRUPTIONS (
            line_id STRING,
            line_name STRING,
            disruption_category STRING,
            description STRING,
            created_at TIMESTAMP
        )
    """)

    # Occupancy
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TFL_OCCUPANCY (
            id STRING,
            name STRING,
            place_type STRING,
            occupancy_level STRING,
            free_space INT,
            total_space INT,
            created_at TIMESTAMP
        )
    """)

    # Journeys
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TFL_JOURNEYS (
            journey_id STRING,
            start_point STRING,
            end_point STRING,
            duration_minutes INT,
            line_name STRING,
            mode STRING,
            crowding_level STRING,
            departure_time TIMESTAMP,
            arrival_time TIMESTAMP,
            created_at TIMESTAMP
        )
    """)

    # Station Status
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TFL_STATION_STATUS (
            station_id STRING,
            station_name STRING,
            line_id STRING,
            line_name STRING,
            status_description STRING,
            created_at TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS TFL_ARRIVALS (
            id STRING,
            operation_type INT,
            vehicle_id STRING,
            naptan_id STRING,
            station_name STRING,
            line_id STRING,
            line_name STRING,
            platform_name STRING,
            direction STRING,
            bearing STRING,
            destination_naptan_id STRING,
            destination_name STRING,
            timestamp STRING,
            time_to_station INT,
            current_location STRING,
            towards STRING,
            expected_arrival STRING,
            time_to_live STRING,
            mode_name STRING,
            created_at TIMESTAMP
        )
    """)


# ------------------ Loaders ------------------

def load_tfl_status(cur, conn, data):
    for line in data:
        line_id = line.get("id")
        name = line.get("name")
        mode = line.get("modeName")

        for status in line.get("lineStatuses", []):
            desc = status.get("statusSeverityDescription")
            severity = status.get("statusSeverity")

            cur.execute("""
                INSERT INTO TFL_STATUS (line_id, line_name, mode_name, severity, status_description, created_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (line_id, name, mode, severity, desc))
    conn.commit()


def load_line_disruptions(cur, conn, data):
    for d in data:
        line_id = d.get("lineId", "")
        name = d.get("lineName", "")
        category = d.get("category", "")
        desc = d.get("description", "")

        cur.execute("""
            INSERT INTO TFL_DISRUPTIONS (line_id, line_name, disruption_category, description, created_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (line_id, name, category, desc))
    conn.commit()


# def load_occupancy(cur, conn, data):
#     for occ in data:
#         occ_id = occ.get("id")
#         name = occ.get("name")
#         place_type = occ.get("placeType")
#         occupancy = occ.get("occupancyType", "")
#         free_space = occ.get("freeSpace", None)
#         total_space = occ.get("totalCapacity", None)

#         cur.execute("""
#             INSERT INTO TFL_OCCUPANCY (id, name, place_type, occupancy_level, free_space, total_space, created_at)
#             VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
#         """, (occ_id, name, place_type, occupancy, free_space, total_space))
#     conn.commit()
def load_arrivals(cur, conn, data):
    """
    Batch load arrivals data into Snowflake table TFL_ARRIVALS
    """
    rows = []
    for arr in data:
        rows.append((
            arr.get("id"),
            arr.get("operationType"),
            arr.get("vehicleId"),
            arr.get("naptanId"),
            arr.get("stationName"),
            arr.get("lineId"),
            arr.get("lineName"),
            arr.get("platformName"),
            arr.get("direction"),
            arr.get("bearing"),
            arr.get("destinationNaptanId"),
            arr.get("destinationName"),
            arr.get("timestamp"),
            arr.get("timeToStation"),
            arr.get("currentLocation"),
            arr.get("towards"),
            arr.get("expectedArrival"),
            arr.get("timeToLive"),
            arr.get("modeName")
        ))
    if rows:
        cur.executemany("""
            INSERT INTO TFL_ARRIVALS
            (id, operation_type, vehicle_id, naptan_id, station_name,
             line_id, line_name, platform_name, direction, bearing,
             destination_naptan_id, destination_name, timestamp, time_to_station,
             current_location, towards, expected_arrival, time_to_live,
             mode_name, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, rows)
        conn.commit()



def load_journeys(cur, conn, data):
    # TfL journey response can be nested, so we pick key parts
    for j in data.get("journeys", []):
        journey_id = j.get("startDateTime", "") + "_" + j.get("arrivalDateTime", "")
        start_point = j.get("legs", [{}])[0].get("departurePoint", {}).get("commonName", "")
        end_point = j.get("legs", [{}])[-1].get("arrivalPoint", {}).get("commonName", "")
        duration = j.get("duration")
        line_name = j.get("legs", [{}])[0].get("routeOptions", [{}])[0].get("name", "")
        mode = j.get("legs", [{}])[0].get("mode", {}).get("id", "")
        crowding = j.get("legs", [{}])[0].get("crowding", {}).get("passengerFlows", "")

        departure_time = j.get("startDateTime")
        arrival_time = j.get("arrivalDateTime")

        cur.execute("""
            INSERT INTO TFL_JOURNEYS (journey_id, start_point, end_point, duration_minutes, line_name, mode, crowding_level, departure_time, arrival_time, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (journey_id, start_point, end_point, duration, line_name, mode, crowding, departure_time, arrival_time))
    conn.commit()


def load_station_status(cur, conn, data):
    rows = []
    for st in data.get("stopPoints", []):
        station_id = st.get("id")
        name = st.get("commonName")
        for line in st.get("lines", []):
            line_id = line.get("id")
            line_name = line.get("name")
            status = "Active"  # placeholder
            rows.append((station_id, name, line_id, line_name, status))
    if rows:
        cur.executemany("""
            INSERT INTO TFL_STATION_STATUS (station_id, station_name, line_id, line_name, status_description, created_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, rows)
        conn.commit()
