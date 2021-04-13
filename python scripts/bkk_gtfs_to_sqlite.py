"""
Creates SQLite indexed database from BKK GTFS files.
BKK GTFS source: https://bkk.hu/apps/gtfs/
GTFS is refreshed daily
"""

import csv
import sqlite3
import zipfile
from pathlib import Path
import click

@click.command()
@click.option('--zip_file', default="default", help='Path to the GTFS .zip file')
@click.option('--db_dir', default="./", help='Path to create the SQLite database')

def create_db(zip_file: str, db_dir: str):
    """
    creates sqlite db
    """
    db_dir = Path(db_dir)
    zip_file = Path(zip_file)
    zip_dir = zip_file.parent
    gtfs_dir = zip_dir/"extracted"
    db_name = zip_file.name.split(".")[0]
    db_name = f"{db_name}.db"

    print(":"*15)
    print()
    print("Database directory: ", db_dir)
    print("Extracted GTFS directory: ", gtfs_dir)
    print("Database name: ", db_name)
    print()

    print("unzipping files..")
    print()
    with zipfile.ZipFile(zip_file) as zfile:
        zfile.extractall(gtfs_dir)

    print("reading files..")

    print("reading agency..")
    with open(gtfs_dir/"agency.txt", 'r') as fin:
        dr = csv.DictReader(fin)
        agency_to_db = [(col["agency_id"],
                         col["agency_name"],
                         col["agency_url"],
                         col["agency_timezone"],
                         col["agency_lang"],
                         col["agency_phone"])
                        for col in dr]

    print("reading calendar_dates..")
    with open(gtfs_dir/"calendar_dates.txt", 'r') as fin:
        dr = csv.DictReader(fin)
        calendar_dates_to_db = [(col["service_id"],
                                 col["date"],
                                 col["exception_type"])
                                for col in dr]

    print("reading routes..")
    with open(gtfs_dir/"routes.txt", 'r') as fin:
        dr = csv.DictReader(fin)
        routes_to_db = [(col["agency_id"],
                         col["route_id"],
                         col["route_short_name"],
                         col["route_long_name"],
                         col["route_type"],
                         col["route_desc"],
                         col["route_color"],
                         col["route_text_color"])
                        for col in dr]

    print("reading trips..")
    with open(gtfs_dir/"trips.txt", 'r') as fin:
        dr = csv.DictReader(fin)
        trips_to_db = [(col["route_id"],
                        col["trip_id"],
                        col["service_id"],
                        col["trip_headsign"],
                        col["direction_id"],
                        col["block_id"],
                        col["shape_id"],
                        col["bikes_allowed"],
                        col["wheelchair_accessible"],
                        col["boarding_door"])
                       for col in dr]

    print("reading stop_times..")
    with open(gtfs_dir/"stop_times.txt", 'r') as fin:
        dr = csv.DictReader(fin)
        stop_times_to_db = [(col["trip_id"],
                             col["stop_id"],
                             col["arrival_time"],
                             col["departure_time"],
                             col["stop_sequence"],
                             col["stop_headsign"],
                             col["pickup_type"],
                             col["drop_off_type"],
                             col["shape_dist_traveled"])
                            for col in dr]

    print("reading stops..")
    with open(gtfs_dir/"stops.txt", 'r') as fin:
        dr = csv.DictReader(fin)
        stops_to_db = [(col["stop_id"],
                        col["stop_name"],
                        col["stop_lat"],
                        col["stop_lon"],
                        col["stop_code"],
                        col["location_type"],
                        col["parent_station"],
                        col["wheelchair_boarding"],
                        col["stop_direction"])
                       for col in dr]

    print("reading shapes..")
    with open(gtfs_dir/"shapes.txt", 'r') as fin:
        dr = csv.DictReader(fin)
        shapes_to_db = [(col["shape_id"],
                         col["shape_pt_sequence"],
                         col["shape_pt_lat"],
                         col["shape_pt_lon"],
                         col["shape_dist_traveled"])
                        for col in dr]

    print("reading pathways..")
    print()
    with open(gtfs_dir/"pathways.txt", 'r') as fin:
        dr = csv.DictReader(fin)
        pathways_to_db = [(col["pathway_id"],
                           col["pathway_mode"],
                           col["is_bidirectional"],
                           col["from_stop_id"],
                           col["to_stop_id"],
                           col["traversal_time"])
                          for col in dr]

    print("creating database: ", db_dir, db_name)
    con = sqlite3.connect(db_dir/db_name)
    cursorObj = con.cursor()

    print("creating tables..")
    try:
        cursorObj.execute("CREATE TABLE IF NOT EXISTS \
        agency (agency_id, agency_name, agency_url, agency_timezone,\
        agency_lang, agency_phone);")
        cursorObj.execute("CREATE TABLE IF NOT EXISTS \
        routes (agency_id, route_id, route_short_name,\
        route_long_name,route_type,route_desc,route_color,route_text_color);")
        cursorObj.execute("CREATE TABLE IF NOT EXISTS \
        trips (route_id,trip_id,service_id,trip_headsign,\
        direction_id,block_id,shape_id,wheelchair_accessible,\
        bikes_allowed,boarding_door);")
        cursorObj.execute("CREATE TABLE IF NOT EXISTS \
        stop_times (trip_id,stop_id,arrival_time,departure_time,\
        stop_sequence,stop_headsign,pickup_type,drop_off_type,\
        shape_dist_traveled);")
        cursorObj.execute("CREATE TABLE IF NOT EXISTS \
        stops (stop_id,stop_name,stop_lat,stop_lon,stop_code,\
        location_type,parent_station,wheelchair_boarding,stop_direction);")
        cursorObj.execute("CREATE TABLE IF NOT EXISTS \
        calendar_dates (service_id,date,exception_type);")
        cursorObj.execute("CREATE TABLE IF NOT EXISTS \
        shapes (shape_id,shape_pt_sequence,shape_pt_lat,\
        shape_pt_lon,shape_dist_traveled);")
        cursorObj.execute("CREATE TABLE IF NOT EXISTS \
        pathways (pathway_id,pathway_type,from_stop_id,\
        to_stop_id,traversal_time,wheelchair_traversal_time );")

    except (sqlite3.OperationalError, sqlite3.IntegrityError) as err:
        print(err)
        print("rolling back table creation...")
        con.rollback()

    print("inserting data..")
    try:
        cursorObj.executemany("INSERT INTO agency \
        (agency_id, agency_name, agency_url, agency_timezone,\
        agency_lang,agency_phone) \
        VALUES (?,?,?,?,?,?);", agency_to_db)
        cursorObj.executemany("INSERT INTO routes \
        (agency_id, route_id, route_short_name,route_long_name,\
        route_type,route_desc,route_color,route_text_color) \
        VALUES (?,?,?,?,?,?,?,?);", routes_to_db)
        cursorObj.executemany("INSERT INTO trips \
        (route_id, trip_id, service_id,trip_headsign,direction_id,\
        block_id,shape_id,bikes_allowed,wheelchair_accessible,boarding_door)\
        VALUES (?,?,?,?,?,?,?,?,?,?);", trips_to_db)
        cursorObj.executemany("INSERT INTO stop_times \
        (trip_id,stop_id,arrival_time,departure_time,stop_sequence,\
        stop_headsign,pickup_type,drop_off_type,shape_dist_traveled)\
        VALUES (?,?,?,?,?,?,?,?,?);", stop_times_to_db)
        cursorObj.executemany("INSERT INTO shapes\
        (shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon,\
        shape_dist_traveled) VALUES (?,?,?,?,?);", shapes_to_db)
        cursorObj.executemany("INSERT INTO pathways\
        (pathway_id,pathway_type,from_stop_id,to_stop_id,\
        traversal_time,wheelchair_traversal_time)\
        VALUES (?,?,?,?,?,?);", pathways_to_db)
        cursorObj.executemany("INSERT INTO stops\
        (stop_id,stop_name,stop_lat,stop_lon,stop_code,location_type,\
        parent_station,wheelchair_boarding,stop_direction)\
        VALUES (?,?,?,?,?,?,?,?,?);", stops_to_db)
        cursorObj.executemany("INSERT INTO calendar_dates\
        (service_id,date,exception_type) \
        VALUES (?,?,?);", calendar_dates_to_db)

    except sqlite3.OperationalError as OE:
        print(OE)
        print("rolling back table insertion...")
        con.rollback()

    print("creating index..")
    try:
        cursorObj.execute("CREATE INDEX \
        trips_route_index ON trips(trip_id);")
        cursorObj.execute("CREATE INDEX \
        triops_trip_index ON trips(trip_id);")
        cursorObj.execute("CREATE INDEX \
        trips_direction_index ON trips(direction_id);")
        cursorObj.execute("CREATE INDEX \
        trips_block_index ON trips(block_id);")
        cursorObj.execute("CREATE INDEX \
        stop_times_trip_index ON stop_times(trip_id);")
        cursorObj.execute("CREATE INDEX \
        stop_times_stop_index ON stop_times(stop_id);")
    except sqlite3.OperationalError as OE:
        print(OE)
        print("rolling back indexing...")
        con.rollback()

    con.commit()
    con.close()

    print("done")
    print(":"*15)

if __name__ == '__main__':
    create_db()
