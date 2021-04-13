# BKK GTFS CSV fileok SQLite adatbázisba írása

#### file forrás: https://bkk.hu/apps/gtfs/


```python
import csv
import os
import sqlite3
import zipfile

from pathlib import Path
```


```python
#todo: pythonnal letölteni a zip file-t
```


```python
DB_DIR = Path("/home/xunguist/Ipython_Notebooks/BKK adatbázis/GTFS/SQLITE_DB")
ZIP_DIR = Path("/home/xunguist/Ipython_Notebooks/BKK adatbázis/GTFS")
GTFS_DIR = ZIP_DIR/"extracted"
zip_file = ZIP_DIR/"budapest_gtfs_12_04_2021.zip"
```


```python
#ZIP kibontása
with zipfile.ZipFile(zip_file) as zf:
    zf.extractall(GTFS_DIR)
```

# GTFS CSV-k beolvasása


```python
%%time

with open(GTFS_DIR/"agency.txt",'r') as fin:
    dr = csv.DictReader(fin)
    agency_to_db = [(col["agency_id"], 
              col["agency_name"],
              col["agency_url"],
              col["agency_timezone"],
              col["agency_lang"],
              col["agency_phone"]
                    ) for col in dr]
    
with open(GTFS_DIR/"calendar_dates.txt",'r') as fin:
    dr = csv.DictReader(fin)
    calendar_dates_to_db = [(col["service_id"], 
              col["date"],
              col["exception_type"])
            for col in dr]

with open(GTFS_DIR/"routes.txt",'r') as fin:
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
    
with open(GTFS_DIR/"trips.txt",'r') as fin:
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
    
with open(GTFS_DIR/"stop_times.txt",'r') as fin:
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

with open(GTFS_DIR/"stops.txt",'r') as fin:
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
    
with open(GTFS_DIR/"shapes.txt",'r') as fin:
    dr = csv.DictReader(fin)
    shapes_to_db = [(col["shape_id"], 
              col["shape_pt_sequence"],
              col["shape_pt_lat"],
              col["shape_pt_lon"],
              col["shape_dist_traveled"])
            for col in dr]
    
with open(GTFS_DIR/"pathways.txt",'r') as fin:
    dr = csv.DictReader(fin)
    pathways_to_db = [(col["pathway_id"], 
              col["pathway_mode"],
              col["is_bidirectional"],
              col["from_stop_id"],
              col["to_stop_id"],
              col["traversal_time"])
            for col in dr]
```

    CPU times: user 30.4 s, sys: 1.66 s, total: 32.1 s
    Wall time: 31.7 s


# SQLite adatbázis készítése


```python
%%time

db_name = "bkk_gtfs_db.db"

con = sqlite3.connect(DB_DIR/db_name)
cursorObj = con.cursor()

try:
    cursorObj.execute("CREATE TABLE IF NOT EXISTS \
    agency (agency_id, agency_name, agency_url, agency_timezone,agency_lang, agency_phone);")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS \
    routes (agency_id, route_id, route_short_name,route_long_name,route_type,route_desc,route_color,route_text_color);")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS \
    trips (route_id, trip_id, service_id,trip_headsign,direction_id,block_id,shape_id,bikes_allowed,wheelchair_accessible,boarding_door);")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS \
    stop_times (trip_id,stop_id,arrival_time,departure_time,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled);")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS \
    stops (stop_id,stop_name,stop_lat,stop_lon,stop_code,location_type,parent_station,wheelchair_boarding,stop_direction);")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS \
    calendar_dates (service_id,date,exception_type);")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS \
    shapes (shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon,shape_dist_traveled);")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS \
    pathways (pathway_id PRIMARY KEY,pathway_type,from_stop_id,to_stop_id,traversal_time,wheelchair_traversal_time );")
    
except (sqlite3.OperationalError, IntegrityError) as err:
    print (err)
    print("rolling back table creation...")
    con.rollback()    

try:
    cursorObj.executemany("INSERT INTO agency \
    (agency_id, agency_name, agency_url, agency_timezone,agency_lang,agency_phone) VALUES (?,?,?,?,?,?);", agency_to_db)
    cursorObj.executemany("INSERT INTO routes \
    (agency_id, route_id, route_short_name,route_long_name,route_type,route_desc,route_color,route_text_color) VALUES (?,?,?,?,?,?,?,?);", routes_to_db)
    cursorObj.executemany("INSERT INTO trips \
    (route_id, trip_id, service_id,trip_headsign,direction_id,block_id,shape_id,bikes_allowed,wheelchair_accessible,boarding_door) VALUES (?,?,?,?,?,?,?,?,?,?);", trips_to_db)
    cursorObj.executemany("INSERT INTO stop_times \
    (trip_id,stop_id,arrival_time,departure_time,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled) VALUES (?,?,?,?,?,?,?,?,?);", stop_times_to_db)
    cursorObj.executemany("INSERT INTO shapes \
    (shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon,shape_dist_traveled) VALUES (?,?,?,?,?);", shapes_to_db)
    cursorObj.executemany("INSERT INTO pathways \
    (pathway_id,pathway_type,from_stop_id,to_stop_id,traversal_time,wheelchair_traversal_time) VALUES (?,?,?,?,?,?);", pathways_to_db)
    cursorObj.executemany("INSERT INTO stops \
    (stop_id,stop_name,stop_lat,stop_lon,stop_code,location_type,parent_station,wheelchair_boarding,stop_direction) VALUES (?,?,?,?,?,?,?,?,?);", stops_to_db)
    cursorObj.executemany("INSERT INTO calendar_dates \
    (service_id,date,exception_type) VALUES (?,?,?);", calendar_dates_to_db)

except sqlite3.OperationalError as OE:
    print (OE)
    print("rolling back table insertion...")
    con.rollback()
    
try:
    cursorObj.execute("CREATE INDEX trips_route_index ON trips(trip_id);")
    cursorObj.execute("CREATE INDEX triops_trip_index ON trips(trip_id);")
    cursorObj.execute("CREATE INDEX trips_direction_index ON trips(direction_id);")
    cursorObj.execute("CREATE INDEX trips_block_index ON trips(block_id);")
    cursorObj.execute("CREATE INDEX stop_times_trip_index ON stop_times(trip_id);")
    cursorObj.execute("CREATE INDEX stop_times_stop_index ON stop_times(stop_id);")
except sqlite3.OperationalError as OE:
    print (OE)
    print("rolling back indexing...")
    con.rollback()  

con.commit()
con.close()
```

    CPU times: user 16 s, sys: 1.36 s, total: 17.3 s
    Wall time: 17.7 s


## file statisztika


```python
st_size = round(os.stat(zip_file)[6]/1000000,2)
print(f"ZIP file : {st_size} MB")
print("#"*27)
tot = 0
for file in os.listdir(GTFS_DIR):
    print(file, end=" ")
    part_size = os.stat(GTFS_DIR/file)[6]/1000000
    print(f": {round(part_size,2)} MB")
    tot += round(part_size, 2)
print(f"*összes unzippelt: {tot} MB")
print("#"*27)
st_size = round(os.stat(DB_DIR/db_name)[6]/1000000,2)
print(f"{db_name} : {st_size} MB")
```

    ZIP file : 42.24 MB
    ###########################
    stops.txt : 0.33 MB
    pathways.txt : 0.01 MB
    agency.txt : 0.0 MB
    calendar_dates.txt : 0.34 MB
    routes.txt : 0.03 MB
    trips.txt : 18.94 MB
    stop_times.txt : 280.12 MB
    feed_info.txt : 0.0 MB
    shapes.txt : 13.62 MB
    *összes unzippelt: 313.39 MB
    ###########################
    bkk_gtfs_db.db : 517.14 MB



```python

```
