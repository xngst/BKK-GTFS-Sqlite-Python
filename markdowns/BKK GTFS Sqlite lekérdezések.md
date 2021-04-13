# BKK GTFS Sqlite lekérdezések


```python
import sqlite3
import time
import datetime
import pandas as pd

from pathlib import Path
from time import strftime

DB_DIR = Path("/home/BKK adatbázis/")
GTFS_DIR = Path("/home//GTFS/SQLITE_DB")
```

## Milyen táblákat tartalmaz a GTFS adabázis?


```python
def tables_in_sqlite_db(conn):
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table';")
    tables = [v[0] for v in cursor.fetchall() if v[0] != "sqlite_sequence"]
    cursor.close()
    return tables
```


```python
con = sqlite3.connect(GTFS_DIR/"bkk_gtfs_db.db")
tables_in_sqlite_db(con)
```




    ['agency',
     'routes',
     'trips',
     'stop_times',
     'stops',
     'calendar_dates',
     'shapes',
     'pathways']



## Milyen megállói vannak egy bizonyos járatnak?


```python
def fetchall(db_path:str, db:str, query:str) -> list:
    """
    path = path to db
    db = database
    query = SQL query as string
    returns list with fetched elements 
    """
    path = Path(db_path)
    con = sqlite3.connect(path/db)
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    con.close()
    result = [row for row in rows]
    
    return result
```


```python
def route_stops(db_path:str, db:str, route:str) -> set:
    """
    returns set of route stops
    """
    query = f'SELECT DISTINCT stops.stop_name\
    FROM routes \
    LEFT OUTER JOIN trips ON trips.route_id = routes.route_id \
    LEFT OUTER JOIN stop_times ON stop_times.trip_id = trips.trip_id \
    LEFT OUTER JOIN stops ON stops.stop_id = stop_times.stop_id \
    WHERE routes.route_short_name = "{route}"'
    result = set(fetchall(db_path, db, query))
    
    return result
```


```python
route = "2"
route_stops(GTFS_DIR, "bkk_gtfs_db.db", route)
```




    {('Boráros tér H',),
     ('Eötvös tér',),
     ('Ferencváros vá. - Málenkij Robot Eh.',),
     ('Fővám tér M',),
     ('Haller utca / Mester utca',),
     ('Haller utca / Soroksári út',),
     ('Jászai Mari tér',),
     ('Kossuth Lajos tér M',),
     ('Közvágóhíd H',),
     ('Mester utca / Könyves Kálmán körút',),
     ('Március 15. tér',),
     ('Müpa - Nemzeti Színház H',),
     ('Országház, látogatóközpont',),
     ('Széchenyi István tér',),
     ('Vigadó tér',),
     ('Vágóhíd utca',),
     ('Zsil utca',)}




```python
def terminal_stations(db_path:str, db:str, route:str) -> tuple:
    """
    """
    query = 'SELECT route_desc FROM routes\
             WHERE "route_short_name"="{0}"'.format(route)
    result = fetchall(db_path, db, query)[0]
    return result
```


```python
route = "2"
terminal_stations(GTFS_DIR, "bkk_gtfs_db.db", route)
```




    ('Jászai Mari tér / Közvágóhíd H',)



### Mikor érkezik a következő járat a megállóba?


```python
def route_next_arrival(db_path:str, 
                       db:str,
                       cur_time: datetime.datetime, 
                       route: str, 
                       stop_name: str) -> str:
    """
    """
    cur_date = cur_time.strftime("%Y%m%d")
    cur_time = datetime.datetime.now().strftime("%H:%M:%S")
    query = f'SELECT calendar_dates.date, stop_times.arrival_time, stops.stop_name \
                    FROM stops \
                    LEFT OUTER JOIN stop_times ON stop_times.stop_id = stops.stop_id \
                    LEFT OUTER JOIN trips ON trips.trip_id = stop_times.trip_id \
                    LEFT OUTER JOIN calendar_dates ON calendar_dates.service_id = trips.service_id \
                    LEFT OUTER JOIN routes ON routes.route_id = trips.route_id \
                    WHERE calendar_dates.date = "{cur_date}" AND route_short_name = "{route}"'
    result = fetchall(db_path, db, query)
    df = pd.DataFrame(result, columns = ["Date", "Arrival", "Stop"])
    newdf = df[(df["Arrival"] > cur_time) & (df["Stop"] == stop_name)]
    newdf.sort_values(by="Arrival").reset_index(inplace=True, drop=True)
    arrival = newdf.iloc[0]["Arrival"]
    return arrival
```


```python
print("pontos idő:", datetime.datetime.now().strftime("%H:%M:%S"))
print("következő járat:")
route_next_arrival(db_path=GTFS_DIR,
                   db="bkk_gtfs_db.db",
                   cur_time = datetime.datetime.now(),
                   route="9",
                   stop_name="Astoria M")
```

    pontos idő: 20:41:55
    következő járat:





    '20:44:00'




```python

```
