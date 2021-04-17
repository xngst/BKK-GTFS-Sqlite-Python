# BKK-GTFS-Sqlite lekerdezesek-pythonnal

"A menetrendi adatok hozzáférhetősége lehetővé teszi, hogy bárki meglevő szoftverébe integrálja az adatokat, illetve olyan új szoftvert fejlesszen, ami ezen adatokra épül. A rendszeresen frissített adatbázis néhány hónapra előre tartalmaz minden indulási és érkezési adatot a teljes nappali és éjszakai hálózatra, minden alágazatban (busz, trolibusz, villamos, metró, HÉV, hajó). Segítségével megjeleníthető a vonalhálózat, valamint minden megálló pontos helye a GPS koordináták segítségével."  
https://bkk.hu/fejlesztesek/fejlesztoknek/  
GTFS file forrás: https://bkk.hu/apps/gtfs/

## A jupyter notebookok elérhetőek mardownként is [itt](https://github.com/xngst/BKK-GTFS-Sqlite-Python/tree/main/markdowns)

### GTFS TÁBLÁK ÁLTALÁNOS SÉMÁJA:
[google reference](https://developers.google.com/transit/gtfs/reference/)

    agency
        agency_id
        agency_name
        agency_url
        agency_timezone
        agency_lang
        agency_phone

    routes
        agency_id
        route_id
        route_short_name
        route_long_name
        route_type
        route_desc
        route_color
        route_text_color

    trips
        route_id
        trip_id
        service_id
        trip_headsign
        direction_id
        block_id
        shape_id
        bikes_allowed
        wheelchair_accessible
        boarding_door

    stop_times
        trip_id,
        stop_id,
        arrival_time
        departure_time
        stop_sequence
        stop_headsign
        pickup_type
        drop_off_type
        shape_dist_traveled

    shapes
        shape_id,
        shape_pt_sequence,
        shape_pt_lat,
        shape_pt_lon,
        shape_dist_traveled

    pathways
        pathway_id
        pathway_type
        from_stop_id
        to_stop_id
        traversal_time
        wheelchair_traversal_time

    stops
        stop_id
        stop_name
        stop_lat
        stop_lon
        stop_code
        location_type
        parent_station
        wheelchair_boarding
        stop_direction

    calendar_dates
        service_id
        date
        exception_type
