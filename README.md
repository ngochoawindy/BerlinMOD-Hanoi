# BerlinMOD-Hanoi Data for MobilityDB

[MobilityDB](https://github.com/ULB-CoDE-WIT/MobilityDB) is an open source software program that adds support for temporal and spatio-temporal objects to [PostgreSQL](https://www.postgresql.org/) and its spatial extension [PostGIS](http://postgis.net/).

This repository adapts the [BerlinMOD benchmark](https://github.com/MobilityDB/MobilityDB-BerlinMOD) to **Hanoi (Vietnam)** using **OSM data** and MobilityDB.  
It provides tools to generate synthetic mobility datasets, convert them into GeoJSON, and visualize them with [**Kepler.gl**](https://kepler.gl/) or **QGIS** + [**MOVE plugin**](https://github.com/MobilityDB/move).

Visualization of a trip:

- With Kepler  
  <img src="https://github.com/user-attachments/assets/148146c4-a882-4051-a0be-cddef9bc37a5" width="800" />

- With QGIS + MobilityDB-Move  
  <img src="https://github.com/user-attachments/assets/427bdcd7-4d9b-43fd-b139-6515b5ef6469" width="800" />

Visualization of municipalities in Hanoi with Kepler:  

<img src="https://github.com/user-attachments/assets/7df390f4-0024-453c-b54d-5e2e535fcadf" width="800" />

---

## 1. Getting the OSM Map of Hanoi

First, download OpenStreetMap (OSM) data of Hanoi with bbox setting as below:  

```bash
CITY="hanoi"
BBOX="105.28,20.56,106.07,21.384"
wget -O "${CITY}.osm" "http://www.overpass-api.de/api/xapi?*[bbox=${BBOX}][@meta]"
```

Create database hanoi, and add all extension needed:

```bash
# in a console:
createdb -h localhost -p 5432 -U dbowner hanoi
# replace localhost with your database host, 5432 with your port,
# and dbowner with your database user
psql -h localhost -p 5432 -U dbowner -d hanoi -c 'CREATE EXTENSION hstore'
# adds the hstore extension needed by osm2pgsql
psql -h localhost -p 5432 -U dbowner -d hanoi -c 'CREATE EXTENSION MobilityDB CASCADE'
# adds the PostGIS and the MobilityDB extensions to the database
psql -h localhost -p 5432 -U dbowner -d hanoi -c 'CREATE EXTENSION pgRouting'
# adds the pgRouting extension
```

Using mapconfig.xml in MobilityDB-BerlinMOD to select the roads type, load  the map and convert it into a routable network topology format: 
```bash
osm2pgrouting -h localhost -p 5432 -U dbowner -W passwd -f hanoi.osm --dbname hanoi \
-c MobilityDB-BerlinMOD/BerlinMOD/mapconfig.xml
```

## 2. Generating Data for Hanoi 
Use the hanoi_preparedata.sql script provided in this reposity to make the data realistic with real population statistics in every administrative region in Hanoi. 

```bash
osm2pgsql -c -H localhost -P 5432 -U dbowner -W -d hanoi hanoi.osm
# loads all layers in the osm file, including the adminstrative regions
psql -h localhost -p 5432 -U dbowner -d hanoi -f hanoi_preparedata.sql
# samples home and work nodes, transforms data to SRID 3857, does further data preparation
```
Then execute generator of BerlinMOD to get synthetic data for Hanoi: 
```bash
psql -h localhost -p 5432 -U dbowner -d hanoi -f MobilityDB-BerlinMOD/BerlinMOD/berlinmod_datagenerator.sql
psql -h localhost -p 5432 -U dbowner -d hanoi \
-c 'select berlinmod_generate(scaleFactor := 0.005)'
#generate data with a specific scale
```
## 3. Exporting Geojson for visualization with keple.gl
We provide SQL functions to export municipalities/trips as GeoJSON for Kepler.gl: 

```bash
psql -h localhost -p 5432 -U dbowner -d hanoi -f export_geojson.sql
psql -h localhost -p 5432 -U dbowner -d hanoi \
-c 'SELECT export_trip('path', tripID)'
# Get trip_<tripId>.geojson file
psql -h localhost -p 5432 -U dbowner -d hanoi \
-c 'SELECT export_municipalities('path')'
# Get municipalities.geojson
```
## 4. Generated Datasets

| Scale Factor | Vehicles | Days | Trips | File | Size |
|--------------|---------:|-----:|------:|-----|-----:|
| SF 0.01 |   200 | 5 |  2,903 | [hanoi_sf0.01.zip](https://drive.google.com/file/d/1qOBRVbmd_15F2zt8iBbAgLIrU6NsNdaV/view?usp=drive_link) | 214.6 MB |
| SF 0.02 |   283 | 6 |  4,641 | [hanoi_sf0.02.zip](https://drive.google.com/file/d/1G1HOqh4_WZcM1FxXXfqZSHwdb98dvzLi/view?usp=drive_link) | 310.4 MB |
| SF 0.05 |   447 | 8 |  9,491 | [hanoi_sf0.05.zip](https://drive.google.com/file/d/1SuLZ8B4SrA6JD2CiaxxGRLoUCg_Z97SY/view?usp=drive_link) | 626.6 MB |
| SF 0.1  |   632 | 11 | 18,910 | [hanoi_sf0.1.zip](https://drive.google.com/file/d/1lniEu0w5uM9yK52TmGVfeN9se9vjoUXz/view?usp=drive_link) | 1.27 GB |

Trips geojson files: [trips.zip](https://drive.google.com/file/d/1KIw2O3msoOyIKR_EiLxXIjwezGy9XPgd/view?usp=drive_link) (There are 2903 files corresponding to 2903 trips for scale 0.01)

Municipalities geojson files: [municipalities.geojson](https://drive.google.com/file/d/1ZYZuM68nFPreFsfChoqeaN8n2sziB2-s/view?usp=drive_link)
