/******************************************************************************
 * Exports all trips as GeoJSON files for kepler.gl
 * - One file per trip: trip_<tripid>.geojson
 * Geometry is a LineString with coordinates [lon, lat, 0, epoch_ts]
 *
 * Usage (psql):
 *   \i berlinmod_export_kepler.sql
 *   SELECT export_trips('/tmp/trips_kepler/');
 *   SELECT export_municipalities('tmp/trips_kepler');
 *****************************************************************************/

DROP FUNCTION IF EXISTS export_trips(fullpath text);
CREATE OR REPLACE FUNCTION export_trips(fullpath text)
RETURNS text AS $$
DECLARE
  startTime timestamptz;
  endTime   timestamptz;
  dir       text;
  r         RECORD;
  outpath   text;
BEGIN
  startTime := clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Exporting trips to GeoJSON to execute kepler.gl';
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO 'Output directory: %', fullpath;
  RAISE INFO '------------------------------------------------------------------';

  IF fullpath IS NULL OR btrim(fullpath) = '' THEN
    RAISE EXCEPTION 'Output directory cannot be empty';
  END IF;
  
  dir := regexp_replace(fullpath, '/+$', '');
  dir := dir || '/';
  PERFORM 1 FROM pg_ls_dir(dir) LIMIT 1;

  -- Loop over all trips; create one file per trip
  FOR r IN
    SELECT DISTINCT t.tripid
    FROM trips t
    ORDER BY t.tripid
  LOOP
    outpath := dir || 'trip_' || r.tripid || '.geojson';

    EXECUTE format($q$
      COPY (
        WITH pts AS (
          SELECT
            t.tripid,
            t.vehicleid,
            ST_X(ST_Transform(getValue(i), 4326)) AS lon,
            ST_Y(ST_Transform(getValue(i), 4326)) AS lat,
            EXTRACT(EPOCH FROM getTimestamp(i))::bigint AS ts
          FROM trips t
          CROSS JOIN LATERAL unnest(instants(t.trip)) AS i
          WHERE t.tripid = %s
        ),
        features AS (
          SELECT
            tripid,
            vehicleid,
            MIN(ts) AS t0,
            json_build_object(
              'type','Feature',
              'properties', json_build_object(
                'tripId', tripid,
                'vehicleId', vehicleid,
                't0', MIN(ts),
                't1', MAX(ts),
                'points', COUNT(*)
              ),
              'geometry', json_build_object(
                'type','LineString',
                -- [lon, lat, altitude, epoch_ts]
                'coordinates', json_agg(json_build_array(lon, lat, 0, ts) ORDER BY ts)
              )
            ) AS f
          FROM pts
          GROUP BY tripid, vehicleid
        )
        SELECT (json_build_object(
          'type','FeatureCollection',
          'features', json_agg(f ORDER BY t0)
        ))::text
        FROM features
      ) TO %L
    $q$, r.tripid, outpath);    
  END LOOP;

  endTime := clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO 'Execution finished at %', endTime;
  RAISE INFO 'Execution time %', endTime - startTime;
  RAISE INFO '------------------------------------------------------------------';

  RETURN 'OK';
END;
$$ LANGUAGE plpgsql;

-- Export Municipalities
DROP FUNCTION IF EXISTS export_municipalities(fullpath text, srid_out int);
CREATE OR REPLACE FUNCTION export_municipalities(
  fullpath  text,
  srid_out  int DEFAULT 4326
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  startTime timestamptz;
  endTime   timestamptz;
  dir       text;
  outpath   text;
BEGIN
  startTime := clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Exporting Municipalities to GeoJSON';
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO 'Output directory: %', fullpath;
  RAISE INFO '------------------------------------------------------------------';

  dir := regexp_replace(fullpath, '/+$', '') || '/'; 
  PERFORM 1 FROM pg_ls_dir(dir) LIMIT 1; 
  outpath := dir || 'municipalities.geojson'; 
  EXECUTE format($Q$ 
    COPY ( 
      SELECT ( 
        jsonb_build_object( 
          'type','FeatureCollection', 'features', 
          jsonb_agg( 
            jsonb_build_object( 'type','Feature', 'properties', 
            jsonb_build_object( 
              'MunicipalityName', m.MunicipalityName, 
              'Population', m.Population, 
              'PopDensityKm2', m.PopDensityKm2 
              ), 
              'geometry', ST_AsGeoJSON( 
                ST_Transform( 
                  m.MunicipalityGeo, 
                  %s 
                ) 
              )::jsonb 
            ) 
          ) 
        ) 
      )::text 
    FROM Municipalities m ) 
  TO %L $Q$, srid_out, outpath);

  endTime := clock_timestamp();
  RAISE INFO '------------------------------------------------------------------';
  RAISE INFO 'Execution started at %', startTime;
  RAISE INFO 'Execution finished at %', endTime;
  RAISE INFO 'Execution time %', endTime - startTime;
  RAISE INFO '------------------------------------------------------------------';

  RETURN format('Exported Municipalities to %s', outpath);
END;
$$;