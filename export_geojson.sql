/******************************************************************************
 * Exports trip as GeoJSON files for kepler.gl
 * - One file per trip: trip_<tripid>.geojson
 * Geometry is a LineString with coordinates [lon, lat, 0, epoch_ts]
 *
 * Usage (psql):
 *   SELECT export_trip('/tmp/trips_kepler/', tripId);
 *   SELECT export_municipalities('/tmp/trips_kepler/');
 *****************************************************************************/

DROP FUNCTION IF EXISTS export_trip(fullpath text, trip_id bigint);
CREATE OR REPLACE FUNCTION export_trip(fullpath text, trip_id bigint)
RETURNS text AS $$
DECLARE
  dir     text;
  outpath text;
BEGIN
  dir := regexp_replace(fullpath, '/+$', '') || '/';
  BEGIN
    PERFORM 1 FROM pg_ls_dir(dir) LIMIT 1;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Output directory "%" does not exist or is not accessible', dir;
  END;  
  outpath := dir || 'trip_' || trip_id || '.geojson';

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
  $q$, trip_id, outpath);

  RETURN format('Exported trip %s to %s', trip_id, outpath);
END;
$$ LANGUAGE plpgsql;


-- Export Municipalities 
DROP FUNCTION IF EXISTS export_municipalities(fullpath text);
CREATE OR REPLACE FUNCTION export_municipalities(fullpath text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  dir     text;
  outpath text;
BEGIN  
  dir := regexp_replace(fullpath, '/+$', '') || '/';
  BEGIN
    PERFORM 1 FROM pg_ls_dir(dir) LIMIT 1;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Output directory "%" does not exist or is not accessible', dir;
  END;

  outpath := dir || 'municipalities.geojson';

  EXECUTE format($Q$
    COPY (
      SELECT (
        jsonb_build_object(
          'type','FeatureCollection',
          'features',
          jsonb_agg(
            jsonb_build_object(
              'type','Feature',
              'properties', jsonb_build_object(
                'MunicipalityName', m.MunicipalityName,
                'Population', m.Population,
                'PopDensityKm2', m.PopDensityKm2
              ),
              'geometry',
                ST_AsGeoJSON(
                  ST_Transform(m.MunicipalityGeo, 4326)
                )::jsonb
            )
          )
        )
      )::text
      FROM Municipalities m
    ) TO %L
  $Q$, outpath);

  RETURN format('Exported Municipalities to %s', outpath);
END;
$$;