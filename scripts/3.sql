-- top 5 starting stations by number of trips
SELECT
  start_station_name,
  COUNT(1) AS total_trips
FROM
  spectacle.bikeshare_trips_external
GROUP BY
  start_station_name
ORDER BY
  total_trips DESC
LIMIT 5
