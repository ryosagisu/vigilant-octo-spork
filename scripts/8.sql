-- most trips by hour
SELECT
  datahour,
  COUNT(1) AS total_trips
FROM
  spectacle.bikeshare_trips_external
GROUP BY
  datahour
ORDER BY
  total_trips DESC
LIMIT 1
