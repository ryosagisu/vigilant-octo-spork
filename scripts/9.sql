-- most trips in a day
SELECT
  datadate,
  COUNT(1) AS total_trips
FROM
  spectacle.bikeshare_trips_external
GROUP BY
  datadate
ORDER BY
  total_trips DESC
LIMIT 1
