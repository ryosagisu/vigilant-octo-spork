-- total number of trips per day
SELECT
  datadate,
  COUNT(1) AS total_trips
FROM
  spectacle.bikeshare_trips_external
GROUP BY
  datadate
ORDER BY
  datadate
