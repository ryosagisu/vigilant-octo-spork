-- total number of trips per day
SELECT
  datadate,
  AVG(duration_minutes) AS avg_duration_minutes
FROM
  spectacle.bikeshare_trips_external
GROUP BY
  datadate
ORDER BY
  datadate
