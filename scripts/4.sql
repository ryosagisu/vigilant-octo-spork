-- average trip duration per hour per day
SELECT
  datadate,
  datahour,
  AVG(duration_minutes) AS avg_duration_minutes
FROM
  spectacle.bikeshare_trips_external
GROUP BY
  datadate,
  datahour
ORDER BY
  datadate,
  datahour
