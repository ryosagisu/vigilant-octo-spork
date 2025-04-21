-- average trip duration per station pair
SELECT
  start_station_name,
  end_station_name,
  AVG(duration_minutes) AS avg_duration_minutes
FROM
  spectacle.bikeshare_trips_external
GROUP BY
  start_station_name,
  end_station_name
ORDER BY
  avg_duration_minutes DESC
LIMIT 1
