-- total number of trips per month
SELECT
  FORMAT_DATE('%Y-%m', datadate) AS datamonth,
  COUNT(1) AS total_trips
FROM
  spectacle.bikeshare_trips_external
GROUP BY
  datamonth
ORDER BY
  datamonth
