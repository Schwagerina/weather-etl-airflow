SELECT city, geo_lat, geo_lon
FROM stg.city_csv
WHERE city IN (
  'Волгоград',
  'Уфа',
  'Казань',
  'Новосибирск'
);