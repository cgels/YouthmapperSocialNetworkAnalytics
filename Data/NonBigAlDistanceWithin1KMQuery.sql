SELECT
    A.id,
    A.version,
    A.author,
    B.id,
    B.version,
    B.author,
    ST_X(A.point),
    ST_Y(A.point),
    ST_X(B.point),
    ST_Y(B.point),
    ST_Distance(A.point, B.point) AS dist
  FROM (SELECT
          C.id,
          C.version,
          C.point,
          D.timestamp,
          D.author
        FROM (node C
          JOIN osm_entity D ON C.id = D.id AND C.version = D.version)
        WHERE author != 'bigalxyz123' AND extract(YEAR FROM D.timestamp) >= 2017) A
    JOIN (SELECT
            N.id,
            N.version,
            N.point,
            O.author
          FROM (node N
            JOIN osm_entity O ON N.id = O.id AND N.version = O.version)
          WHERE extract(YEAR FROM O.timestamp) >= 2017 AND author != 'bigalxyz123') B
      ON ST_DWithin(A.point, B.point, 1)
  WHERE A.id != B.id
  LIMIT 500000;
