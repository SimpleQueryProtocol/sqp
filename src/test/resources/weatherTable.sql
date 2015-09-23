-- for postgres
-- DROP TABLE weather;
CREATE TABLE weather
(
  city character varying(80),
  temp_lo integer,
  temp_hi integer,
  prob real,
  date date
)

-- for transbase. Note that real in tb means "double", while float is 4 byte.
-- for postgres it's the other way around: float defaults to "double", while real is a 4 byte.
CREATE TABLE weather
(
  city character varying(80),
  temp_lo integer,
  temp_hi integer,
  prob float,
  date date
)

-- ALTER TABLE weather OWNER TO proxyuser;

