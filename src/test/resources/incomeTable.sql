-- for postgres only

CREATE TABLE income
(
  "timestamp" timestamp(3) with time zone NOT NULL,
  value money NOT NULL,
  "position" point DEFAULT point((0.0)::double precision, (0.0)::double precision),
  attempts smallint
)

ALTER TABLE income OWNER TO proxyuser;
