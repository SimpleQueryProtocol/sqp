-- for transbase
CREATE TABLE birthday
(
  birthday DATETIME[MO:DD],
  firstname char(20),
  picture BITS2(*)
)

-- for postgres
CREATE TABLE birthday
(
  birthday point,
  firstname char(20),
  picture bytea
)

ALTER TABLE birthday OWNER TO proxyuser;

