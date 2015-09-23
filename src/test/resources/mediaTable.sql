-- for postgres
CREATE TABLE media
(
  name character varying(50),
  added timestamp with time zone,
  content oid,
  transcription text
)
WITH (
  OIDS=FALSE
);
ALTER TABLE media OWNER TO proxyuser;

-- for transbase
CREATE TABLE media
(
  name character varying(50),
  added timestamp,
  content BLOB,
  transcription CLOB
);
