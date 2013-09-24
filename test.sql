CREATE SERVER log_server
  FOREIGN DATA WRAPPER multicorn
  OPTIONS (wrapper 'logfdw.LogFDW');

CREATE FOREIGN TABLE logtable (
  ts TIMESTAMP,
  message VARCHAR
) SERVER log_server;

SELECT * from logtable;
