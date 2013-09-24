DROP EXTENSION multicorn cascade;

CREATE EXTENSION multicorn;

CREATE SERVER log_server
  FOREIGN DATA WRAPPER multicorn
  OPTIONS (wrapper 'logfdw.LogFDW');

CREATE FOREIGN TABLE logtable (
  message VARCHAR
) SERVER log_server OPTIONS (
  log_file './log'
);

-- SELECT * from logtable;

SELECT count(1) from logtable;

CREATE FOREIGN TABLE logtable_with_severity (
  severity VARCHAR,
  message VARCHAR
) SERVER log_server OPTIONS (
  log_file './log',
  line_pattern '^(?:(\w*):) (.*)'
);

SELECT * from logtable_with_severity;

SELECT distinct severity from logtable_with_severity;
