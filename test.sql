DROP EXTENSION multicorn cascade;

CREATE EXTENSION multicorn;

CREATE SERVER log_server
  FOREIGN DATA WRAPPER multicorn
  OPTIONS (wrapper 'logfdw.LogFDW');

CREATE FOREIGN TABLE logtable (
  date timestamp OPTIONS (is_timestamp 't'),
  severity VARCHAR,
  message VARCHAR
) SERVER log_server OPTIONS (
  log_file './log',
  line_pattern '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?:(\w*):) (.*)'
);

CREATE FOREIGN TABLE logtable_without_date (
  date timestamp,
  severity VARCHAR,
  message VARCHAR
) SERVER log_server OPTIONS (
  log_file './log',
  line_pattern '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (?:(\w*):) (.*)'
);




select * from logtable where date < '2013-09-24 13:43:46';


select * from logtable_without_date where date < '2013-09-24 13:43:46';

