DROP EXTENSION multicorn cascade;

CREATE EXTENSION multicorn;

CREATE SERVER index_server
  FOREIGN DATA WRAPPER multicorn
  OPTIONS (wrapper 'logfdw.IndexedFDW');


CREATE TABLE ref_values (
  id integer,
  value varchar
);

CREATE FOREIGN TABLE without_index (
  id integer,
  value varchar
) SERVER index_server OPTIONS (
  is_indexed 'False'
);

CREATE FOREIGN TABLE with_index (
  id integer,
  value varchar
) SERVER index_server OPTIONS (
  is_indexed 'True'
);

explain select * from with_index where id = 2;
explain select * from without_index where id = 2;

explain select * from with_index inner join ref_values using(id);
explain select * from without_index inner join ref_values using(id);
