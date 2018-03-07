#!/usr/bin/env bash

------- https://github.com/confluentinc/ksql/blob/master/docs/syntax-reference.md

ksql-cli local --bootstrap-server kafka:9092 --schema-registry-url http://schema-registry:8081

SHOW PROPERTIES;

SET 'auto.offset.reset' = 'earliest';



CREATE STREAM orders \
  WITH (KAFKA_TOPIC='lab_wc18_orders', \
        VALUE_FORMAT='AVRO');

SELECT * FROM orders LIMIT 5;        


CREATE TABLE retailers \
  WITH (KAFKA_TOPIC='lab_wc18_retailers', \
        VALUE_FORMAT='AVRO', KEY='retailerId');
           
SELECT * FROM retailers LIMIT 5;



----------------------- group

CREATE TABLE top_products AS \
SELECT product, count(product) FROM orders \
WINDOW TUMBLING (SIZE 20 SECONDS) \
GROUP BY product;


SELECT name, count(location) FROM retailers \
WINDOW TUMBLING (SIZE 20 SECONDS) \
GROUP BY location;
