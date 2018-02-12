Kafka examples
-----

## docker-compose Usage

```
docker-compose -f confluent-all.yml up -d               # Start servies from file selected file in background
docker-compose -f confluent-all.yml down                # Stop and remove containers, networks, images, and volumes
docker-compose -f confluent-all.yml restart <service>   # Restart container-name
``` 

## Exposed ports

| Component                         | Port |
|-----------------------------------|------|
| ZooKeeper                         | 2181 |
| Apache Kafka brokers (plain text) | 9092 |
| Kafka Connect REST API            | 8083 |
| REST Proxy                        | 8082 |
| Schema Registry REST API          | 8081 |


## Reference

https://docs.confluent.io/current/kafka-rest/docs/api.html
https://docs.confluent.io/current/schema-registry/docs/api.html
https://docs.confluent.io/current/connect/restapi.html#connectors
