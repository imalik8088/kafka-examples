Kafka examples
-----

## docker-compose Usage

```
docker-compose -f docker/confluent-all.yml up -d               # Start servies from file selected file in background
docker-compose -f docker/confluent-all.yml down                # Stop and remove containers, networks, images, and volumes
docker-compose -f docker/confluent-all.yml restart <service>   # Restart container-name
``` 

## Exposed ports

| Component                         | Port |
|-----------------------------------|------|
| ZooKeeper                         | 2181 |
| Apache Kafka brokers (plain text) | 9092 |
| Kafka Connect REST API            | 8083 |
| REST Proxy                        | 8082 |
| Schema Registry REST API          | 8081 |
| Schema Registry UI                | 8001 |
| Rest Proxy UI                     | 8002 |
| Kafka Connect UI                  | 8083 |


## Reference

https://docs.confluent.io/current/kafka-rest/docs/api.html
https://docs.confluent.io/current/schema-registry/docs/api.html
https://docs.confluent.io/current/connect/restapi.html#connectors
