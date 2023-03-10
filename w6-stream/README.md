# Week 6 - Streaming

Theory is similar to GCP's pub/sub. Components:

- producer
- event
- consumer
- topic
- source data
- sink data

producers can transmit events to various topics, and consumers receive those events if they are subscribed to those topics. Each message sent to the topic has a specified retention time, and once a message is consumed by, it may persist for other consumers depending on config

## Confluent - Cloud kafka

Create a cluster with the 30 day free-trial.

### API key

Used as credential to connect with the confluent cluster

1. create key
1. global access
1. download

### Topic

Create topic for us to send data and subscribe to. Name it `tutorial_topic`.

1.[optional] define data contract for this topic to ensure producers and consumers have a common schema
2. Messages > text editor that defines some default message in JSON format:

    ```json
    # value:
    {
        "ordertime": 1497014222380,
        "orderid": 18,
        "itemid": "Item_184",
        "address": {
            "city": "Mountain View",
            "state": "CA",
            "zipcode": 94041
        }
    }
    # key:
    18
    ```

    Manually `Produce` the message to send it to our `tutorial_topic`

When creating topic, there is the option of `partitions`. Each topic is divided into partitions, and more partitions allow greater parallelization and thus throughput

### Connector

Left nav > Connectors > Datagen for mock streaming data. Add this connector

1. Select `tutorial_topic` for this source to connect to
1. Credentials - the source needs credentials to connect to the confluent cluster
1. Config:
    - value formats:
        - avro
        - json
        - protobuf
    - templates:
        - orders
        - users
        - clickstream
        - ratings
        - stock trades
        - pageviews
        - and a lot more
1. Create connector - will take about a minute

This connector will now send mock streaming events to our `tutorial_topic`

### Clients

Applications written in programming lang of your choice to send/receive data to/from confluent via topics
