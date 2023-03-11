# Week 6 - Streaming

Theory is similar to GCP's pub/sub. Components:

- producer
- event
- consumer
- topic
- source data
- sink data

producers can transmit events to various topics, and consumers receive those events if they are subscribed to those topics. Each message sent to the topic has a specified retention time, and once a message is consumed, it may persist for other consumers, or guarantee that it is consumed only once, depending on config.

## Confluent - Cloud kafka

Create a cluster with the 30 day free-trial.

### API key

Used as credential to connect with the confluent cluster

1. create key
1. global access
1. download

### Topic

- Append-only log 
- can be read only by seeking some specified *offset*. 
- immutable
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

Topic replication occur at the broker level. There may be multiple nodes, depending on the replication factor, which contain the same topic log. Consumers and producers always talk to the same node, i.e. the *leader node* for this topic. Given a replication factor of 3, the leader will replicate that topic log amongst 3 other nodes. If leader node goes down, another node will automatically be appointed the new leader node.

#### Partitioning

When creating topic, there is the option of `partitions`. Each topic is divided into partitions, and more partitions allow greater parallelization and thus throughput. If a topic has multiple producers attached, each producer may send events to separate partitions of that topic.

Because of partitions, topics are no longer constrained to a single node in the cluster; a topic can now be distributed amongst the cluster via partitioning.

How do we determine which partitions should each message be sent?

- if a message has no key, then messages are distributed round-robin style amongst all partitions so that each partition has equal share
- if there is key, then the destination is determined by the hash. Thus, messages of the same key will always end up at the same partition, and always in order, within context of that key
Topics are replicated by factor of 3 by default to provide fault-tolerance and high-availability. This occurs at the topic-partition level
- Partitions are also replicated in topic replication

#### Consumer group ID

Consumer group IDs facilitate workload sharing by grouping consumers that do the same job, promoting scalability and fault-tolerance.

Use case:

Given a ride topic with two partitions, perhaps separated by day/night, we start with one consumer. Both partitions are consumed by `A`. If consumption slows down, perhaps due to API calls taking longer, or some hardware issue, we decide to add a second consumer, `B`. Now partition 1 feeds `A`, and 2 feeds `B`. Kafka allows `B` to consume from partition 2 because we assigned the same *Consumer Group ID* to consumer `B`.

If we add a third consumer, `C`, it will only idle because only one consumer can connect a partition. Kafka may activate `C` if it realizes one of `A` or `B` goes down. More commonly, a consumer will be responsible for multiple parts, e.g. 4 consumers for 8 parts. It may start off with 2 parts for each consumer; if one consumer goes down, 8 parts are now split between 3 consumers. The brokers are responsible for rebalancing and ensuring the workload is distributed.

Consumer group ID is assigned when creating the consumer client.

#### Offset

Topics are append-only logs, and so offsets indicate the position of the messages. Kafka maintains an internal topic, `__consumer_offset`, which keeps track of which message offset has been read. It keeps track of

- consumer group ID
- topic
- partition
- offset

This lets consumers in each group keep track of which *offsets* have been consumed, and what's the available stock of messages to be consumed

#### Auto Offset Reset

This is a consumer configuration which determines how it retrieves messages from the subscription

- earliest - takes earliest available topic
- latest - takes latest available, after the consumer has been introduced

#### Ack All

Producer configuration option. Possible acknowledgement modes:

- 0: fire and forget
- 1: leader successful - followers do not need ack
- all: leader *and* followers - do not reply successful unless the produced event is replicated properly between all replication nodes
    - most safe
    - slowest as it could trigger many re-sends

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

### Servers

These are the clusters on confluent. There are two types:

- Brokers - storage layer responsible for replication and serving data to clients
    - replication promotes availability and resilience 
- Kafka Connect - continuously import and export data as event streams

Kafka clusters are scalable and fault-tolerant; if one server fails, another node will take its place
