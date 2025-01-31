# Fabric Kafka Connect Kusto Sink Connector

This repository contains the source code of the Kafka Connect Fabric sink connector. Currently supports writing data to
Eventhouse (Azure Data Explorer / Kusto) workloads on Fabric.


## Sections in the document

<!-- TOC -->
* [Fabric Kafka Connect Kusto Sink Connector](#fabric-kafka-connect-kusto-sink-connector)
  * [Sections in the document](#sections-in-the-document)
  * [1. Overview](#1-overview)
  * [2. Design](#2-design)
  * [3. Features supported](#3-features-supported)
    * [3.1. Configurable behavior on errors](#31-configurable-behavior-on-errors)
    * [3.2. Configurable retries](#32-configurable-retries)
    * [3.3. Serialization formats](#33-serialization-formats)
    * [3.4. Schema registry](#34-schema-registry)
    * [3.5. Kafka Connect converters](#35-kafka-connect-converters)
    * [3.6. Kafka Connect transformers](#36-kafka-connect-transformers)
    * [3.7. Topics to tables mapping](#37-topics-to-tables-mapping)
    * [3.8. Kafka Connect Dead Letter Queue](#38-kafka-connect-dead-letter-queue)
    * [3.9. Miscellaneous Dead Letter Queue](#39-miscellaneous-dead-letter-queue)
    * [3.11. Delivery semantics](#311-delivery-semantics)
    * [3.12. Overrides](#312-overrides)
    * [3.13. Parallelism](#313-parallelism)
    * [3.14. Authentication & Authorization to Azure Data Explorer](#314-authentication--authorization-to-azure-data-explorer)
    * [3.16. Security related](#316-security-related)
  * [4. Connect worker properties](#4-connect-worker-properties)
    * [4.1. Confluent Cloud](#41-confluent-cloud)
  * [5. Sink properties](#5-sink-properties)
  * [6. Topic Table Mapping attributes](#6-topic-table-mapping-attributes)
  * [7. Streaming ingestion](#7-streaming-ingestion)
  * [8. Roadmap](#8-roadmap)
  * [9. Deployment overview](#9-deployment-overview)
    * [9.1. Standalone Kafka Connect deployment mode](#91-standalone-kafka-connect-deployment-mode)
    * [9.2. Distributed Kafka Connect deployment mode](#92-distributed-kafka-connect-deployment-mode)
  * [10. Connector download/build from source](#10-connector-downloadbuild-from-source)
    * [10.1. Download a ready-to-use uber jar from our Github repo releases listing](#101-download-a-ready-to-use-uber-jar-from-our-github-repo-releases-listing)
    * [10.2. Build uber jar from source](#102-build-uber-jar-from-source)
  * [11. Test drive the connector - standalone mode](#11-test-drive-the-connector---standalone-mode)
    * [11.1. Self-contained Dockerized setup](#111-self-contained-dockerized-setup)
  * [12. Release History](#12-release-history)
  * [12. Contributing](#12-contributing)
<!-- TOC -->
<hr>

## 1. Overview

The Fabric connector for Kafka intends to provide an extensible, scalable, and fault-tolerant way to ingest data from 
Kafka to [Microsoft Fabric](https://www.microsoft.com/en-us/microsoft-fabric). The initial release intends to support
writes to [Eventhouse](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/eventhouse). Future releases will
add support for [Eventstreams](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/overview)

<hr>

## 2. Design
The connector is designed to be a sink connector, ingesting data from Kafka to components in Microsoft Fabric. The initial
design is based on [Kafka Kusto Sink](https://github.com/Azure/kafka-sink-azure-kusto) with slight changes to the 
ingestion process.

The connector parses payload based on type of message and ingests the data to ADX. The ingestion to ADX uses JSON as the format.

* **kafkamd** : Dynamic field that contains metadata about the message. This field is used to store the partition, 
offset, and topic of the message.
* **headers** : Dynamic field that contains headers set in the message.
* **keys** : Dynamic field that has the keys in the kafka message.

Values in the message are stored in the root of the message as-is. In the sample below the fields department, id, external_identifier
external_id_type are stored in the root of the message.

**Note**: 
  * The fields in the message should not have the same name as the fields in the dynamic fields. If the fields have the same name, the values in the dynamic fields will be overwritten by the values in the root of the message.

  * Tombstone records are supported. The value columns will be null and the keys, metadata and header columns will be populated.

```json
{
  "kafkamd": {
    "partition": "0",
    "offset": "1",
    "topic": "e2e.bytes-avro.topic"
  },
  "headers": {
    "vtype": "bytes-avro",
    "iteration": "2"
  },
  "keys": {
    "Timestamp": 100002,
    "IterationKey": "2"
  },
  "department": "0193",
  "id": "41417891001",
  "external_identifier": null,
  "external_id_type": "1"
}
```

A sample KQL statement can be as follows

```kql
.create-merge table ['KustoTable'] (
  ['headers']:dynamic, 
  ['kafkamd']:dynamic,
  ['keys']:dynamic,
  ['department']:string,
  ['id']:string,
  ['external_identifier']:string,
  ['external_id_type']:string
  )
```

The corresponding mapping can be
```kql
.create table ['KustoTable'] ingestion json mapping 'KustoTable_mapping' '[
{"column":"event_type", "Properties":{"Path":"$[\'event_type\']"}},
......
{"column":"keys", "Properties":{"Path":"$[\'keys\']"}},
{"column":"headers", "Properties":{"Path":"$[\'headers\']"}},{
"column":"kafkamd", "Properties":{"Path":"$[\'kafkamd\']"}}]'
```

## 3. Features supported


### 3.1. Configurable behavior on errors

- The connector supports configurable behavior in case of errors. The following are the possible values:
  - Shut down the connector task upon encountering an error
  - Log the error and continue processing
  - Log the error, send record to dead letter queue, and continue processing


### 3.2. Configurable retries

- The connector supports retries for transient errors with the ability to provide relevant parameters
- and retries with exponential backoff

### 3.3. Serialization formats

- The connector supports Avro, JSON, CSV formats

### 3.4. Schema registry

- The connector supports schema registry for avro and json

### 3.5. Kafka Connect converters

- The connector supports the following converters:

| # | Converter                                              | Details                                                  | 
|:--|:-------------------------------------------------------|:---------------------------------------------------------| 
| 1 | org.apache.kafka.connect.storage.StringConverter       | Use with csv/json                                        |
| 2 | org.apache.kafka.connect.json.JsonConverter            | Use with schemaless json                                 |
| 3 | io.confluent.connect.avro.AvroConverter                | Use with avro                                            |
| 4 | io.confluent.connect.json.JsonSchemaConverter          | Use with json with schema registry                       |

### 3.6. Kafka Connect transformers

- The connector does not support transformers. Prefer transformation on the server side in Kafka or ingestion time in
  Azure Data Explorer
  with [update policies](https://docs.microsoft.com/azure/data-explorer/kusto/management/updatepolicy).

### 3.7. Topics to tables mapping

- The connector supports multiple topics to multiple tables configuration per Kafka Connect worker

### 3.8. Kafka Connect Dead Letter Queue

- The connector supports user provided "Dead Letter Queue", a Kafka Connect construct; E.g. If Avro messages are written
  to a "dead letter queue" topic that is expecting Json, the avro messages are written to a configurable dead letter
  queue instead of just being dropped. This helps prevent data loss and also data integrity checks for messages that did
  not make it to the destination. Note that for a secure cluster, in addition to bootstrap server list and topic name,
  the security mechanism, the security protocol, jaas config have to be provided for the Kafka Connect worker and in the
  sink properties
- Messages written to DLQ will be serialized using a ByteArrayConverter and written to the DLQ topic as byte arrays. 
- The enqueued DLQ messages will have the following additional headers for troubleshooting and retry
  - **kafka_topic**: The topic from which the message was read
  - **kafka_partition**: The partition from which the message was read
  - **kafka_offset**: The offset from which the message was read
  - **kafka_timestamp**: The timestamp of the message

### 3.9. Miscellaneous Dead Letter Queue

- The connector supports user provided miscellaneous "Dead Letter Queue" for transient and non-deserialization errors (
  those not managed by Kafka Connect); E.g. If network connectivity is lost to Azure Data Explorer, the connector
  retries and eventually writes the queued up messages to the miscellaneous "Dead Letter Queue". Note that for a secure
  cluster, in addition to bootstrap server list and topic name, the security mechanism, the security protocol, jaas
  config have to be provided for the Kafka Connect worker and in the sink properties

### 3.11. Delivery semantics

- Azure Data Explorer is an append only immutable database. Infrastructure failures and unavoidable external variables
  that can lead to duplicates can't be remediated via upsert commands as upserts are not supported. <br>

Therefore, the connector supports "At least once" delivery guarantees.

### 3.12. Overrides

- The connector supports overrides at the sink level *if overrides are specified at a Kafka Connect worker level*. This
  is a Kafka Connect feature, not specific to the Kusto connector plugin.

### 3.13. Parallelism

- As with all Kafka Connect connectors, parallelism comes through the setting of connector tasks count, a sink property

### 3.14. Authentication & Authorization to Azure Data Explorer

- Azure Data Explorer supports Azure Active Directory authentication. For the Kusto Kafka connector, we need an Azure
  Active Directory Service Principal created and "admin" permissions granted to the Azure Data Explorer database.
- The Service Principal can either be
    - an Enterprise Application, authenticated using the OAuth2 endpoint of Active Directory, using the Tenant ID,
      Application ID and Application Secret
    - a Managed Identity, using [the private Instance MetaData Service](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-java) accessible from within Azure VMs
        - for more information on managed identities, see [Managed Identities for Azure Resources](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/) and [AAD Pod Identity](https://github.com/Azure/aad-pod-identity) for AKS
        - in that scenario, the tenant ID and client ID of the managed identity can be deduced from the context of the call site and are optional

### 3.16. Security related

- Kafka Connect supports all security protocols supported by Kafka, as does our connector
- See below for some security related config that needs to be applied at Kafka Connect worker level as well as in the
  sink properties


## 4. Connect worker properties

- There are some core configs that need to be set at the Kafka connect worker level. Some of these are security configs
  and the (consumer) override policy. These for e.g. need to be baked into


### 4.1. Confluent Cloud

The below covers Confluent Cloud-<br>
[Link to end to end sample](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/5-configure-connector-cluster.md)

```
ENV CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY=All
ENV CONNECT_SASL_MECHANISM=PLAIN
ENV CONNECT_SECURITY_PROTOCOL=SASL_SSL
ENV CONNECT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=https
ENV CONNECT_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<yourConfluentCloudAPIKey>\" password=\"<yourConfluentCloudAPISecret>\";"
```

## 5. Sink properties

The following is complete set of connector sink properties -

| #  | Property                                     | Purpose                                                                                       | Details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | 
|:---|:---------------------------------------------|:----------------------------------------------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1  | connector.class                              | ClassName of the Kusto sink                                                                   | Hard code to ```com.microsoft.fabric.connect.eventhouse.sink.FabricSinkConnector```<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 2  | connection.string                            | Connection string                                                                             | Supports eventhouse [connection string](https://learn.microsoft.com/en-us/kusto/api/connection-strings/kusto?view=microsoft-fabric) <br>*Optional*. Either of connection string or  ingest url required                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 3  | topics                                       | Kafka topic specification                                                                     | List of topics separated by commas<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 4  | kusto.ingestion.url                          | EventHouse ingestion endpoint URL                                                             | Provide the ingest URL of your ADX cluster<br>Use the following construct for the private URL - https://ingest-private-[cluster].kusto.windows.net<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 5  | kusto.query.url                              | EventHouse query endpoint URL                                                                 | Provide the engine URL of your ADX cluster<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 6  | aad.auth.strategy                            | Credentials for EventHouse                                                                    | Strategy to authenticate against Azure Active Directory, either ``application`` (default) or ``managed_identity`` or ``workload_identity``.<br>*Optional, `application` by default*                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 7  | aad.auth.authority                           | Credentials for EventHouse                                                                    | Provide the tenant ID of your Azure Active Directory<br>*Required when authentication is done with an `application` or when `kusto.validation.table.enable` is set to `true`*                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 8  | aad.auth.appid                               | Credentials for EventHouse                                                                    | Provide Azure Active Directory Service Principal Name<br>*Required when authentication is done with an `application` or when `kusto.validation.table.enable` is set to `true`*                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 9  | aad.auth.appkey                              | Credentials for EventHouse                                                                    | Provide Azure Active Directory Service Principal secret<br>*Required when authentication is done with an `application`*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 10 | kusto.tables.topics.mapping                  | Mapping of topics to tables                                                                   | Provide 1..many topic-table comma-separated mappings as follows-<br>[{'topic': '\<topicName1\>','db': '\<datebaseName\>', 'table': '\<tableName1\>','format': '<format-e.g.avro/csv/json>', 'mapping':'\<tableMappingName1\>','streaming':'false'},{'topic': '\<topicName2\>','db': '\<datebaseName\>', 'table': '\<tableName2\>','format': '<format-e.g.avro/csv/json>', 'mapping':'\<tableMappingName2\>','streaming':'false'}]<br>*Required* <br> Note : The attribute mapping (Ex:'mapping':''tableMappingName1') is an optional attribute. During ingestion, Azure Data Explorer automatically maps column according to the ingestion format |
| 11 | key.converter                                | Deserialization                                                                               | One of the below supported-<br>org.apache.kafka.connect.storage.StringConverter<br> org.apache.kafka.connect.json.JsonConverter<br>io.confluent.connect.avro.AvroConverter<br>io.confluent.connect.json.JsonSchemaConverter<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                         |
| 12 | value.converter                              | Deserialization                                                                               | One of the below supported-<br>org.apache.kafka.connect.storage.StringConverter<br> org.apache.kafka.connect.json.JsonConverter<br>io.confluent.connect.avro.AvroConverter<br>io.confluent.connect.json.JsonSchemaConverter<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                         |
| 13 | value.converter.schema.registry.url          | Schema validation                                                                             | URI of the Kafka schema registry<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 14 | value.converter.schemas.enable               | Schema validation                                                                             | Set to true if you have embedded schema with payload but are not leveraging the schema registry<br>Applicable for avro and json<br><br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 15 | tasks.max                                    | connector parallelism                                                                         | Specify the number of connector copy/sink tasks<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 16 | flush.size.bytes                             | Performance knob for batching                                                                 | Maximum bufer byte size per topic+partition combination that in combination with flush.interval.ms (whichever is reached first) should result in sinking to EventHouse<br>*Default - 1 MB*<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 17 | flush.interval.ms                            | Performance knob for batching                                                                 | Minimum time interval per topic+partition combo that in combination with flush.size.bytes (whichever is reached first) should result in sinking to EventHouse<br>*Default - 30 seconds*<br>*Required*                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 18 | tempdir.path                                 | Local directory path on Kafka Connect worker to buffer files to before shipping to EventHouse | Default is value returned by ```System.getProperty("java.io.tmpdir")``` with a GUID attached to it<br><br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 19 | behavior.on.error                            | Configurable behavior in response to errors encountered                                       | Possible values - log, ignore, fail<br><br>log - log the error, send record to dead letter queue, and continue processing<br>ignore - log the error, send record to dead letter queue, proceed with processing despite errors encountered<br>fail - shut down connector task upon encountering<br><br>*Default - fail*<br>*Optional*                                                                                                                                                                                                                                                                                                              |
| 20 | errors.retry.max.time.ms                     | Configurable retries for transient errors                                                     | Period of time in milliseconds to retry for transient errors<br><br>*Default - 300 ms*<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 21 | errors.retry.backoff.time.ms                 | Configurable retries for transient errors                                                     | Period of time in milliseconds to backoff before retry for transient errors<br><br>*Default - 10 ms*<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 22 | errors.deadletterqueue.bootstrap.servers     | Channel to write records that failed deserialization                                          | CSV or kafkaBroker:port <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 23 | errors.deadletterqueue.topic.name            | Channel to write records that failed deserialization                                          | Pre-created topic name <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 24 | errors.deadletterqueue.security.protocol     | Channel to write records that failed deserialization                                          | Securitry protocol of secure Kafka cluster <br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 25 | errors.deadletterqueue.sasl.mechanism        | Channel to write records that failed deserialization                                          | SASL mechanism of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 26 | errors.deadletterqueue.sasl.jaas.config      | Channel to write records that failed deserialization                                          | JAAS config of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 27 | misc.deadletterqueue.bootstrap.servers       | Channel to write records that due to reasons other than deserialization                       | CSV of kafkaBroker:port <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 28 | misc.deadletterqueue.topic.name              | Channel to write records that due to reasons other than deserialization                       | Pre-created topic name <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 29 | misc.deadletterqueue.security.protocol       | Channel to write records that due to reasons other than deserialization                       | Securitry protocol of secure Kafka cluster <br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 30 | misc.deadletterqueue.sasl.mechanism          | Channel to write records that due to reasons other than deserialization                       | SASL mechanism of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 31 | misc.deadletterqueue.sasl.jaas.config        | Channel to write records that due to reasons other than deserialization                       | JAAS config of secure Kafka cluster<br>*Optional but when feature is used with secure cluster, is required*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 32 | consumer.override.bootstrap.servers          | Security details explicitly required for secure Kafka clusters                                | Bootstrap server:port CSV of secure Kafka cluster <br>*Required for secure Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 33 | consumer.override.security.protocol          | Security details explicitly required for secure Kafka clusters                                | Security protocol of secure Kafka cluster <br>*Required for secure Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 34 | consumer.override.sasl.mechanism             | Security details explicitly required for secure Kafka clusters                                | SASL mechanism of secure Kafka cluster<br>*Required for secure Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 35 | consumer.override.sasl.jaas.config           | Security details explicitly required for secure Kafka clusters                                | JAAS config of secure Kafka cluster<br>*Required for secure Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 36 | consumer.override.sasl.kerberos.service.name | Security details explicitly required for secure Kafka clusters, specifically kerberized Kafka | Kerberos service name of kerberized Kafka cluster<br>*Required for kerberized Kafka clusters*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 37 | consumer.override.auto.offset.reset          | Configurable consuming from offset                                                            | Possible values are - earliest or latest<br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 38 | consumer.override.max.poll.interval.ms       | Config to prevent duplication                                                                 | Set to a value to avoid consumer leaving the group while the Connector is retrying <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 39 | proxy.host                                   | Host details of proxy server                                                                  | Host details of proxy server configuration <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 40 | proxy.port                                   | Port details of proxy server                                                                  | Port details of proxy server configuration <br>*Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 41 | headers.to.project                           | Header fields                                                                                 | List of header fields to persist in the format ['hf1','hf2'..] *Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 42 | headers.to.drop                              | Header fields                                                                                 | List of header fields to drop ['hf1','hf2'..] *Optional*                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

<hr>

## 6. Topic Table Mapping attributes

| # | Property       |   | Purpose                                                                                                                        | Details                                                                                                                                                                           |
|:--|:---------------|:--|:-------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | topic          |   | Kafka topic specification                                                                                                      | Topic name<br>*Required*                                                                                                                                                          |
| 2 | db             |   | Target database                                                                                                                | Database name<br>*Required*                                                                                                                                                       |
| 3 | mapping        |   | Ingestion mapping reference                                                                                                    | Database ingestion mapping reference. [Reference](https://learn.microsoft.com/en-us/kusto/management/mappings?view=microsoft-fabric)   <br>*Optional*                             |
| 4 | streaming      |   | If the ingestion should use streaming                                                                                          | Enable streaming ingest on. 'true'/'false' . [Reference](https://learn.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming?tabs=azure-portal%2Ccsharp)   <br>*Optional* |
| 5 | table          |   | Target table for ingestion                                                                                                     | The table should be pre-created <br>*Required*                                                                                                                                    |
| 6 | dynamicPayload |   | If the payload should be a dynamic field.<br> Useful in scenarios where multiple topics need to write to the same target table | The table has the definition ```.create-merge table <tbl> ([payload]:dynamic,[keys]:dynamic,[headers]:dynamic,[kafkamd]:dynamic ) ``` in this case <br>*Optional*                 |

## 7. Streaming ingestion

EventHouse supports [Streaming ingestion](https://docs.microsoft.com/azure/data-explorer/ingest-data-streaming) in order to
achieve sub-second latency.

This connector supports this
using [Managed streaming client](https://github.com/Azure/azure-kusto-java/blob/master/ingest/src/main/java/com/microsoft/azure/kusto/ingest/ManagedStreamingIngestClient.java)
.

Usage: configure per topic-table that streaming should be used. For example:

```
kusto.tables.topics.mapping=[{'topic': 't1','db': 'db', 'table': 't1','format': 'json', 'mapping':'map', 'streaming': true}].
```

Requirements: Streaming enabled on the
cluster. [Streaming policy](https://docs.microsoft.com/azure/data-explorer/kusto/management/streamingingestionpolicy)
configured on the table or database.

Additional configurations: flush.size.bytes and flush.interval.ms are still used to batch
records together before ingestion - flush.size.bytes should not be over 4MB, flush.interval.ms
is suggested to be low (hundreds of milliseconds).
We still recommend configuring ingestion batching policy at the table or database level, as the client falls back to
queued ingestion in case of failure and retry-exhaustion.

## 8. Roadmap

The following is the roadmap-<br>

| # | Roadmap item            | 
|:--|:------------------------|
| 1 | Support for EventStream |


## 9. Deployment overview

Kafka Connect connectors can be deployed in standalone mode (just for development) or in distributed mode (production)

### 9.1. Standalone Kafka Connect deployment mode

This involves having the connector plugin jar in /usr/share/java of a Kafka Connect worker, reference to the same plugin
path in connect-standalone.properties, and launching of the connector from command line. This is not scalable, not fault
tolerant, and is not recommended for production.

### 9.2. Distributed Kafka Connect deployment mode

Distributed Kafka Connect essentially involves creation of a KafkaConnect worker cluster as shown in the diagram
below.<br>

- Azure Kubernetes Service is a great infrastructure for the connect cluster, due to its managed and scalable nature
- Kubernetes is a great platform for the connect cluster, due to its scalable nature and self-healing
- Each orange polygon is a Kafka Connect worker and each green polygon is a sink connector instance
- A Kafka Connect worker can have 1..many task instances which helps with scale
- When a Kafka Connect worker is maxed out from a resource perspective (CPU, RAM), you can scale horizontally, add more
  Kafka Connect workers, ands tasks within them
- Kafka Connect service manages rebalancing of tasks to Kafka topic partitions automatically without pausing the
  connector tasks in recent versions of Kafka
- A Docker image needs to be created to deploy the Fabric sink connector in a distributed mode. This is detailed below.

![CONNECTOR](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/confluent-cloud/images/AKS-ADX.png)
<br>
<br>
<hr>
<br>

## 10. Connector download/build from source

Multiple options are available-

### 10.1. Download a ready-to-use uber jar from our Github repo releases listing

https://github.com/microsoft/kafka-sink-ms-fabric/releases

### 10.2. Build uber jar from source

The dependencies are-

* JDK >= 1.8 [download](https://www.oracle.com/technetwork/java/javase/downloads/index.html)
* Maven [download](https://maven.apache.org/install.html)

**1. Clone the repo**<br>

```bash
git clone git://github.com/microsoft/kafka-sink-ms-fabric.git
cd ./kafka-sink-ms-fabric
```

**2. Build with maven**<br>
For an Uber jar, run the below-

```bash
mvn clean compile assembly:single
```

For the connector jar along with jars of associated dependencies, run the below-

```bash
mvn clean install
```

Look
within `target/components/packages/microsoftcorporation-kafka-sink-ms-fabric-<version>/microsoftcorporation-kafka-sink-ms-fabric-<version>/lib/`
folder



<hr>

## 11. Test drive the connector - standalone mode

In a standalone mode (not recommended for production), the connector can be test-driven in any of the following ways-

### 11.1. Self-contained Dockerized setup

[Review this hands on lab](https://github.com/Azure/azure-kusto-labs/blob/master/kafka-integration/dockerized-quickstart/README.md)
. It includes dockerized kafka, connector and Kafka producer to take away complexities and allow you to focus on the
connector aspect.


## 12. Release History

| Release Version | Release Date | Changes Included                                                                             |
|-----------------|--------------|----------------------------------------------------------------------------------------------|
| 1.0.0           | 2024-12-30   | <ul><li>Initial release</li></ul>                                                            |  
| 1.1.0           | 2024-01-21   | <ul><li>Add additional headers for DLQ support</li><br/><li>Bump Kusto SDK version</li></ul> |  
| 1.2.0           | 2024-01-31   | <ul><li>Support for optional field dynamicPayload</li></ul>                                  |  

## 12. Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

In order to make the PR process efficient, please follow the below checklist:

* **There is an issue open concerning the code added** - Either a bug or enhancement. Preferably the issue includes an
  agreed upon approach.
* **PR comment explains the changes done** - This should be a TL;DR; as the rest of it should be documented in the
  related issue.
* **PR is concise** - Try to avoid making drastic changes in a single PR. Split it into multiple changes if possible. If
  you feel a major change is needed, make sure the commit history is clear and maintainers can comfortably review both
  the code and the logic behind the change.
* **Please provide any related information needed to understand the change** - Especially in the form of unit tests, but
  also docs, guidelines, use-case, best practices, etc as appropriate.
* **Checks should pass**
* Run `mvn dependency-check:check`. This should return no High/Medium vulnerabilities in any libraries/dependencies

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.