echo "Downloading libs"
curl "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.3.3/kafka-connect-avro-converter-7.3.3.jar" -o /kafka/connect/kafka-sink-ms-fabric/kafka-connect-avro-converter-7.3.3.jar
curl "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.3.3/kafka-avro-serializer-7.3.3.jar" -o /kafka/connect/kafka-sink-ms-fabric/kafka-avro-serializer-7.3.3.jar
curl "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.3.3/kafka-schema-registry-client-7.3.3.jar" -o /kafka/connect/kafka-sink-ms-fabric/kafka-schema-registry-client-7.3.3.jar
curl "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.3.3/kafka-connect-avro-data-7.3.3.jar" -o /kafka/connect/kafka-sink-ms-fabric/kafka-connect-avro-data-7.3.3.jar
curl "https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.0/avro-1.11.0.jar" -o /kafka/connect/kafka-sink-ms-fabric/avro-1.11.0.jar
curl "https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.21/commons-compress-1.21.jar" -o /kafka/connect/kafka-sink-ms-fabric/commons-compress-1.21.jar
echo "Finished downloading libs"