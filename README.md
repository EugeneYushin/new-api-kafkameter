# new-api-kafkameter

This project applies streaming messages into Kafka through JMeter. Project is based on [kafkameter](https://github.com/BrightTag/kafkameter) but with new kafka producer API, available since Kafka 0.8.2 relesed.

For this particular purpose, generic `Add -> Config Element -> CSV Data Set` was used. Configuration of this item is self-explanatory: you could specify file name, custom delimiter, etc.

## Install
Build the extension:  
```Shell
mvn package
```
Install the extension into `$JMETER_HOME/lib/ext`:
```Shell
cp target/new-api-kafkameter-x.y.z.jar $JMETER_HOME/lib/ext
```

## Usage
### Kafka Producer Sampler

After installing kafkameter, add a Java Request Sampler (`Add -> Sampler -> Java Request`) and select the NewApiKafkaProducerSampler class name. Note, some property names changed in Kafka in 0.8.2 version.

|Property|Mandatory|Description|
|----|----|----|
|kafka_topic|Y|the topic in Kafka to which the message will be published
|kafka_message|Y|the message itself
|kafka_key_serializer|N|the Kafka client key.serializer property
|kafka_value_serializer|N|the Kafka client value.serializer property

![alt text](https://github.com/EugeneYushin/new-api-kafkameter/blob/master/JMeter.png "JMeter Kafka Request")

Stay in touch ;)
