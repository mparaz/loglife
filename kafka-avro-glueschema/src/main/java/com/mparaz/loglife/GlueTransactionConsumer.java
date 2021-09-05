package com.mparaz.loglife;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class GlueTransactionConsumer {

    final static Logger log = LoggerFactory.getLogger(GlueTransactionConsumer.class);

    public static void main(String[] args) {
        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        // Required for autocommit
        props.put("group.id", "transactionlog");

        // Autocommit is scheduled and not on every read.
        // Alternative is to manually commit the last offset read, or arbitrary offsets.
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class
                .getName());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "ap-southeast-2");

        // The documentation defaults to GENERIC_RECORD.
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());

        // This appears to be for compatibility
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        // It looks like Glue can only deal with Avro GenericRecord?
        KafkaConsumer<Address, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<Address, Transaction> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<Address, Transaction> record : records) {
                log.info("Consumed: key:{}, value: {}", record.key().toString(), record.value().toString());
            }
        }
    }
}
