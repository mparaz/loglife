package com.mparaz.loglife;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * GenericRecord consumer - can listen to any topic without fear.
 */
public class GenericRecordConsumer {

    public static void main(String[] args) {
        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        // Required for autocommit
        // Since it is listening in, it should be on its own consumer group.
        props.put("group.id", topic + "-" + "genericrecord");

        // Autocommit is scheduled and not on every read.
        // Alternative is to manually commit the last offset read, or arbitrary offsets.
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        // GenericRecord only
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                System.out.println("Consumed: partition: " + record.partition() +
                        ", offset: " + record.offset() +
                        ", timestamp: " + record.timestamp() +
                        ",  value: " + record.value());
            }
        }
    }
}
