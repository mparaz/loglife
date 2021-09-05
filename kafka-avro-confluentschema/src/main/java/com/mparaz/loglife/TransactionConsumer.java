package com.mparaz.loglife;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TransactionConsumer {

    final static Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

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
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        // This appears to be for compatibility
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        KafkaConsumer<Address, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<Address, Transaction> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<Address, Transaction> record : records) {
                log.info("Consumed: {}", record.value().toString());
            }
        }
    }
}
