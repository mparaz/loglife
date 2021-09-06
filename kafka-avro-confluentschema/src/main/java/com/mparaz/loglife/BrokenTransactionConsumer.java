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

/**
 * Broken Transaction Consumer - demonstrate autocommit false.
 */
public class BrokenTransactionConsumer {

    public static void   main(String[] args) {
        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        // Required - looks like all the time.
        props.put("group.id", topic + "-broken");

        // Autocommit off for finer control.
        props.put("enable.auto.commit", "false");

        // Equivalent to CLI --from-beginning
        props.put("auto.offset.reset", "earliest");

        // The default:
        // props.put("auto.offset.reset", "latest");

        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        // Required for Avro generated object deserialisation
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        KafkaConsumer<Address, Transaction> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<Address, Transaction> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<Address, Transaction> record : records) {
                System.out.println("Consumed: partition: " + record.partition() +
                        ", offset: " + record.offset() +
                        ", timestamp: " + record.timestamp() +
                        ",  value: " + record.value());
            }

            // Manual commit. But, we fail.
            // consumer.commitSync();
        }
    }
}
