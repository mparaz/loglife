package com.mparaz.loglife;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * One-shot Transaction Producer.
 */
public class TransactionProducer {

    public static void main(String[] args) {
        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");

        String uuidString = UUID.randomUUID().toString();
        Faker faker = new Faker();
        String addressString = faker.address().streetAddress();
        Address address = new Address(addressString);
        Random random = new Random();
        BigDecimal amount = new BigDecimal(random.nextInt(1000000) + 400000);

        Transaction transaction = new Transaction(uuidString, addressString, amount, TransactionType.REFINANCE);

        try (Producer<Address, Transaction> producer = new KafkaProducer<>(props)) {
            // Oversimplifying - the property is identified uniquely by the address.
            ProducerRecord<Address, Transaction> producerRecord = new ProducerRecord<>(topic, address, transaction);

            // send() returns a Future, to simplify, get() will block.
            // Another option is to use a callback.
            try {
                RecordMetadata recordMetadata = producer.send(producerRecord).get();
                System.out.println("Produced: partition: " + recordMetadata.partition() +
                        ", offset: " + recordMetadata.offset() +
                        ", timestamp: " + recordMetadata.timestamp() +
                        ", value: " + transaction);
            } catch (InterruptedException | ExecutionException e) {
                // Future is interrupted, or Kafka client underlying exception
                e.printStackTrace();
            }
        }
    }
}
