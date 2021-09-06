package com.mparaz.loglife.service;

import com.github.javafaker.Faker;
import com.mparaz.loglife.*;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Order Service that consumes Order, produces Transaction, consumes for TransactionStatus response.
 */
public class OrderService {

    public static void main(String[] args) {
        String transactionTopic = args[0];
        String transactionStatusTopic = args[1];
        String orderTopic = args[2];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", transactionTopic + "-service");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        // Let processing run in their own thread.
        // Note that KafkaProducer is thread-safe.
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        Producer<Address, Transaction> producer = new KafkaProducer<>(props);
        Consumer<Address, SpecificRecord> consumer = new KafkaConsumer<>(props);
        // Closing these should be handled.

        consumer.subscribe(List.of(orderTopic, transactionStatusTopic));

        AtomicInteger counter = new AtomicInteger();
        Random random = new Random();

        Map<String, Order> orderMap = new ConcurrentHashMap<>();

        while (true) {
            ConsumerRecords<Address, SpecificRecord> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<Address, SpecificRecord> record : records) {
                System.out.println("Consumed: partition: " + record.partition() +
                        ", offset: " + record.offset() +
                        ", timestamp: " + record.timestamp() +
                        ",  value: " + record.value());

                if (record.value() instanceof Order) {
                    // Make a transaction for the order
                    executorService.submit(() -> {

                        String uuidString = UUID.randomUUID().toString();
                        Order order = (Order) record.value();
                        orderMap.put(uuidString, order);

                        BigDecimal amount = BigDecimal.valueOf(0);
                        switch (order.getType()) {
                            case APARTMENT:
                                amount = new BigDecimal("500000.99");
                                break;
                            case HOUSE:
                                amount = new BigDecimal("999999.99");
                                break;
                            case MANSION:
                                amount = new BigDecimal("19999888.88");
                                break;
                        }

                        Transaction transaction = new Transaction(uuidString, order.getAddress(), amount, TransactionType.TRANSFER);
                        ProducerRecord<Address, Transaction> producerRecord = new ProducerRecord<>(transactionTopic,
                                new Address(order.getAddress()), transaction);

                        try {
                            // Simulate a processing delay
                            int delay = random.nextInt(10) + 10;
                            System.out.println("Processing for " + delay + "s:" + record.value());
                            Thread.sleep(1000 * delay);

                            RecordMetadata recordMetadata = producer.send(producerRecord).get();
                            System.out.println("Produced: partition: " + recordMetadata.partition() +
                                    ", offset: " + recordMetadata.offset() +
                                    ", timestamp: " + recordMetadata.timestamp() +
                                    ", value: " + transaction);
                        } catch (InterruptedException | ExecutionException e) {
                            // Future is interrupted, or Kafka client underlying exception
                            e.printStackTrace();
                        }
                    });
                } else if (record.value() instanceof TransactionStatus) {
                    // Print the status and the matching order.
                    executorService.submit(() -> {
                        TransactionStatus transactionStatus = (TransactionStatus) record.value();
                        Order order = orderMap.get(transactionStatus.getUuid());
                        System.out.println("Status of order: " + order + " is: " + transactionStatus);
                    });
                }
            }
        }
    }
}
