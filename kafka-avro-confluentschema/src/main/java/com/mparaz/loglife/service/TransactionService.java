package com.mparaz.loglife.service;

import com.mparaz.loglife.Address;
import com.mparaz.loglife.Transaction;
import com.mparaz.loglife.TransactionStatus;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transaction Service that consumes Transaction and produces TransactionStatus
 */
public class TransactionService {

    public static void main(String[] args) {
        String transactionTopic = args[0];
        String transactionStatusTopic = args[1];

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

        Producer<Address, TransactionStatus> producer = new KafkaProducer<>(props);
        Consumer<Address, Transaction> consumer = new KafkaConsumer<>(props);
        // Closing these should be handled.

        consumer.subscribe(List.of(transactionTopic));

        AtomicInteger counter = new AtomicInteger();
        Random random = new Random();

        while (true) {
            ConsumerRecords<Address, Transaction> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<Address, Transaction> record : records) {
                System.out.println("Consumed: partition: " + record.partition() +
                        ", offset: " + record.offset() +
                        ", timestamp: " + record.timestamp() +
                        ",  value: " + record.value());

                executorService.submit(() -> {
                    // Produce the status and use the transaction key.
                    Transaction transaction = record.value();
                    TransactionStatus transactionStatus = new TransactionStatus(transaction.getUuid(),
                            "all good " + counter.getAndIncrement() + " " + transaction.getAmount());
                    ProducerRecord<Address, TransactionStatus> producerRecord = new ProducerRecord<>(transactionStatusTopic,
                            record.key(), transactionStatus);

                    try {
                        // Simulate a processing delay
                        int delay = random.nextInt(10) + 10;
                        System.out.println("Processing for " + delay + "s:" + transaction);
                        Thread.sleep(1000 * delay);

                        RecordMetadata recordMetadata = producer.send(producerRecord).get();
                        System.out.println("Produced: partition: " + recordMetadata.partition() +
                                ", offset: " + recordMetadata.offset() +
                                ", timestamp: " + recordMetadata.timestamp() +
                                ", value: " + transactionStatus);
                    } catch (InterruptedException | ExecutionException e) {
                        // Future is interrupted, or Kafka client underlying exception
                        e.printStackTrace();
                    }

                    }
                );
            }
        }
    }
}
