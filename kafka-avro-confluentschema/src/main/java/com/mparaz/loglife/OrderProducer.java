package com.mparaz.loglife;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * One-shot Order Producer.
 */
public class OrderProducer {

    public static void main(String[] args) {
        String topic = args[0];
        int count = Integer.parseInt(args[1]);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");

        try (Producer<Address, Order> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String uuidString = UUID.randomUUID().toString();
                Faker faker = new Faker();
                String addressString = faker.address().fullAddress();
                Address address = new Address(addressString);

                Random random = new Random();
                OrderSize orderSize = OrderSize.HOUSE;
                switch (random.nextInt(3)) {
                    case 0:
                        orderSize = OrderSize.APARTMENT;
                        break;
                    case 1:
                        orderSize = OrderSize.HOUSE;
                        break;
                    case 2:
                        orderSize = OrderSize.MANSION;
                        break;
                }

                Order order = new Order(uuidString, addressString, faker.funnyName().name(), orderSize);

                // Oversimplifying - the property is identified uniquely by the address.
                ProducerRecord<Address, Order> producerRecord = new ProducerRecord<>(topic, address, order);

                try {
                    RecordMetadata recordMetadata = producer.send(producerRecord).get();
                    System.out.println("Produced: partition: " + recordMetadata.partition() +
                            ", offset: " + recordMetadata.offset() +
                            ", timestamp: " + recordMetadata.timestamp() +
                            ", value: " + order);
                } catch (InterruptedException | ExecutionException e) {
                    // Future is interrupted, or Kafka client underlying exception
                    e.printStackTrace();
                }
            }
        }
    }
}
