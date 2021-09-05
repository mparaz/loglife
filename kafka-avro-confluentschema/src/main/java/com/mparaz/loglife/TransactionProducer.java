package com.mparaz.loglife;

import com.github.javafaker.Faker;
import com.github.javafaker.service.FakerIDN;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TransactionProducer {

    final static Logger log = LoggerFactory.getLogger(TransactionProducer.class);

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
            producer.send(producerRecord);
        }
    }
}
