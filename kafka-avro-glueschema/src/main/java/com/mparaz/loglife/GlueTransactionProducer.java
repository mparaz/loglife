package com.mparaz.loglife;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class GlueTransactionProducer {

    final static Logger log = LoggerFactory.getLogger(GlueTransactionProducer.class);

    public static void main(String[] args) {
        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "ap-southeast-2");
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "my-registry");

        // Schemas are explicitly named and not derived from the topic.
        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "transaction");

        String uuidString = UUID.randomUUID().toString();
        Faker faker = new Faker();
        String addressString = faker.address().streetAddress();
        Address address = new Address(addressString);
        Random random = new Random();
        BigDecimal amount = new BigDecimal(random.nextInt(1000000) + 400000);

        Transaction transaction = new Transaction(uuidString, addressString, amount, TransactionType.REFINANCE);

        try (Producer<String, Transaction> producer = new KafkaProducer<>(props)) {
            // Oversimplifying - the property is identified uniquely by the address.
            ProducerRecord<String, Transaction> producerRecord = new ProducerRecord<>(topic, addressString, transaction);
            producer.send(producerRecord);
        }
    }
}
