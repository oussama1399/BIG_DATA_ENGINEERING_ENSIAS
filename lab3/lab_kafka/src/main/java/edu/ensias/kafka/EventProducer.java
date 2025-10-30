package edu.ensias.kafka;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class EventProducer {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please provide topic name as argument");
            System.exit(1);
        }
        String topicName = args[0];

        // Producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        
        try {
            // Send a test message
            String message = "Hello from Kafka Producer!";
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "key1", message);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending message: " + exception.getMessage());
                } else {
                    System.out.printf("Message sent to topic %s, partition %d, offset %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            
            // Ensure the message is sent before closing
            producer.flush();
            System.out.println("Message sent successfully");
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}