package edu.ensias.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {
  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      System.out.println("Entrer le nom du topic");
      return;
    }

    String topicName = args[0];

    Properties props = new Properties();
    // Adresse du broker Kafka
    props.put("bootstrap.servers", "localhost:9092");

    // Comportement d'acknowledgement
    props.put("acks", "all");

    // Nombre de retries si un envoi échoue
    props.put("retries", 0);

    // Taille des batches
    props.put("batch.size", 16384);

    // Mémoire tampon côté producteur
    props.put("buffer.memory", 33554432);

    // Sérialiseurs
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);

    try {
      for (int i = 0; i < 10; i++) {
        producer.send(new ProducerRecord<>(topicName, Integer.toString(i), Integer.toString(i)));
      }
      System.out.println("Messages envoyés avec succès dans le topic " + topicName);
    } finally {
      producer.close();
    }
  }
}
