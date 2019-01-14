package com.opencore.ruv;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Compression {
  public static void main(String[] args) {
    // Producer code
    Properties producerProps = new Properties();
    String kafkaPort = "9092";
    producerProps.put("bootstrap.servers", "127.0.0.1:" + kafkaPort); // Kafka Broker
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("compression.type", "gzip");
    producerProps.put("batch.size", "900");
    producerProps.put("linger.ms", "10000");

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);

    // dieser Aufruf sendet noch nicht wirklich, der Producer batched nach eigenem Ermessen, die zurückgegebene Future enthält
    // später das Ergebnis, wenn wirklich gesendet wurde

    String topic = "test9";
    ArrayList<Future> results = new ArrayList<>();
    int numberOfRecords = 1000;


    for (int i = 1; i <= numberOfRecords; i++) {
      results.add(producer.send(new ProducerRecord<String, String>(topic, "key-" + i, "value-" + i)));
    }

    // flush erzwingt das Senden aller Records die vorher per .send() gesendet wurden
    producer.flush();
    try {
      // Beim .get() würden wir evtl. Exceptions zurückbekommen die aufgetreten sind
      for (Future result : results) {
        result.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("schiefgegangen");
      System.exit(1);
    }

    System.out.println("Sent " + results.size() + " records!");

  }
}
