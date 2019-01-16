package com.opencore;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerTest {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
//    props.setProperty("auto.offset.reset", "none");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.commit.interval.ms", "1000");
    props.setProperty("group.id", "opencore");

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Arrays.asList("foo", "bar"));
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
          // consumer.commitAsync(); // Offset Commit Request absenden, aber nicht auf ein Ergebnis warten
          // or
          // consumer.commitSync();  // blockiert bis der Request abgeschlossen ist

        }
      }
    }
  }

}
