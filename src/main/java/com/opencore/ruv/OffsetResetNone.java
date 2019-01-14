package com.opencore.ruv;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class OffsetResetNone {

  static String kafkaPort = "9099";

  public static void main(String[] args) {
    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", "127.0.0.1:" + kafkaPort); // der Kafka Broker der angesprochen werden soll
    consumerProperties.put("auto.offset.reset", "none"); // gibt an, ob ihr initial Daten erhaltet oder am Ende des Topics zu lesen anfangt, wenn eine Consumer Group das allererste Mal verwendet wird
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // wenn wir zu json serialisieren können wir diesen einfach als String übertragen
    consumerProperties.put("group.id", "cg" + String.valueOf(System.currentTimeMillis())); // Consumer Group
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

//      consumer.poll(1000);
    //TopicPartition singleTestPart0 = new TopicPartition(topic, 0);

    // consumer.assign(Collections.singletonList(singleTestPart0));
    //    long lastOffset = consumer.position(singleTestPart0);
    // consumer.seek(singleTestPart0, 11);
    consumer.subscribe(Collections.singletonList("test"));

//      System.out.println("Last offset: " + lastOffset);
//      System.out.println("Current offset: " + consumer.position(singleTestPart0));


    for (int i = 0; i < 3; i++) {
      ConsumerRecords<String, String> records = consumer.poll(100); // nach neuen Records fragen
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.offset() + ": " + record.key() + "-" + record.value());

      }
      consumer.commitAsync(); // Offset Commit Request absenden, aber nicht auf ein Ergebnis warten
      // oder
      consumer.commitSync();  // blockiert bis der Request abgeschlossen ist

    }
  }
}
