package com.opencore.ruv;

import static java.lang.Math.round;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TransactionalMixed {
  public static void main(String[] args) {


    Set<String> LOCK_HELD = Collections.synchronizedSet(new HashSet<>());

    LOCK_HELD.add("test");
    // Producer code
    Properties producerProps = new Properties();
    String kafkaPort = "9092";
    producerProps.put("bootstrap.servers", "127.0.0.1:" + kafkaPort); // Kafka Broker
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producerNonTrans = new KafkaProducer<String, String>(producerProps);

    producerProps.put("enable.idempotence", "true");
    producerProps.put("transactional.id", "kafkatest");
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);

    // dieser Aufruf sendet noch nicht wirklich, der Producer batched nach eigenem Ermessen, die zurückgegebene Future enthält
    // später das Ergebnis, wenn wirklich gesendet wurde

    String topic = "test9";
    int numberOfTransactions = 10;
    int recordsPerTransaction = 10;
    String value = "";
    ArrayList<Future> results = new ArrayList<>();

    char[] chars = new char[1500 * 1000];
    // Optional step - unnecessary if you're happy with the array being full of \0
    Arrays.fill(chars, 'f');
    String largeMessage = new String(chars);

    producer.initTransactions();
      try {
        producer.beginTransaction();
        System.out.println("Started transaction");
        //TimeUnit.SECONDS.sleep(10);
        for (int recordId = 1; recordId <= recordsPerTransaction; recordId++) {
          value = "xxx";
          results.add(producer.send(new ProducerRecord<String, String>(topic, null, recordId + "-" + value)));
          System.out.println("Sent message " + recordId + " " + value);
          if (recordId == round(recordsPerTransaction / 2)) {
            producerNonTrans.send(new ProducerRecord<String, String>(topic,null,"non-trans"));
            producerNonTrans.flush();
            System.out.println("Sent non transactional message, pausing ... ");
            TimeUnit.SECONDS.sleep(15);
            System.out.println("Continuing");
          }

        }
        //producer.sendOffsetsToTransaction(n);
        producer.abortTransaction();
        producer.flush();
        for (Future result : results) {
          result.get();
        }
        results.clear();
      } catch (Exception e) {
        producer.abortTransaction();
        System.out.println("Aborted due to " + e.getMessage());
        results.clear();
      }

    producer.close();
  }
}
