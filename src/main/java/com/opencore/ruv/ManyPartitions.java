package com.opencore.ruv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;
import sun.java2d.xr.MutableInteger;

public class ManyPartitions {

  static String kafkaPort = "9092";

  public static void main(String[] args) {
    Logger log;
    LogContext logContext = new LogContext("App:");
    log = logContext.logger(ManyPartitions.class);
    List<String> topics = new ArrayList();
    long runTime = 600000;
    long statusIntervall = 10000;

    topics.add("mmm");
    topics.add("aaa");
    topics.add("zzz");
    topics.add("bbb");
    topics.add("000");
    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", "127.0.0.1:" + kafkaPort); // der Kafka Broker der angesprochen werden soll
    consumerProperties.put("auto.offset.reset", "earliest"); // gibt an, ob ihr initial Daten erhaltet oder am Ende des Topics zu lesen anfangt, wenn eine Consumer Group das allererste Mal verwendet wird
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // wenn wir zu json serialisieren können wir diesen einfach als String übertragen
    consumerProperties.put("group.id", "cg" + String.valueOf(System.currentTimeMillis())); // Consumer Group
    consumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024 * 1024);
    //consumerProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024);
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

    consumer.subscribe(topics);

//      System.out.println("Last offset: " + lastOffset);
//      System.out.println("Current offset: " + consumer.position(singleTestPart0));

    long startTime = System.currentTimeMillis();
    long lastStatus = startTime;
    long endTime = startTime + runTime;
    int zeroPolls = 0;

    HashMap<String, Integer> recordsReceived = new HashMap<>();
    HashMap<String, Long> recordsFirstReceived = new HashMap<>();
    while (System.currentTimeMillis() < endTime) { //&& zeroPolls < 4) {
      ConsumerRecords<String, String> records = consumer.poll(10000); // nach neuen Records fragen
//      System.out.println("Got " + records.count() + records);
      if (records.count() == 0) {
        zeroPolls++;
      } else {
        zeroPolls = 0;
      }
      for (ConsumerRecord<String, String> record : records) {
        if (!recordsReceived.containsKey(record.topic())) {
          recordsReceived.put(record.topic(), 0);
        }
        recordsReceived.put(record.topic(), recordsReceived.get(record.topic()) + 1);

        ArrayList<String> recordsReceivedList = new ArrayList<>();
        for (String topic : recordsReceived.keySet()) {
          recordsReceivedList.add(topic + ": " + recordsReceived.get(topic));
        }

        if (!recordsFirstReceived.containsKey(record.topic())) {
          recordsFirstReceived.put(record.topic(), System.currentTimeMillis());
          //System.out.println("Got first record from topic " + record.topic() + " after " + (System.currentTimeMillis() - startTime) + " ms");


          log.error("Got first record from topic " + record.topic() + " after " + (System.currentTimeMillis() - startTime) + " ms // " + String.join(", ", recordsReceivedList));
        }

        if (System.currentTimeMillis() - lastStatus > statusIntervall) {
          lastStatus = System.currentTimeMillis();
          log.error("Received:  " + String.join(", ", recordsReceivedList));
        }

      }
    }
  }
}
