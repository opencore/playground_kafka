package com.opencore.ruv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
      // Producer code
      Properties producerProps = new Properties();
      String kafkaPort = "9099";
      producerProps.put("bootstrap.servers", "127.0.0.1:" + kafkaPort); // Kafka Broker
      producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      KafkaProducer <String, String> producer = new KafkaProducer<String, String>(producerProps);

      // dieser Aufruf sendet noch nicht wirklich, der Producer batched nach eigenem Ermessen, die zurückgegebene Future enthält
      // später das Ergebnis, wenn wirklich gesendet wurde

      String topic = "singletest" + System.currentTimeMillis();
      ArrayList<Future> results = new ArrayList<>();
      int numberOfRecords = 10;



      //ProducerRecord record = new ProducerRecord<String, String>("topic", "testnachricht");


      //ProducerRecord record = new ProducerRecord<String, String>("topic", null, System.currentTimeMillis(),"testkey", "testnachricht");


      List<Header> headers = new ArrayList<>();
      headers.add(new RecordHeader("transaktion", "testtransaktion".getBytes()));
      headers.add(new RecordHeader("kunde", "testkunde".getBytes()));

      ProducerRecord record = new ProducerRecord<String, String>("topic", null, "testkey", "testnachricht", headers);



      for (int i = 1; i <= numberOfRecords; i++) {
        results.add(producer.send(new ProducerRecord<String, String>(topic,"key-" + i, "value-" + i)));
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

      System.out.println( "Sent " + results.size() + " records!" );

      // Consumer code
      // exemplarisch in einer Schleife, dies müßte für euren Code so umgebaut werden, dass
      // ihr regelmäßig nachfragt, ob es neue Nachrichten gibt (Sekundentakt optimalerweise)
      Properties consumerProperties = new Properties();
      consumerProperties.put("bootstrap.servers", "127.0.0.1:" + kafkaPort); // der Kafka Broker der angesprochen werden soll
      consumerProperties.put("auto.offset.reset", "none"); // gibt an, ob ihr initial Daten erhaltet oder am Ende des Topics zu lesen anfangt, wenn eine Consumer Group das allererste Mal verwendet wird
      consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // wenn wir zu json serialisieren können wir diesen einfach als String übertragen
      consumerProperties.put("group.id","ruv2"); // Consumer Group
      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

      consumer.poll(1000);
      //TopicPartition singleTestPart0 = new TopicPartition(topic, 0);

     // consumer.assign(Collections.singletonList(singleTestPart0));
  //    long lastOffset = consumer.position(singleTestPart0);
     // consumer.seek(singleTestPart0, 11);
      consumer.subscribe(Collections.singletonList("test"));

//      System.out.println("Last offset: " + lastOffset);
//      System.out.println("Current offset: " + consumer.position(singleTestPart0));



      for (int i = 0; i < 3; i++) {
        ConsumerRecords<String, String> records = consumer.poll(100); // nach neuen Records fragen
        for (ConsumerRecord<String, String> recordRead : records) {
          System.out.println(recordRead.offset() + ": " + recordRead.key() + "-" + recordRead.value());

        }
        consumer.commitAsync(); // Offset Commit Request absenden, aber nicht auf ein Ergebnis warten
        // oder
        consumer.commitSync();  // blockiert bis der Request abgeschlossen ist

      }
    }
}
