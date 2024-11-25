package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class SimpleProducerSync {
    public static final Logger logger= LoggerFactory.getLogger(SimpleProducerSync.class.getName());
    public static void main(String[] args){


        String topicName = "simple-topic";

        Properties props = new Properties();


//        props.setProperty("bootstrap.servers","192.168.1.163:9092");  이것도 된다. 아래것도 되고.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,"hello World4");

//        Future<RecordMetadata> send = kafkaProducer.send(producerRecord); 이렇게 해도 되는데 보통은 아래 방법으로 한다.

        try {

            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n##### recordMetadata received ### \n"+
                    "partition:"+recordMetadata.partition() + "\n"+
                    "offset : "+recordMetadata.offset() +"\n"+
                    "timestamp : "+recordMetadata.timestamp());
        } catch (InterruptedException e) {  //스레드다보니까.
            e.printStackTrace();
//            throw new RuntimeException(e);
        } catch (ExecutionException e) {  //동기화 작업이다보니까.
            e.printStackTrace();
//            throw new RuntimeException(e);
        }
        finally {
            kafkaProducer.close();  //반드시 exception이 발생하면 close를 시켜줘야 한다.
        }


        kafkaProducer.flush();
        kafkaProducer.close();

    }

}
