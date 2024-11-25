package com.example.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerPartitionAssign {
    public static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssign.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek");
//        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        TopicPartition topicPartition = new TopicPartition(topicName,0);

//        kafkaConsumer.subscribe(List.of(topicName));

        kafkaConsumer.assign(Arrays.asList(topicPartition));
        //main thread 참조 변수
        Thread mainThread = Thread.currentThread();

//        kafkaConsumer.wakeup(); //메인 프로그램이 죽을떄, 메인 스레드 죽기 전에 너가 하고싶은 마지막 유언?을 남기는 방법....
        //
        //셧다운 할때, 훅을 건다.
        //main스레드 종료시 별도의 thread로 kafkaConsumer wakeup()을 호출하게 한다.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("main program start to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
                    e.printStackTrace();
                }

            }

        });


//        kafkaConsumer.close();
//        pollAutoCommit(kafkaConsumer);
        pollCommitSync(kafkaConsumer);
//        pollCommitAsync(kafkaConsumer);

    }

    private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                logger.info("###### loopCnt = {} consumerRecords count: {}", loopCnt++, consumerRecords.count());

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {} , partition: {}, record offset: {} // record value: {},", record.key(), record.partition(), record.offset(), record.value());
                }

                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if(exception != null){
                            logger.error("offsets {} is not completed, error {}",offsets.toString(),exception.getMessage());
                        }
                    }
                });

            }
        } catch (WakeupException wakeupException) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }


    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        {

            int loopCnt = 0;
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                    logger.info("###### loopCnt = {} consumerRecords count: {}", loopCnt++, consumerRecords.count());

                    for (ConsumerRecord record : consumerRecords) {
                        logger.info("record key: {} , partition: {}, record offset: {} // record value: {},", record.key(), record.partition(), record.offset(), record.value());
                    }
                    try {
                        if (consumerRecords.count() > 0) {
                            kafkaConsumer.commitSync();
                            logger.info("commit sync has been called");
                        }
                    } catch (CommitFailedException e) {
                        logger.error(e.getMessage());

                    }
                }
            } catch (WakeupException wakeupException) {
                logger.error("wakeup exception has been called");
            } finally {
                logger.info("finally consumer is closing");
                kafkaConsumer.close();
            }


        }
    }

    private static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {

        int loopCnt = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                logger.info("###### loopCnt = {} consumerRecords count: {}", loopCnt++, consumerRecords.count());

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {} , partition: {}, record offset: {} // record value: {},", record.key(), record.partition(), record.offset(), record.value());
                }
                try {
                    logger.info("main thread is sleeping {} ms during while loop", 10000);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();

                }
            }
        } catch (WakeupException wakeupException) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }


    }
}
