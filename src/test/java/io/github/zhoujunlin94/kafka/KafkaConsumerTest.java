package io.github.zhoujunlin94.kafka;

import io.github.zhoujunlin94.common.SettingFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author zhoujunlin
 * @date 2024-08-27-15:41
 */
public class KafkaConsumerTest {

    private static KafkaConsumer<String, Object> kafkaConsumer() {
        Properties kafkaProps = SettingFactory.CONF_SETTING.getProperties("kafka");
        return new KafkaConsumer<>(kafkaProps);
    }

    public static void main(String[] args) {
        KafkaConsumer<String, Object> kafkaConsumer = kafkaConsumer();

        // 订阅一个或多个主题
        kafkaConsumer.subscribe(Collections.singletonList("flink-kafka-test"));

        try {
            while (true) {
                // 轮询Kafka，获取数据
                ConsumerRecords<String, Object> records = kafkaConsumer.poll(Duration.ofMillis(100));

                // 处理消息
                for (ConsumerRecord<String, Object> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n",
                            record.offset(), record.key(), record.value());
                }
            }
        } finally {
            // 关闭消费者
            kafkaConsumer.close();
        }
    }

}
