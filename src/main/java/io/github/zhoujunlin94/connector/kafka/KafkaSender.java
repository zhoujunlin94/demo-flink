package io.github.zhoujunlin94.connector.kafka;

import com.alibaba.fastjson.JSONObject;
import io.github.zhoujunlin94.common.SettingFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author zhoujunlin
 * @date 2024-08-27-11:36
 */
public class KafkaSender {

    private static KafkaProducer<String, Object> kafkaProducer() {
        Properties kafkaProps = SettingFactory.CONF_SETTING.getProperties("kafka");
        return new KafkaProducer<>(kafkaProps);
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, Object> producer = kafkaProducer();

        for (int i = 1; i <= 100; i++) {
            String msg = new JSONObject().fluentPut("table_name", "t_order").fluentPut("user_id", i).fluentPut("order_token", "ORDER" + i).toJSONString();
            ProducerRecord<String, Object> record = new ProducerRecord<>("flink-kafka-test", null, null, msg);
            producer.send(record);
            System.out.println("发送数据: " + msg);
            Thread.sleep(1000);
        }

        producer.flush();
    }

}
