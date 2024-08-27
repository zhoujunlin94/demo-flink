package io.github.zhoujunlin94.connector.kafka;

import cn.hutool.setting.Setting;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author zhoujunlin
 * @date 2024-08-27-11:36
 */
public class KafkaSender {

    public static void main(String[] args) throws InterruptedException {
        Setting setting = new Setting("conf.setting");
        Properties kafkaProps = setting.getProperties("kafka");
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        for (int i = 1; i <= 100; i++) {
            String msg = new JSONObject().fluentPut("userName", "test" + i).fluentPut("idx", i).toJSONString();
            ProducerRecord<String, String> record = new ProducerRecord<>("flink-kafka-test", null, null, msg);
            producer.send(record);
            System.out.println("发送数据: " + msg);
            Thread.sleep(1000);
        }

        producer.flush();
    }

}
