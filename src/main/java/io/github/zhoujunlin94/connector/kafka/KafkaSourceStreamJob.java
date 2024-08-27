package io.github.zhoujunlin94.connector.kafka;

import cn.hutool.setting.Setting;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author zhoujunlin
 * @date 2024-08-27-10:42
 */
public class KafkaSourceStreamJob {

    public static void main(String[] args) throws Exception {
        Setting setting = new Setting("conf.setting");
        Properties kafkaProps = setting.getProperties("kafka");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink-kafka-test", new SimpleStringSchema(), kafkaProps);
        consumer.setStartFromLatest();

        DataStreamSource<String> kafkaDS = env.addSource(consumer);

        SingleOutputStreamOperator<String> streamOperator = kafkaDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                JSONObject json = JSONObject.parseObject(value);
                collector.collect("姓名：" + json.getString("userName") + ", 排名：" + json.getInteger("idx"));
            }
        }).setParallelism(2);

        streamOperator.print();

        env.execute("flink kafka connector test");
    }

}
