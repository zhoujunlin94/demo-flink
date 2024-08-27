package io.github.zhoujunlin94.connector.kafka2mysql;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import io.github.zhoujunlin94.common.SettingFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author zhoujunlin
 * @date 2024-08-27-10:42
 */
public class Kafka2MysqlStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        Properties kafkaProps = SettingFactory.CONF_SETTING.getProperties("kafka");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink-kafka-test", new SimpleStringSchema(), kafkaProps);
        consumer.setStartFromEarliest();

        DataStreamSource<String> kafkaDS = env.addSource(consumer);

        kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                JSONObject json = JSONObject.parseObject(value);
                String tableName = json.getString("table_name");
                if (StrUtil.isBlank(tableName)) {
                    return;
                }
                collector.collect(json);
            }
        }).addSink(new SinkToMySQL());

        env.execute("flink kafka 2  mysql test");
    }

}
