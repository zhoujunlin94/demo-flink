package io.github.zhoujunlin94.connector.mysql2kafka;

import cn.hutool.db.Entity;
import com.alibaba.fastjson.JSONObject;
import io.github.zhoujunlin94.common.SettingFactory;
import io.github.zhoujunlin94.connector.mysql.MysqlSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author zhoujunlin
 * @date 2024-08-27-10:42
 */
public class Mysql2KafkaStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<List<Entity>> mysqlDS = env.addSource(new MysqlSource());

        mysqlDS.flatMap(new FlatMapFunction<List<Entity>, String>() {
            @Override
            public void flatMap(List<Entity> entityList, Collector<String> collector) throws Exception {
                for (Entity entity : entityList) {
                    collector.collect(JSONObject.toJSONString(entity));
                }
            }
        }).addSink(new FlinkKafkaProducer<String>(SettingFactory.CONF_SETTING.get("kafka", "bootstrap.servers"), "flink-kafka-test", new SimpleStringSchema()));

        env.execute("flink mysql 2 kafka test");
    }

}
