package io.github.zhoujunlin94.connector.mysql;

import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Entity;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zhoujunlin
 * @date 2024-08-27-10:42
 */
public class MysqlSourceStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<List<Entity>> mysqlDS = env.addSource(new MysqlTableSource());

        SingleOutputStreamOperator<String> streamOperator = mysqlDS.flatMap(new FlatMapFunction<List<Entity>, String>() {
            @Override
            public void flatMap(List<Entity> entityList, Collector<String> collector) throws Exception {
                String msg = entityList.stream().map(entity -> entity.getInt("user_id") + "-" + entity.getStr("order_token")).collect(Collectors.joining(StrUtil.COMMA));
                collector.collect(msg);
            }
        }).setParallelism(2);

        streamOperator.print();

        env.execute("flink mysql connector test");
    }

}
