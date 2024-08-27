package io.github.zhoujunlin94.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhoujunlin
 * @date 2024/8/24 09:25
 * DataStreamAPI读取有界流  文件
 */
public class WordCountStreamJob {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据：从文件读
        DataStreamSource<String> txtDS = env.readTextFile("input/words.txt");

        // 3. 处理数据：切分、转换、分组、聚合

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2Operator = txtDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : value.split(" ")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tuple2Operator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumOperator = keyedStream.sum(1);

        // 4. 输出
        sumOperator.print();

        // 5. 执行
        env.execute();
    }


}
