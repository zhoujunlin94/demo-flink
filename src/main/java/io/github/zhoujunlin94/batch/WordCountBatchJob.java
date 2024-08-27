package io.github.zhoujunlin94.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zhoujunlin
 * @date 2024/8/22 22:09
 * DataSet API（不推荐）实现word count  (统计input/words.txt文件中每个单词个数)
 */
public class WordCountBatchJob {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据：从文件中读取
        DataSource<String> txtDS = env.readTextFile("input/words.txt");

        // 3. 切分、转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordTuple2 = txtDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            // 将每一行字符串转换为 (每个单词,1)  例如  hello world  => (hello, 1)  (world, 1)
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 3.1 将一行单词按空格切分
                String[] words = line.split(" ");
                for (String word : words) {
                    // 3.2 将每一个单词转为 (word,1)
                    Tuple2<String, Integer> wordTuple = Tuple2.of(word, 1);
                    // 3.3 使用Collector向下游发送数据
                    out.collect(wordTuple);
                }
            }
        });

        // 4. 按照单词分组  按照二元组元素索引分组 0是word
        UnsortedGrouping<Tuple2<String, Integer>> wordGroup = wordTuple2.groupBy(0);

        // 5. 各分组聚合统计 元组中第二个元素求和
        AggregateOperator<Tuple2<String, Integer>> sum = wordGroup.sum(1);

        // 6. 输出：控制台
        sum.print();
    }

}
