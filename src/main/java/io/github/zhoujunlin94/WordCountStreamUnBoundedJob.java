package io.github.zhoujunlin94;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhoujunlin
 * @date 2024/8/24 09:25
 * DataStreamAPI读取无界流  Socket  linux/mac运行命令   nc -lk 9993
 * 程序不会退出  一直等事件
 * <p>
 * mvn clean package之后  提交任务到管理平台
 * io.github.zhoujunlin94.WordCountStreamUnBoundedJob
 *
 *
 * 并行度优先级：  代码设置算子 > env设置 > 提交时命令行指令 > 配置文件中配置
 */
public class WordCountStreamUnBoundedJob {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 带有本地webui的环境(http://localhost:8081)  这样就不用起flink环境了  默认并行度是电脑核心数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 全局设置并行度  应用于所有算子
        env.setParallelism(6);

        // 2. 读取数据：从文件读  socket类型的并行度只能是1
        DataStreamSource<String> txtDS = env.socketTextStream("192.168.1.100", 9993);

        // 3. 处理数据：切分、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumOperator = txtDS.flatMap((String value, Collector<Tuple2<String, Integer>> collector) -> {
                    for (String word : value.split(" ")) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                })
                // 设置flatMap并行度  局部并行度优先于全局并行度
                .setParallelism(2)
                // 解决lambda类型擦除问题
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0).sum(1);

        // 4. 输出
        sumOperator.print();

        // 5. 执行
        env.execute();
    }


}
