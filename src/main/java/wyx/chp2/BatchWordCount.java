package wyx.chp2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> textDS = env.readTextFile("input/word.txt");
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = textDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });
        //按照word进行分组，求出不同组内的单词数，0和1都是Tuple里的相对位置
        wordAndOne.groupBy(0).sum(1).print();
        //如下形式会报错，Aggregate does not support grouping with KeySelector functions, yet.
//        wordAndOne.groupBy(new KeySelector<Tuple2<String, Long>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Long> value) throws Exception {
//                return value.f0;
//            }
//        }).sum(1).print();
    }
}
