package startup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCountSocket {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> stream = env.socketTextStream("localhost", 9999);
        stream.flatMap(new Tokenizer()).keyBy(r -> r.f0).sum(1).print();
        env.execute("wc-java");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) {
            for (String str: value.split(" ")) {
                collector.collect(new Tuple2<>(str, 1));
            }
        }
    }
}
