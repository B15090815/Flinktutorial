package dataStreamApi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.Random;

public class AverageSensorReading {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        String dataPath = "/home/pi/workspace/java/Flinktutorial/src/main/java/dataStreamApi/sensordata.txt";
        DataStream<String> stream = env.readTextFile(dataPath);
        DataStream<SensorReading> dataStream = stream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // split dataStream, return SplitStream
         SplitStream<SensorReading> splitStream = dataStream.split(sensor -> {
            if (sensor.temperature > 25.0) return Collections.singletonList("high");
            else return Collections.singletonList("low");
        });

         DataStream<SensorReading> highStream = splitStream.select("high");
         DataStream<SensorReading> lowStream = splitStream.select("high");

         // connect stream
        DataStream<Tuple2<String, Double>> warning = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensor) throws Exception {
                return new Tuple2<>(sensor.id, sensor.temperature);
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warning.connect(lowStream);
        DataStream<Object> coMapStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple3<>(value.id, value.temperature, "healthy");
            }
        });

        coMapStream.print();

        env.execute("stream-api");
    }
}

class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private volatile boolean running = true;
    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random rand = new Random();

        String[] ids = new String[10];
        double[] curTmp = new double[10];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = String.format("sensor_%d", i);
            curTmp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {
            long curTime = System.nanoTime();
            for (int i = 0; i < 10; i++) {
                curTmp[i] += rand.nextGaussian() * 0.5;
                sourceContext.collect(new SensorReading(ids[i], curTime, curTmp[i]));
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
