package dataStreamApi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkTest {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String dataPath = "/home/pi/workspace/java/Flinktutorial/src/main/java/dataStreamApi/sensordata.txt";
        DataStream<String> stream = env.readTextFile(dataPath);
        DataStream<SensorReading> dataStream = stream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });



    }
}
