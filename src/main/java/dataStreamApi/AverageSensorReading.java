package dataStreamApi;

import org.apache.flink.streaming.api.TieCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageSensorReading {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        DataStream<SensorReading> sensorData = env.addSource();
//        DataStream<SensorReading> avgTemp = sensorData
//                .map(r -> {
//                    Double celsius = (r.temperature - 32) * (5.0 / 9.0);
//                    return SensorReading(r.id, r.timestamp, celsius)；
//                })
//                .keyBy(r -> r.id)
//                .timeWindow(Time.seconds(5))
//                .apply()


    }
}
