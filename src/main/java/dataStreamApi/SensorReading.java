package dataStreamApi;
import java.util.Random;

public class SensorReading {
    public String id;
    public long timestamp;
    public double temperature;

    public SensorReading() {}

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }

    public static void main(String[] args) throws InterruptedException {
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 5; i++) {
                Random rand = new Random();
                double t = 20.0 + rand.nextDouble() * 10.0;

                System.out.printf("sensor_%d,%d,%.2f\n", i,System.currentTimeMillis(), t);

                Thread.sleep(50);
            }
        }
    }
}
