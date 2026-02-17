package app;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import java.util.Iterator;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

public class FirstExercise {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableGenericTypes();

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("sensors")
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> s1 =
				env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


		DataStream<Entry> s2 = s1.map(json -> Entry.parseJson(json));


		DataStream<String> windowCounts =
				s2
						.keyBy(
								entry -> entry.key
						)
						.window(
								SlidingProcessingTimeWindows.of(Time.milliseconds(8000L), Time.milliseconds(6000L))
						)
								.process(new ProcessWindowFunction<>() {
                                    @Override
                                    public void process(
                                            String key,
                                            Context ctx,
                                            Iterable<Entry> elements,
                                            Collector<String> out) {

										int count = 0;
										Iterator<Entry> it = elements.iterator();
										while (it.hasNext()) {
											it.next();
											count++;
										}

										String result = "[ " + ctx.window().getStart() + " : " + ctx.window().getEnd()
												+ " ], Num(" + key + ") = " + count;
                                        out.collect(result);
                                    }
                                });


		KafkaSink<String> sink = KafkaSink.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("results1")
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
		windowCounts.sinkTo(sink);

		env.execute("Prvo Baranje");
	}


	public static class Entry {
		public String key;
		public int value;
		public long time;

		public Entry(String k, int v, long t) {
			key = k;
			value = v;
			time = t;
		}
		public Entry() {
		}
		public static Entry parseJson(String json) {
			String cleaned = json.substring(1, json.length() - 1);

			String[] fields = cleaned.split(",");

			String k = null;
			int val = 0;
			long t = 0;

			for (String field : fields) {
				String[] kv = field.split(":");
				String ime = kv[0].trim().replace("\"", "");
				String v = kv[1].trim().replace("\"", "");

				switch (ime) {
					case "key":
						k = v;
						break;
					case "value":
						val = Integer.parseInt(v);
						break;
					case "timestamp":
						t = Long.parseLong(v);
						break;
				}
			}

			return new Entry(k, val, t);
		}
	}

}
