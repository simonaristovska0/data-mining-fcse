package app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;



public class SecondExercise {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("sensors")
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> s1 =
				env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		DataStream<Entry2> s2 = s1.map(json -> Entry2.parseJson(json));



		DataStream<String> aggregated =
				s2
						.keyBy(
								zapis -> zapis.key
						)
						.window(
								SlidingProcessingTimeWindows.of(Time.milliseconds(8000L), Time.milliseconds(6000L))
						)
						.process(new ProcessWindowFunction<>() {

                            @Override
                            public void process(
                                    String key,
                                    Context ctx,
                                    Iterable<Entry2> elements,
                                    Collector<String> out) {

								int total = 0;
								int smallest = Integer.MAX_VALUE;
								int largest = Integer.MIN_VALUE;
								double accumulator = 0.0;

								// Iterate once and compute all metrics
								for (Entry2 e : elements) {
									total++;
									accumulator += e.value;

									if (e.value < smallest) {
										smallest = e.value;
									}
									if (e.value > largest) {
										largest = e.value;
									}
								}

								double avg = total == 0 ? 0.0 : (accumulator / total);

								long start = ctx.window().getStart();
								long end = ctx.window().getEnd();

								StringBuilder sb = new StringBuilder();
								sb.append("{ \"key\":\"").append(key).append("\"")
										.append(", \"window_start\":").append(start)
										.append(", \"window_end\":").append(end)
										.append(", \"min_value\":").append(smallest)
										.append(", \"count\":").append(total)
										.append(", \"average\":").append(String.format("%.2f", avg))
										.append(", \"max_value\":").append(largest)
										.append(" }");

								String output = sb.toString();
								out.collect(output);
                            }
                        });


		KafkaSink<String> sink = KafkaSink.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic("results2")
						.setValueSerializationSchema(new SimpleStringSchema())
						.build()
				)
				.build();

		aggregated.sinkTo(sink);

		env.execute("Vtoro Baranje");
	}




	public static class Entry2 {
		public String key;
		public int value;
		public long time;

		public Entry2(String k, int v, long t) {
			key = k;
			value = v;
			time = t;
		}
		public Entry2() {}
		public static Entry2 parseJson(String json) {
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

			return new Entry2(k, val, t);
		}
	}

}
