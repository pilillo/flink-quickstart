package com.github.pilillo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// get execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// open the input stream
		DataStream<String> input;

		long windowSize = 10 * 60 * 1000L;
		long triggerInterval = 60 * 1000L;
		long allowedLag = 5 * 1000L;

		String bucketPath; 				// e.g. bucketPath
		long batchSize;					// e.g. 1024 * 1024 * 400 is 400 MB
		long batchRolloverInterval;		// e.g. 20 * 60 * 1000 is 20 mins
		String datePartitioningFormat = "yyyy-MM-dd--HHmm";
		String timezone;				// e.g. "Europe/San_Marino"

		int splittingValue = 0;

		try {
			final ParameterTool pt = ParameterTool.fromArgs(args);

			// windowing details
			windowSize = pt.getLong("windowSizeMs");
			triggerInterval = pt.getLong("triggerIntervalMs");
			allowedLag = pt.getLong("allowedLagMs");
			// sink details
			bucketPath = pt.get("buketPath");
			batchSize = pt.getLong("batchSizeMs");
			batchRolloverInterval = pt.getLong("batchRolloverIntervalMs");
			datePartitioningFormat = pt.get("datePartitioningFormat");
			timezone = pt.get("timezone");
			splittingValue = pt.getInt("splittingValue");

			// add connector from one of the following
			// https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/connectors/
			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", pt.get("bootstrapServers")); // e.g. "localhost:9092"
			properties.setProperty("group.id", pt.get("groupID"));
			input = env.addSource(new FlinkKafkaConsumer<>(pt.get("topic"),
															new SimpleStringSchema(), // serialized as JSON String
															properties));

		} catch (Exception e) {
			System.err.println("Please run 'StreamingJob " +
								"--windowSizeMs <long> " +
								"--triggerIntervalMs <long> " +
								"--allowedLagMs <long>" +
								"--bucketPath <string>" +
								"--batchSizeMs <long>" +
								"--batchRolloverIntervalMs <long>" +
								"--datePartitioningFormat <string>" +
								"--timezone <string>" +
								"--splittingValue <int>" +
								"--bootstrapServers <string>" +
								"--groupID <string>" +
								"--topic <string>" +
								"'");
			return;
		}

		// we want to use the event time for the computation, as we might post-process the events (fault recovery)
		// using the watermark already defined in the ClickEventGenerator (with max 1 min delay)
		// https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html
		// https://ci.apache.org/projects/flink/flink-docs-stable/dev/event_timestamps_watermarks.html
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// configure checkpointing
		// https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/state/checkpointing.html
		// start a checkpoint every trigger interval
		env.enableCheckpointing(triggerInterval);
		// default processing is exactly-once
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		// at least half trigger interval should happen between between checkpoints
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Math.round(triggerInterval / 2));
		// checkpoints have to complete within one trigger interval, or are discarded (as old)
		env.getCheckpointConfig().setCheckpointTimeout(triggerInterval);
		// allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// enable externalized checkpoints which are retained after job cancellation
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		// set processor for the input data
		SingleOutputStreamOperator<Tuple3<String, Long, Integer>> counts = getProcessor(input, windowSize, triggerInterval, Time.of(allowedLag, TimeUnit.MILLISECONDS));
		// split stream in 2 different branches
		Tuple2<SingleOutputStreamOperator, SingleOutputStreamOperator> results = splitStream(counts, splittingValue);
		// add file sinks to both branches
		results.f0.addSink(getFileSink(Paths.get(bucketPath, "active").toString(), batchSize, batchRolloverInterval, datePartitioningFormat, ZoneId.of(timezone)));
		results.f1.addSink(getFileSink(Paths.get(bucketPath, "regular").toString(), batchSize, batchRolloverInterval, datePartitioningFormat, ZoneId.of(timezone)));

		// execute program
		env.execute("Streaming Analytics");
	}

	public static Tuple2<SingleOutputStreamOperator, SingleOutputStreamOperator>
			splitStream(SingleOutputStreamOperator<Tuple3<String, Long, Integer>> inStream, Integer splittingValue){
		return new Tuple2<SingleOutputStreamOperator, SingleOutputStreamOperator>(
				inStream.filter(t -> t.f2 >= splittingValue),
				inStream.filter(t -> t.f2 < splittingValue)
		);
	}


	public static BucketingSink getFileSink(String bucketPath,
											long batchSize,
											long batchRolloverInterval,
											String datePartitioningFormat,
											ZoneId partitioningTimezone){

		// https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/connectors/filesystem_sink.html
		BucketingSink<String> sink = new BucketingSink<String>(bucketPath);
		sink.setBucketer(new DateTimeBucketer<String>(datePartitioningFormat, partitioningTimezone));
		sink.setBatchSize(batchSize);
		sink.setBatchRolloverInterval(batchRolloverInterval);
		return sink;
	}
	

	public static SingleOutputStreamOperator<Tuple3<String, Long, Integer>>

		getProcessor(DataStream<String> input, long windowSize, long triggerInterval, Time allowedLag){

		final ObjectMapper objectMapper = new ObjectMapper();

		return input
				// convert string to tuple
				.map( s -> {
							JsonNode jsonNode = objectMapper.readTree(s);
							String userID = jsonNode.get("userid").asText();
							String userIP = jsonNode.get("userip").asText();
							return new Tuple3<>(userID, userIP, 1);
				}).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))

				// filter out unlogged users
				.filter( (t) -> !t.getField(0).toString().equals("-1"))
				// in future we could handle unlogged users too using their IP, now don't care
				.keyBy(0)
				// collect user actions in a sliding window of windowSize , and with a smaller trigger interval
				.window(SlidingEventTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(triggerInterval)))
				// set max allowed message lateness
				.allowedLateness(allowedLag)
				/*
				.trigger(new Trigger<Tuple3<String, String, Integer>, TimeWindow>() {
					@Override
					public TriggerResult onElement(Tuple3<String, String, Integer> stringStringLongTuple3, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
						System.out.println("on element");
						return TriggerResult.CONTINUE;
					}

					@Override
					public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
						System.out.println("on processing time");
						return TriggerResult.CONTINUE;
					}

					@Override
					public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
						System.out.println("on event time");
						return TriggerResult.FIRE;
					}

					@Override
					public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
						System.out.println("on clear");
					}
				})*/
				.reduce( (t1, t2) ->  new Tuple3<>(t1.f0, t1.f1, (int) t1.f2 + (int) t2.f2),

						new ProcessWindowFunction<Tuple3<String, String, Integer>,	// in
												Tuple3<String, Long, Integer>, 		// out
												Tuple,								// key
												TimeWindow>() {						// window
							@Override
							public void process(Tuple tuple,										// key
												Context context,									// window
												Iterable<Tuple3<String, String, Integer>> counts,	// in
												Collector<Tuple3<String, Long, Integer>> out		// out
							) throws Exception {
								Tuple3<String, String, Integer> agg = counts.iterator().next();
								Integer count = agg.getField(2);
								/*
								System.out.println("Window end! k:"+agg.getField(0)
													+" w:"+context.window()
													+" t:"+context.window().getEnd()
													+" t:"+agg.getField(1)
													+", "+count);
								*/
								out.collect(new Tuple3<String, Long, Integer>(	agg.getField(0).toString(),
																				context.window().getEnd(),
																				count));
							}
						}

				);

	}
}
