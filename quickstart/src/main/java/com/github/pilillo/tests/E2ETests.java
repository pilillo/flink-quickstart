package com.github.pilillo.tests;

import com.github.pilillo.StreamingJob;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class E2ETests extends AbstractTestBase {
    // https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html#integration-testing

    long windowSize = 10;
    long triggerInterval = 5;
    long allowedLag = 0;
    Integer splittingValue = 2; // 1 folder with users with max 1 event, 1 folder with users with >= 2 events

    // https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/connectors/filesystem_sink.html
    String bucketPath = "test_files/path";
    long batchSize = 2048; // e.g. 1KB = 1024, 1 MB = (1024 * 1024), 1MB * 400 is 400 MB

    long batchRolloverInterval = 6000; // default is 1 min = 60 000 ms, 6 seconds is enough for the test
    String datePartitioningFormat = "yyyy-MM-dd--HHmm";
    String timezone = "Europe/San_Marino";

    @Test
    public void e2eTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set stream time to event time, using the watermark
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // set parallelism to 1 to simplify testing
        env.setParallelism(1);


        DataStream<String> input = env.fromElements(
                " {\"userip\":\"77.158.183.15\", \"userid\":-1, \"asset\":\"1234\", \"timestamp\":0}",
                " {\"userip\":\"77.158.183.15\", \"userid\":100, \"asset\":\"1234\", \"timestamp\":"+triggerInterval+"}",
                " {\"userip\":\"77.158.183.15\", \"userid\":100, \"asset\":\"973\", \"timestamp\":"+(triggerInterval+1)+"}",
                " {\"userip\":\"77.158.183.15\", \"userid\":100, \"asset\":\"914\", \"timestamp\":"+(2*triggerInterval+1)+"}",
                " {\"userip\":\"77.158.183.15\", \"userid\":100, \"asset\":\"999\", \"timestamp\":"+(2*triggerInterval+5)+"}",
                " {\"userip\":\"77.158.183.15\", \"userid\":200, \"asset\":\"1234\", \"timestamp\":"+(2*triggerInterval+4)+"}",
                " {\"userip\":\"77.158.183.15\", \"userid\":200, \"asset\":\"14\", \"timestamp\":"+(2*triggerInterval+5)+"}",
                " {\"userip\":\"77.158.183.15\", \"userid\":100, \"asset\":\"134\", \"timestamp\":"+(4*triggerInterval+1)+"}",
                " {\"userip\":\"77.158.183.15\", \"userid\":200, \"asset\":\"189\", \"timestamp\":"+(4*triggerInterval+1)+"}"
        );

        final ObjectMapper objectMapper = new ObjectMapper();
        // https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_timestamp_extractors.html#assigners-allowing-a-fixed-amount-of-lateness
        DataStream<String> withTimestampsAndWatermarks = input.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<String>(
                        // based on the ClickEventGen the max out of order lag is of 60 secs
                        Time.seconds(allowedLag)) {

                    @Override
                    public long extractTimestamp(String element) {
                        // retrieve the timestap
                        long timestamp = 0L;
                        JsonNode jsonNode = null;
                        try {
                            jsonNode = objectMapper.readTree(element);
                            timestamp = jsonNode.get("timestamp").asLong();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        //System.err.println("Returning watermark "+timestamp);
                        return timestamp;
                    }
                });

        // output values are collected in a static collection (reset it every time the test is ran)
        CollectSink.values.clear();

        // set the streaming topology to test
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> counts = StreamingJob.getProcessor(withTimestampsAndWatermarks, windowSize, triggerInterval, Time.of(allowedLag, TimeUnit.MILLISECONDS));
                
        // add mock sink to collect results
        counts.addSink(new CollectSink());


        // split stream in 2 different branches
        Tuple2<SingleOutputStreamOperator, SingleOutputStreamOperator> results = StreamingJob.splitStream(counts, splittingValue);

        String activeUsersPath = Paths.get(bucketPath, "active").toString();
        String regularUsersPath = Paths.get(bucketPath, "regular").toString();

        // add file sinks to both branches
        results.f0.addSink(StreamingJob.getFileSink(activeUsersPath, batchSize, batchRolloverInterval, datePartitioningFormat, ZoneId.of(timezone)));
        results.f1.addSink(StreamingJob.getFileSink(regularUsersPath, batchSize, batchRolloverInterval, datePartitioningFormat, ZoneId.of(timezone)));

        // execute
        env.execute();


        // verify results
        assertEquals(
                Sets.newHashSet(    Tuple3.of("100",10L,2),
                                    Tuple3.of("100",15L,3),
                                    Tuple3.of("200",15L,1),
                                    Tuple3.of("200",20L,2),
                                    Tuple3.of("100",20L,2),
                                    Tuple3.of("200",25L,2),
                                    Tuple3.of("100",25L,2),
                                    Tuple3.of("100",30L,1),
                                    Tuple3.of("200",30L,1)
                ),
                CollectSink.values);


        // make sure the results were correctly split to the target folders and files
        ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();

        // create a configuration object
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);

        File[] activeUsersDirs = new File(activeUsersPath).listFiles();
        assertEquals(1, activeUsersDirs.length);

        File[] regularUsersDirs = new File(regularUsersPath).listFiles();
        assertEquals(1, regularUsersDirs.length);

        List<String> linesActiveUsers = env2.readTextFile(Paths.get(activeUsersDirs[0].getPath(),"_part-0-0.pending").toString())
                                     .withParameters(parameters).collect();
        List<String> linesRegularUsers = env2.readTextFile(Paths.get(regularUsersDirs[0].getPath(),"_part-0-0.pending").toString())
                .withParameters(parameters).collect();


        Set<Tuple3> printSRes = Sets.newHashSet();
        // check active users
        for(String line : linesActiveUsers){
            String[] e = line.replaceAll("[()]", "").split(",");
            int c = Integer.parseInt(e[2]);
            assertTrue(c >= splittingValue);
            printSRes.add(Tuple3.of(e[0], Long.parseLong(e[1]), c));
        }
        // check regular users
        for(String line : linesRegularUsers){
            String[] e = line.replaceAll("[()]", "").split(",");
            int c = Integer.parseInt(e[2]);
            assertTrue(c < splittingValue);
            printSRes.add(Tuple3.of(e[0], Long.parseLong(e[1]), c));
        }

        // compare saved results with collected sink
        assertEquals(printSRes, CollectSink.values);

    }

    @After
    public void deleteTestResults() {
        // remove result folder
        try {
            Files.walk(Paths.get(bucketPath))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //new File(bucketPath).delete(); // works only on empty folders
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Tuple3<String, Long, Integer>> {

        // must be static
        public static final Set<Tuple3<String, Long, Integer>> values = new HashSet<>();

        @Override
        public synchronized void invoke(Tuple3<String, Long, Integer> value) throws Exception {
            values.add(value);
        }
    }

}
