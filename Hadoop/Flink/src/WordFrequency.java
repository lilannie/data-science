import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.PriorityQueue;

public class WordFrequency {
    // Sort the word counts in decreasing order
    static Comparator<Tuple2<String, Integer>> comp = new Comparator<Tuple2<String, Integer>>() {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o2.f1 - o1.f1;
        }
    };

    // Priority que to use in the sink function
    static PriorityQueue<Tuple2<String, Integer>> wordCounts = new PriorityQueue<>(comp);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text = env.readTextFile("hdfs:///cpre419/shakespeare");

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0)
                        .sum(1);

        // Added a sink function to sort the word counts
        counts.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            // For every word count, add it to a priority queue
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                wordCounts.add(value);
            }

            // At the end of the sink function, output the word counts in decreasing order.=
            @Override
            public void close() throws Exception {
                while(!wordCounts.isEmpty()) {
                    Tuple2<String, Integer> word = wordCounts.poll();
                    System.out.println("("+word.f0+","+word.f1+")");
                }
                super.close();
            }
        });

        env.execute("Streaming WordFrequency");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
