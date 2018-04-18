import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BigramCount {
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

        // emit result
        counts.print();

        env.execute("Streaming BiagramCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Split the input into discrete sentences
            String[] sentences = value.toLowerCase().split("(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?|\\!)\\s");

            for(String sentence: sentences) {

                if (sentence.length() > 0) {
                    // Remove any punctuation from the sentence
                    String sanitized = sentence.replace(".", "")
                            .replace("!", "").replace("?", "");

                    // Split the sentence into words by using spaces
                    String[] words = sanitized.split("\\W+");

                    if (words.length > 0) {
                        String firstWord = words[0];

                        // For each bigram, output the count 1
                        for (int i = 1; i < words.length; i++) {
                            out.collect(new Tuple2<>(firstWord+" "+words[i], 1));
                        }
                    }
                }
            }
        }
    }
}
