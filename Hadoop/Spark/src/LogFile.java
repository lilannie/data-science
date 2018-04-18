
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class LogFile {

	private final static int numOfReducers = 4;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: LogFile <part_a_output> <part_b_output>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("Lab 7 Exp 2");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> ipLines = context.textFile("/cpre419/ip_trace");

        // Get all ip logged connections
        // Map each where key = connection_id and value = log string
        JavaPairRDD<String, String> ipConnections = ipLines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] inputs = s.split(" ");
                String line = inputs[0] + " " + inputs[1] + " " + inputs[2] + " " + inputs[4];
                return new Tuple2<String, String>(inputs[1], line);
            }
        });

        JavaRDD<String> blockedLines = context.textFile("/cpre419/raw_block");

        // Get all connections with the associated actions
        // Map each where key = connection_id and value = action
        JavaPairRDD<String, String> connections = blockedLines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] inputs = s.split(" ");
                return new Tuple2<String, String>(inputs[0], inputs[1]);
            }
        });

        // Filter out the allowed connections
        JavaPairRDD<String, String> blockedConnections = connections.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> connection) {
                return connection._2.compareTo("Blocked") == 0;
            }
        });

        // Join the logged connections and the connection actions tables
        JavaPairRDD<String, Tuple2<String, String>> joinPairs = ipConnections.join(blockedConnections, numOfReducers);

        // Recreated and save the log file
        JavaPairRDD<String, String> logFile = joinPairs.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> item) throws Exception {
                return new Tuple2<String, String>(item._2._1 + " "+ item._2._2, "");
            }
        });

        logFile.saveAsTextFile(args[0]);

        // Map each source_ip address to a single count
        JavaPairRDD<String, Integer> sourceIpOnes = joinPairs.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<String, String>> item) throws Exception {
                String[] inputs = item._2._1.split(" ");
                return new Tuple2<String, Integer>(inputs[2], 1);
            }
        });

        // Add up the number of times the source_ip was blocked
        JavaPairRDD<String, Integer> sourceIpCounts = sourceIpOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        }, numOfReducers);

        // Swap the key count and value source_ip
        JavaPairRDD<Integer, String> swappedPairs = sourceIpCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }
        });

        // Sort by number of times blocked
        JavaPairRDD<Integer, String> sortedCounts = swappedPairs.sortByKey(false, numOfReducers);

        sortedCounts.saveAsTextFile(args[1]);

        context.stop();
        context.close();
	}
}