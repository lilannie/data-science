import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Github {
    private final static int numOfReducers = 8;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: WordCount <output>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("Lab 8 Exp 1");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> repositories = context.textFile("/cpre419/github.csv");

        // Parse the input
        // Map input to key = language and value = <repository, numStars>
        JavaPairRDD<String, String> languages = repositories.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] inputs = s.split(",");
                // Condense the string into <repository, numStars>
                String line = inputs[0] + "," + inputs[12];
                return new Tuple2<String, String>(inputs[1], line);
            }
        });

        // Group input by language
        JavaPairRDD<String , Iterable<String>> reposByLanguages = languages.groupByKey(numOfReducers);

        // Find the most starred repository per language
        JavaPairRDD<String, String> output = reposByLanguages.mapToPair(new PairFunction<Tuple2<String , Iterable<String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String , Iterable<String>> item) throws Exception {
                int max = 0;
                String mostStarred = null;
                int num_repos = 0;

                for (String repo_info: item._2) {
                    String[] repo = repo_info.split(",");
                    int numStars = Integer.parseInt(repo[1]);
                    if (mostStarred == null) {
                        mostStarred = repo[0];
                        max = numStars;
                    }
                    else if (numStars > max) {
                        mostStarred = repo[0];
                        max = numStars;
                    }
                    num_repos++;
                }

                return new Tuple2<String, String>(item._1 + " "+num_repos+" "+mostStarred+" "+max, "");
            }
        });

        output.repartition(1).saveAsTextFile(args[0]);
        context.stop();
        context.close();

    }
}
