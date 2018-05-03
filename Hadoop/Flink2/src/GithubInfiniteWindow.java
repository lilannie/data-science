import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Hashtable;
import java.util.LinkedList;

/**
 * @author Annie Steenson
 *
 */
public class GithubInfiniteWindow
{
	// Key = Langauge, Value = < num_repos, top_starred_repo, num_stars >
	static Hashtable<String, Tuple3<Integer, String, Integer>> languages = new Hashtable<>();

	public static void main(String[] args) throws Exception	
	{
		int windowSize = 200000;

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> text = env.readTextFile("hdfs:///cpre419/github.csv");

		// count by windows
		SingleOutputStreamOperator<String> counts = text
				// convert string to integer
				.flatMap(new InputParser())
				// group into windows
				.countWindowAll(windowSize)
				// apply aggregate function in each window
				.apply(new AllWindowFunction<Tuple2<String, String>, String, GlobalWindow>()
				{
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(GlobalWindow window, Iterable<Tuple2<String, String>> values, Collector<String> out) throws Exception
					{
						// For each repository
						// line = < language, repository+','+num_stars >
						for (Tuple2<String, String> line : values)
						{
							String[] repo_info = line.f1.split(",");
							Integer num_stars = Integer.parseInt(repo_info[1]);

							// We have already encountered this Language
							if (languages.containsKey(line.f0)) {

								// Get the languages < num_repos, top_starred_repo, num_stars >
								Tuple3<Integer, String, Integer> counts = languages.get(line.f0);

								// If the current repo has more stars then updated the Hashtable
								if (num_stars >= counts.f2) {
									languages.put(line.f0, new Tuple3<>(counts.f0+1, repo_info[0], num_stars));
								}
								else {
									languages.put(line.f0, new Tuple3<>(counts.f0+1, counts.f1, counts.f2));
								}
							}

							// Encountered new Language
							// Add the new language and the first repository that uses that language to the Hashtable
							else {
								languages.put(line.f0, new Tuple3<>(1, repo_info[0], num_stars));
							}

						}

						LinkedList<String> sorted = new LinkedList<String>(languages.keySet());
						sorted.sort(new Comparator<String>() {
							@Override
							public int compare(String o1, String o2) {
								return languages.get(o2).f0 - languages.get(o1).f0;
							}
						});

						for(String lanaguge: sorted) {
							Tuple3<Integer, String, Integer> counts = languages.get(lanaguge);

							// Emit <language> <num_of_repo> <name_of_repo_highest_start> <num_starts>
							out.collect(lanaguge + " " + counts.f0 + " " + counts.f1 + " " + counts.f2);
						}
					}
				});

		// emit result
		counts.writeAsText("hdfs:///user/lilannie/lab10/exp2/output");

		// execute program
		env.execute("Github Infinite Windows");
	}

	public static final class InputParser implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception  {
			String[] inputs = s.split(",");
			// Condense the string into <repository, numStars>
			String line = inputs[0] + "," + inputs[12];
			collector.collect(new Tuple2<String, String>(inputs[1], line));
		}
	}
}
