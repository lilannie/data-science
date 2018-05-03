import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Sample code to show how to use Streaming Windows in Flink.<br>
 * Test the samle code:<br>
 * <code> flink run -m yarn-cluster -yarncontainer 1 -c lab10.WindowsSample cpre419.jar hdfs:///cpre419/intStream hdfs:///user/<netid>/output 4 </code>
 * 
 * @author trong
 *
 */
public class WindowsSample
{
	public static void main(String[] args) throws Exception	
	{
		// countingWindow(args);
		
		infiniteWindow(args);
	}
	
	private static void countingWindow(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.out.println("Usage : TumblingWindow hdfs://<inputURL> hdfs://<outputURL> <window size>");
			System.exit(1);
		}
		
		int windowSize = Integer.parseInt(args[2]);
		
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> text = env.readTextFile(args[0]);

		// count by windows
		DataStream<Integer> counts = text
				// convert string to integer
				.map(t -> Integer.parseInt(t))
				// group into windows
				.countWindowAll(windowSize)
				// apply aggregate function in each window
				.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>()
				{
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(GlobalWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception
					{
						int windowSum = 0;
						for (int v : values)
						{
							windowSum += v;
						}
						out.collect(windowSum);
					}
				});

		// emit result
		counts.writeAsText(args[1]);

		// execute program
		env.execute("Counting Windows Example");
	}
	
	
	
	static int rollingSum;
	
	private static void infiniteWindow(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.out.println("Usage : TumblingWindow hdfs://<inputURL> hdfs://<outputURL> <print_out_period>");
			System.exit(1);
		}
		
		int windowSize = Integer.parseInt(args[2]);
		
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> text = env.readTextFile(args[0]);
		
		rollingSum = 0;
		
		// count by windows
		DataStream<Integer> counts = text
				// convert string to integer
				.map(t -> Integer.parseInt(t))
				// group into windows
				.countWindowAll(windowSize)
				// apply aggregate function in each window				
				.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>()
				{					
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(GlobalWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception
					{				
						int windowSum = 0;
						for (int v : values) 
						{
							windowSum += v;
						}
												
						rollingSum += windowSum;
						
						out.collect(rollingSum);
					}
				});

		// emit result
		counts.writeAsText(args[1]);

		// execute program
		env.execute("Infinite Windows Exsample");
	}
}
