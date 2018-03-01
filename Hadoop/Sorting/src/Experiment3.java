import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class Experiment3 {

    private static int num_temp_dirs = 1;
    private static int reduce_tasks = 10;

    private static String input_dir = "/cpre419/input-5k";
    private static String user_dir = "/user/lilannie/lab4/exp2/";
    private static String[] temp_dirs = new String[num_temp_dirs];
    private static String partition_dir = user_dir+"partition";
    private static String output_dir = user_dir+"output";

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        setup();
        FileSystem hdfs = FileSystem.get(new Configuration());
        cleanTempDirs(hdfs);
        cleanOutputDir(hdfs);

        exitCode = ToolRunner.run(new SampleDataDriver(), new String[]{ input_dir, temp_dirs[0] });

        //cleanTempDirs();

        System.exit(exitCode);
    }

    private static void setup() {
        for (int i = 0; i < temp_dirs.length ; i++){
            temp_dirs[i] = user_dir+"temp"+i;
        }
    }

    private static void cleanTempDirs(FileSystem hdfs) {
        try {
            Path file;

            // delete any existing directories
            for (String temp_dir: temp_dirs) {
                file = new Path(temp_dir);
                if (hdfs.exists(file)) hdfs.delete(file, true);
            }

            file = new Path(partition_dir);
            if (hdfs.exists(file)) hdfs.delete(file, true);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void cleanOutputDir(FileSystem hdfs) {
        try {
            Path file;

            file = new Path(output_dir);
            if (hdfs.exists(file)) hdfs.delete(file, true);

        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
