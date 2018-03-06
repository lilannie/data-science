import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonRecordReader extends RecordReader <LongWritable, Text> {
	private LineReader lineReader = null;
    private LongWritable key = null;
    private Text value = null;
    private long start, end, position, no_of_calls;

    private boolean foundEndFile = false;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(conf);

        start = split.getStart();
        end = start + split.getLength();
        position = start;

        FSDataInputStream input_file = fs.open(split.getPath());
        input_file.seek(start);

        lineReader = new LineReader(input_file, conf);

        no_of_calls = 0;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (position - start) / (float)(end - start));
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public void close() throws IOException {
        if ( lineReader != null )
            lineReader.close();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (foundEndFile)
            return false;

        no_of_calls = no_of_calls + 1;

        // Begin reading keyvalue pairs
        if (key == null && value == null) {
            key = new LongWritable();
            value = new Text(" ");

            lineReader.readLine(new Text()); // Read first line "["
            no_of_calls = no_of_calls + 1;
        }

        if (foundTweet()) {
            key.set(no_of_calls);
            return true;
        }

        return false;
    }

    private boolean foundTweet() throws IOException {
        String json = "";
        Text temp_text = new Text(" ");

        int read_length = lineReader.readLine(temp_text);
        position = position + read_length;
        json += temp_text.toString();

        // Terminate if you find the end of the file "]"
        if (json.compareTo("]") == 0) {
            foundEndFile = true;
            return false;
        }

        Stack<Character> stack = new Stack<>();
        if (temp_text.toString().contains("{")) stack.push('b'); // b for beginning bracket
        else return false;  // NOTE: Does not handle invalid JSON Objects

        while (!stack.isEmpty()) {
            read_length = lineReader.readLine(temp_text);
            position = position + read_length;

            String temp_text_str = temp_text.toString();
            json += temp_text_str;

            // b for beginning bracket
            if (temp_text_str.contains("{") || temp_text_str.contains("[")) stack.push('b');
            // Found ending bracket
            if (temp_text_str.contains("}") || temp_text_str.contains("]")) stack.pop();

            // NOTE: Does not handle invalid JSON Objects
        }

        if (json.charAt(json.length()-1) == ',') json = json.substring(0, json.length() - 1);

        value.set(json);
        return true;
    }
}
