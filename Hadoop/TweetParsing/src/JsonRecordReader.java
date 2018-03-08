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

/**
 * Custom RecordReader to read JSON twitter objects
 * @author Annie Steenson
 */
public class JsonRecordReader extends RecordReader <LongWritable, Text> {
    private LineReader lineReader;
    private LongWritable key;
    private Text value;
    long start, end, position, no_of_calls;

    /*** Use this variable to mark when I am at a beginning of a input split **/
    private boolean foundBeginObject = false;

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

        // Find the first tweet object
        // Had to add this in case some tweets were divided between input splits
        Text temp_text = new Text("");
        int read_length;

        while (position < end) {
            read_length = lineReader.readLine(temp_text);
            position = position + read_length;

            if (temp_text.toString().equals("{")) {
                foundBeginObject = true;
                break;
            }
        }

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
        no_of_calls = no_of_calls + 1;

        if (position >= end)  {
            return false;
        }

        if (key == null) {
            key = new LongWritable();
        }

        if (value == null) {
            value = new Text("");
        }

        key.set(no_of_calls);

        // Look for the next tweet
        return foundTweet();
    }

    private boolean foundTweet() throws IOException {
        Text temp_text = new Text("");
        int read_length = 0;
        StringBuilder json = new StringBuilder();

        // If you are at the first object in the input split, add the beginning curly brace
        if (foundBeginObject) {
            json.append("{");
        }

        read_length = lineReader.readLine(temp_text);
        position = position + read_length;
        json.append(temp_text.toString());

        // Check if this is the end of the tweet object list
        if (json.toString().equals("]")) return false;

        // Initialize a stack to keep track of matching curly braces and brackets
        Stack<Character> stack = new Stack<>();
        if (foundBeginObject || json.toString().equals("{")) {
            stack.push('{');
            foundBeginObject = false;
        }
        else {
            return false;  // NOTE: Does not handle invalid JSON Objects
        }

        temp_text.clear();

        // Aggregate the tweet object or until we reach the end of the input split
        while (!stack.isEmpty() && position < end) {
            read_length = lineReader.readLine(temp_text);
            position = position + read_length;

            String temp_text_str = temp_text.toString();
            if (temp_text_str.equals("{")) {
                stack.push('{');
            }
            else if (temp_text_str.equals("[")) {
                stack.push('[');
            }
            else if (temp_text_str.equals("}") || temp_text_str.equals("},")) {
                stack.pop();
            }
            else if (temp_text_str.equals("]") || temp_text_str.equals("],")) {
                stack.pop();
            }

            json.append(temp_text_str);
            temp_text.clear();
        }


        // If we went past the input split end, then return false
        if (position >= end) {
            return false;
        }

        // Remove any trailing commas so the JSONParser can parse the tweet
        if (json.charAt(json.length()-1) == ',') {
            json = new StringBuilder(json.substring(0, json.length() - 1));
        }

        value.set(json.toString());
        return true;
    }
}
