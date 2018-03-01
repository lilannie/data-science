import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class CheckSum {
 public static void main ( String [] args ) throws Exception {
 // The system configuration
 Configuration conf = new Configuration();
 // Get an instance of the Filesystem
 FileSystem fs = FileSystem.get(conf);

 String path_name = "/cpre419/bigdata";

 Path path = new Path(path_name);

 // The Output Data Stream to write into
 FSDataInputStream file = fs.open(path);

 // Calculate 8-bit checksum
 byte[] buffer = new byte[1000];
 long position = 1000000000;
 file.readFully(position, buffer, 0, 1000);
 
 byte sum = buffer[0];
 for (int i = 1; i < buffer.length; i++) {
	 sum ^= buffer[i];
 } 
 System.out.println(sum);

 // Close the file and the file system instance
 file.close();
 fs.close();
 }
}
