import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class BloomJoin {
    public BloomJoin(String r1, String r2) {

    }

    public void join(String r3) {
        try {
            File f = new File(r3);
            PrintWriter pw = new PrintWriter(f);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
