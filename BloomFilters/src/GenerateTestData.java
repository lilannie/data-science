import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * @author Annie Steenson
 * This class generates two files each with a million randomly generated data points
 */
public class GenerateTestData {
    private static int numTestCases = 1000000;

    public static void main(String[] args) {
        try {
            File f = new File("ExampleStringData.txt");
            PrintWriter p = new PrintWriter(f);

            for (int i = 0; i < numTestCases; i++) {
                p.print(generateRandomString()+"\n");
            }

            p.flush();

            Random rand = new Random();
            f = new File("ExampleIntegerData.txt");
            p = new PrintWriter(f);

            for (int i = 0; i < numTestCases; i++) {
                p.print(rand.nextInt(Integer.MAX_VALUE)+"\n");
            }

            p.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static String generateRandomString() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }
}
