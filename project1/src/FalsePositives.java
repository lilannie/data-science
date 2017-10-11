import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * @author Annie Steenson
 * This class is used to test the False Positivity of each BloomFilter
 */
public class FalsePositives {
    private BloomFilter filter;
    private int setSize = 500000;
    private String testFile = "/Users/lilannaye/Development/Classes/data-science/ExampleStringData.txt";

    /**
     * This method adds half the data from testFile to a filter and then calculates the False Positivity from the
     * other half of the data from the file
     */
    public void findFalsePositive() {
        int countInput = setSize;
        int numIncorrect = 0;
        long startTime, endTime, totalTime;

        try {
            File f = new File(testFile);
            Scanner scan = new Scanner(f);

            startTime = System.currentTimeMillis();

            while (scan.hasNextLine() && countInput > 0) {
                filter.add(scan.nextLine().trim());
                countInput--;
            }

            endTime   = System.currentTimeMillis();
            totalTime = endTime - startTime;
            System.out.println("Total time to add "+setSize+" elements: "+totalTime+" ms");

            countInput = setSize;

            startTime = System.currentTimeMillis();

            while (scan.hasNextLine() && countInput > 0) {
                if(filter.appears(scan.nextLine().trim())) {
                    numIncorrect++;
                }
                countInput--;
            }

            endTime   = System.currentTimeMillis();
            totalTime = endTime - startTime;
            System.out.println("Total time to check "+setSize+" elements: "+totalTime+" ms");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        System.out.println("False positive rate: "+((double)numIncorrect/(double)setSize));

    }

    /**
     * This is a method used to test the False Positivity
     * @param args String[]
     */
    public static void main(String[] args) {
        FalsePositives fp = new FalsePositives();
        int bitsPerElement = 16;

        System.out.println("-- FNV --");
        fp.filter = new BloomFilterFNV(fp.setSize, bitsPerElement);
        fp.findFalsePositive();

        System.out.println("----------------------");

        System.out.println("-- Murmur --");
        fp.filter = new BloomFilterMurmur(fp.setSize, bitsPerElement);
        fp.findFalsePositive();

        System.out.println("----------------------");

        System.out.println("-- Random --");
        fp.filter = new BloomFilterRan(fp.setSize, bitsPerElement);
        fp.findFalsePositive();

        System.out.println("----------------------");

        System.out.println("-- Dynamic --");
        fp.filter = new DynamicFilter(bitsPerElement);
        fp.findFalsePositive();
    }
}
