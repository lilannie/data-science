import javafx.scene.effect.Bloom;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * @author Annie Steenson
 */
public class BloomJoin {
    String r1;
    String r2;
    BloomFilter filter;

    /**
     * This instantiates a class that will read the tuples from r1 and store the join property in a BloomFilter
     * @param r1 String - absolute path to file with data to join
     * @param r2 String - absolute path to file with data to join
     */
    public BloomJoin(String r1, String r2) {
        this.r1 = r1;
        this.r2 = r2;
        this.filter = new BloomFilterFNV(2000000, 3);

        try {
            processFile(r1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Joins data in r1 and r2 and outputs the join to r3
     * This method uses the bloom filter and a hashmap to do the join work efficiently
     * @param r3 String
     */
    public void join(String r3) {
        try {
            File fileR2 = new File(r2);
            Scanner scanR2 = new Scanner(fileR2);
            HashMap<String, LinkedList<String>> map = new HashMap<>();

            while (scanR2.hasNextLine()) {
                String line = scanR2.nextLine();
                String[] input = line.split("   ");

                if (filter.appears(input[0])) {
                    if (!map.containsKey(input[0])) {
                        map.put(input[0], new LinkedList<>());
                    }

                    map.get(input[0]).add(input[1]);
                }
            }

            File fileR3 = new File(r3);
            PrintWriter out = new PrintWriter(fileR3);

            // Join r1 with the map of h2
            Scanner scanR1 = new Scanner(new File(r1));
            while (scanR1.hasNextLine()) {
                String[] input = scanR1.nextLine().split("   ");

                if (map.containsKey(input[0])) {
                    for (String s: map.get(input[0])) {
                        out.print(input[1] + " " + input[0] + " " + s+"\n");
                    }
                }
            }

            out.flush();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * This helper method processes a file and stores it in the BloomFilter attached to the class instantiation
     * @param file String
     * @throws FileNotFoundException when file is not found
     */
    private void processFile(String file) throws FileNotFoundException {
        File f = new File(file);
        Scanner scan = new Scanner(f);

        while (scan.hasNextLine()) {
            String[] input = scan.nextLine().split("   ");

            // Add join attribute
            this.filter.add(input[0]);
        }
    }

    /**
     * Method to test BloomJoin class
     * @param args String[]
     */
    public static void main(String[] args) {
        String exampleR1 = "/Users/lilannaye/Development/Classes/data-science/ExampleRelation1.txt";
        String exampleR2 = "/Users/lilannaye/Development/Classes/data-science/ExampleRelation2.txt";
        String exampleJoin = "ExampleJoin.txt";

        String relation1 = "/Users/lilannaye/Development/Classes/data-science/Relation1.txt";
        String relation2 = "/Users/lilannaye/Development/Classes/data-science/Relation2.txt";
        String join = "join.txt";

        BloomJoin bj = new BloomJoin(exampleR1, exampleR2);
        bj.join(exampleJoin);

        bj = new BloomJoin(relation1, relation2);
        bj.join(join);
    }
}
