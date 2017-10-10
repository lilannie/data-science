import javafx.scene.effect.Bloom;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;

public class BloomJoin {
    String r1;
    String r2;
    BloomFilter filter;

    public BloomJoin(String r1, String r2) {
        this.r1 = r1;
        this.r2 = r2;
        try {
            processFile(r1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

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
            Scanner scanR1 = new Scanner(r1);
            while (scanR1.hasNextLine()) {
                String[] input = scanR1.nextLine().split("   ");
                if (map.containsKey(input[0])) {
                    for (String s: map.get(input[0])) {
                        out.print(input[1] + " " + input[0] + " " + s);
                    }
                }
            }
            out.flush();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void processFile(String file) throws FileNotFoundException {
        DynamicFilter filter = null;

        File f = new File(file);
        Scanner scan = new Scanner(f);

        while (scan.hasNextLine()) {
            // Assuming 2-ary relations
            String[] input = scan.nextLine().split("   ");
            if (filter == null) {
                // TODO Make sure this math is correct for bitsPerElement;
                filter = new DynamicFilter(input[0].getBytes().length*8);
            }

            // Add join attribute
            filter.add(input[0]);
        }

        this.filter = filter;
    }
}
