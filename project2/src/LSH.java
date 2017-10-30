import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class LSH {
    int[][] minHashMatrix;
    int bands;

    int numDocuments;
    int numHashFunctions;
    int rowsPerBand;
    boolean rowsDivideEqually;

    ArrayList<HashMap<Integer, HashSet<String>>> bandTables;
    HashFunctionRan[] hashfunctions;


    /**
     *
     * @param minHashMatrix the MinHash matrix of the document collection
     * @param docNames an array of Strings consisting of names of documents/les in the
     *                 document collection
     * @param bands the number of bands to be used to perform locality sensitive hashing
     */
    public LSH(int[][] minHashMatrix, String[] docNames, int bands) {
        this.bands = bands;
        this.minHashMatrix = minHashMatrix;

        numDocuments = minHashMatrix.length;
        numHashFunctions = minHashMatrix[0].length;
        rowsPerBand = numHashFunctions / bands;
        rowsDivideEqually = numHashFunctions % bands == 0;

        // For each band initialize a hashTable
        bandTables = new ArrayList<>();
        for (int i = 0; i < bands; i++) {
            bandTables.add(new HashMap<Integer, HashSet<String>>());
        }

        // For each band initialize a hash function
        hashfunctions = new HashFunctionRan[bands];
        for(int i = 0; i < bands; i++){
            hashfunctions[i] = new HashFunctionRan(numDocuments);
        }// end for loop creating permutation functions


        // For each band
        for (int currBand = 0; currBand < bands; currBand++) {
            HashMap<Integer, HashSet<String>> table = bandTables.get(currBand);
            HashFunctionRan hashFunc = hashfunctions[currBand];

            // For each document
            for (int currDoc = 0; currDoc < numDocuments; currDoc++) {
                if (currBand == bands - 1 && !rowsDivideEqually) {
                    rowsPerBand += (numHashFunctions) - (rowsPerBand * bands);
                }

                // For each element in the band create a string
                String bandString = "";
                for (int currRow = 0; currRow < rowsPerBand; currRow++) {
                    int rowIndex = currRow + (currBand * rowsPerBand);
                    bandString += minHashMatrix[rowIndex];
                }

                // Hash the string
                int hashVal = hashFunc.hash(bandString);
                if (!table.containsKey(hashVal)) table.put(hashVal, new HashSet<String>());
                table.get(hashVal).add(docNames[currDoc]);
            }
        }
    }

    /**
     *
     * @param docName name of a document
     * @return an array list of names of the near duplicate documents.
     */
    public ArrayList<String> nearDuplicatesOf(String docName) {
        ArrayList<String> nearDuplicates = new ArrayList<>();

        // For each band
        for (int currBand = 0; currBand < bands; currBand++) {
            // Corresponding table and hash function respective to the current band
            HashMap<Integer, HashSet<String>> table = bandTables.get(currBand);
            HashFunctionRan hashFunc = hashfunctions[currBand];

            // For each document
            if (currBand == bands - 1 && !rowsDivideEqually) {
                rowsPerBand += (numHashFunctions) - (rowsPerBand * bands);
            }

            // For each element in the band create a string
            String bandString = "";
            for (int currRow = 0; currRow < rowsPerBand; currRow++) {
                int rowIndex = currRow + (currBand * rowsPerBand);
                bandString += minHashMatrix[rowIndex];
            }

            // Hash the string
            int hashVal = hashFunc.hash(bandString);
            nearDuplicates.addAll(table.get(hashVal));
        }

        return nearDuplicates;
    }

    public static void main(String args[]) {
        String base_dir = System.getProperty("user.dir") + "/project2/space/";
        MinHash mh = new MinHash(base_dir, 200);
        int[][] minHashSig = mh.minHashMatrix();
        String[] docNames = mh.allDocs();
        LSH lsh = new LSH(minHashSig, docNames, 4);
    }
}
