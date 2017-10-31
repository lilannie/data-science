import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class LSH {
    private int[][] minHashMatrix;
    private int bands;
    private HashMap<String, Integer> docIndex;

    private int numDocuments;
    private int numHashFunctions;
    private int rowsPerBand;
    private boolean rowsDivideEqually;

    private ArrayList<HashMap<Integer, HashSet<String>>> bandTables;
    private HashFunctionRan[] hashfunctions;

    /**
     *
     * @param minHashMatrix the MinHash matrix of the document collection
     * @param docNames an array of Strings consisting of names of documents/les in the
     *                 document collection
     * @param bands the number of bands to be used to perform locality sensitive hashing
     */
    public LSH(int[][] minHashMatrix, String[] docNames, int bands) {
        this.minHashMatrix = minHashMatrix;
        this.bands = bands;
        docIndex = new HashMap<>();

        numDocuments = minHashMatrix.length;
        numHashFunctions = minHashMatrix[0].length;
        rowsPerBand = numHashFunctions / bands;
        rowsDivideEqually = numHashFunctions % bands == 0;

        // For each band initialize a hashTable
        bandTables = new ArrayList<>();
        for (int i = 0; i < bands; i++) {
            bandTables.add(new HashMap<>());
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

            // If it is the last band, account for any extra rows
            int extraRows = 0;
            if (currBand == bands - 1 && !rowsDivideEqually) {
                extraRows = (numHashFunctions) - (rowsPerBand * bands);
            }

            // For each document
            for (int currDoc = 0; currDoc < numDocuments; currDoc++) {
                // Store the document's index
                docIndex.put(docNames[currDoc], currDoc);

                int startRowIndex = currBand * rowsPerBand;
                int endRowIndex = (currBand * rowsPerBand) + rowsPerBand + extraRows;

                // Hash the string
                int hashVal = hashFunc.hash(Arrays.copyOfRange(minHashMatrix[currDoc], startRowIndex, endRowIndex));
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

            // If it is the last band, account for any extra rows
            int extraRows = 0;
            if (currBand == bands - 1 && !rowsDivideEqually) {
                extraRows = (numHashFunctions) - (rowsPerBand * bands);
            }

            int currDocIndex = docIndex.get(docName);
            int startRowIndex = currBand * rowsPerBand;
            int endRowIndex = (currBand * rowsPerBand) + rowsPerBand + extraRows;

            // Hash the string
            int hashVal = hashFunc.hash(Arrays.copyOfRange(minHashMatrix[currDocIndex], startRowIndex, endRowIndex));
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
