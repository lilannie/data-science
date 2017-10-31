import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

public class MinHash {

    private HashMap<String, HashSet<String>> termDocMatrix; // Key = document name, Value = set of terms in document
    private HashMap<String, BitSet> docBinaryVectors;   // Key = document name, Value = binary frequency vector
    private HashFunctionRan[] permutations;     // Array of random hash functions
    private HashMap<String, Integer> documentIndex; // Key = document name, Value = column index in minHashMatrix
    private HashSet<String> collectionTerms;    // All the terms in the Collection
    private int numTerms;   // Number of terms in Collection
    private int[][] minHashMatrix;

    /**
     * folder is the name of a folder containing our
     * document collection for which we wish to construct MinHash matrix. numPermutations denotes
     * the number of permutations to be used in creating the MinHash matrix.
     * @param folder String
     * @param numPermutations int
     */
    public MinHash(String folder, int numPermutations) {
        File[] files = new File(folder).listFiles();
        termDocMatrix = new HashMap<>();
        docBinaryVectors = new HashMap<>();
        permutations = new HashFunctionRan[numPermutations];
        documentIndex = new HashMap<>();
        collectionTerms = new HashSet<>();

        for(int i = 0; i < files.length; i++) {
            // collect all terms and place them in a term-document Hashmap
            String document = files[i].getAbsolutePath();
            HashSet documentTerms = collectTerms(document);
            collectionTerms.addAll(documentTerms);    // Does not add duplicates
            termDocMatrix.put(document, documentTerms);
            documentIndex.put(document, i);
        }// end for loop over all documents

        numTerms = collectionTerms.size();

        String[] documents = allDocs();
        /*
        String[] collectionTermsArr = collectionTerms.toArray(new String[0]);

        // For every doc create a binary frequency vector
        for(int docIndex = 0; docIndex < documents.length; docIndex++) {
            String currDoc = documents[docIndex];

            // Get the set of terms contained in the current doc
            HashSet<String> termSet = termDocMatrix.get(currDoc);
            BitSet binaryVector = new BitSet(numTerms);

            // For every term check if it is in the document
            for (int termIndex = 0; termIndex < collectionTermsArr.length; termIndex++) {
                if (termSet.contains(collectionTermsArr[termIndex])) {
                    binaryVector.set(termIndex, true);
                }
                else {
                    binaryVector.set(termIndex, false);
                }
            }
            docBinaryVectors.put(currDoc, binaryVector);
        }
        */

        // Create an array of permutation functions
        for(int i = 0; i < numPermutations; i++){
            permutations[i] = new HashFunctionRan(numTerms);
        }// end for loop creating permutation functions

        minHashMatrix = new int[documents.length][numPermutations];

        for(int i = 0; i < documents.length; i++){
            minHashMatrix[i] = minHashSig(documents[i]);
        }// end for loop creating our minHash matrix
    }// end MinHash constructor

    /**
     * Returns an array of String consisting of all the names of
     * files in the document collection.
     * @return String[]
     */
    public String[] allDocs() {
        Set<String> keys = documentIndex.keySet();
        return keys.toArray(new String[keys.size()]);
    }// end function allDocs

    /**
     * Get names of two les (in the document collection) file1 and file2 as
     * parameters and returns the exact Jaccard Similarity of the les.
     * @param file1 String
     * @param file2 String
     * @return double
     */
    public double exactJaccard(String file1, String file2) {
        BitSet binaryVector1 = (BitSet) docBinaryVectors.get(file1).clone();
        BitSet binaryVector2 = docBinaryVectors.get(file2);
        binaryVector1.and(binaryVector2);
        int dotProduct = binaryVector1.cardinality();
        double magnitude1 = Math.sqrt(termDocMatrix.get(file1).size());
        double magnitude2 = Math.sqrt(termDocMatrix.get(file2).size());

        return dotProduct / ( Math.pow(Math.abs(magnitude1), 2) + Math.pow(Math.abs(magnitude2), 2) - dotProduct );
    }// end function exactJaccard

    /**
     * Returns the MinHash the minhash signature of the document
     * named fileName, which is an array of ints.
     * @param fileName String
     * @return int[]
     */
    public int[] minHashSig(String fileName) {
        int[] minHashSig = new int[numPermutations()];
        HashSet<String> s = termDocMatrix.get(fileName);
        String[] documentTerms = s.toArray(new String[s.size()]);

        for(int i = 0; i < numPermutations(); i++){

            if(documentTerms.length == 0){
                minHashSig[i] = numTerms();
                continue;
            }// end if no terms in the document

            // find the minimum for each permutation
            int min = permutations[i].hash(documentTerms[0]);

            for(int j = 1; j < documentTerms.length; j++){
                int newVal = permutations[i].hash(documentTerms[j]);
                if(newVal < min){
                    min = newVal;
                }// end if new minimum value
            }// end for loop over all terms in the document

            minHashSig[i] = min;
        }// end for loop over all permutations

        return minHashSig;
    }// end function minHashSig

    /**
     * Returns the MinHash the minhash signature of the document
     * named fileName, which is an array of ints.
     * @param file1 String
     * @param file2 String
     * @return double
     */
    public double approximateJaccard(String file1, String file2) {
        int[] signature1 = minHashMatrix[documentIndex.get(file1)];
        int[] signature2 = minHashMatrix[documentIndex.get(file2)];

        int matches = 0;
        for(int i = 0; i < numPermutations(); i++){
            if(signature1[i] == signature2[i]){
                matches++;
            }// end if components in minHashSignature match
        }// end for loop over number of permutations

        return (double) matches / numPermutations();
    }// end function approximateJaccard

    /**
     * Returns the MinHash Matrix of the collection.
     * @return int[][]
     */
    public int[][] minHashMatrix() {
        return minHashMatrix;
    }// end function minHashMatrix

    /**
     * Returns the number of terms in the document collection.
     * @return int
     */
    public int numTerms() {
        return numTerms;
    }// end function numTerms

    /**
     * Returns the number of permutations used to construct the MinHash matrix.
     * @return
     */
    public int numPermutations() {
        return permutations.length;
    }// end function numPermutations

    /**
     * Returns set of terms in a document after cleaning up the file
     * and removing stop words
     * @return HashSet<String>
     */
    private HashSet<String> collectTerms(String document){
        HashSet<String> documentTerms = new HashSet<>();
        Scanner lineScanner = null;

        try {
            lineScanner = new Scanner(new FileInputStream(document), "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }// end try-catch FileNotFound

        while (lineScanner.hasNextLine()) {
            Scanner wordScanner = new Scanner(lineScanner.nextLine());

            while (wordScanner.hasNext()) {
                String term = clean(wordScanner.next());

                if(term.length() >= 3 && !term.equals("the")){
                    documentTerms.add(term);
                }// end if this is a relevant term

            }// end while there are still words in the line

        }// end while there are still lines in the file
        return documentTerms;
    }// end function collectTerms

    /**
     * Clean a string before adding it to term collection
     * @return String
     */
    private String clean(String term){
        // clean a string to prepare it for comparison
        String[] punctuation = {".", ",", ":", ";", "'"};
        String cleaned = term.toLowerCase();

        for(String p : punctuation){
            cleaned = cleaned.replace(p, "");
        }// end foreach over punctuation chars

        return cleaned;
    }// end function clean

    /**
     * Example run test case
     * @param args String[]
     */
    public static void main(String[] args)
    {
        //String base_dir = System.getProperty("user.dir") + "\\project2\\F17PA2\\";
        String base_dir = System.getProperty("user.dir") + "/project2/F17PA2/";
        MinHash m = new MinHash(base_dir, 400);
        String file1 = base_dir + "baseball0.txt";
        String file2 = base_dir + "baseball0.txt.copy1";
        System.out.println("Exact Jaccard: " + m.exactJaccard(file1, file2));
        System.out.println("Approx Jaccard: " + m.approximateJaccard(file1, file2));
    }// end main test function

}// end class MinHash
