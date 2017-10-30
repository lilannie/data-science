import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

public class MinHash {

    private HashMap<String, HashSet<String>> termDocMatrix;
    private HashFunctionRan[] permutations;
    private int numTerms;
    private String[] documents;
    private int[][] minHashMatrix;

    /**
     * folder is the name of a folder containing our
     * document collection for which we wish to construct MinHash matrix. numPermutations denotes
     * the number of permutations to be used in creating the MinHash matrix.
     * @param folder
     * @param numPermutations
     */
    public MinHash(String folder, int numPermutations) {
        File[] files = new File(folder).listFiles();
        termDocMatrix = new HashMap<>();
        permutations = new HashFunctionRan[numPermutations];
        HashSet terms = new HashSet<String>();
        documents = new String[files.length];

        for(int i = 0; i < files.length; i++) {
            // collect all terms and place them in a term-document Hashmap
            String document = files[i].getAbsolutePath();
            HashSet documentTerms = collectTerms(document);
            terms.addAll(documentTerms);
            termDocMatrix.put(document, documentTerms);
            documents[i] = document;
        }// end for loop over all documents

        numTerms = terms.size();
        for(int i = 0; i < numPermutations; i++){
            permutations[i] = new HashFunctionRan(numTerms);
        }// end for loop creating permutation functions

        minHashMatrix = new int[documents.length][numPermutations()];

        for(int i = 0; i < documents.length; i++){
            minHashMatrix[i] = minHashSig(documents[i]);
        }// end for loop creating our minHash matrix
    }// end MinHash constructor

    /**
     * Returns an array of String consisting of all the names of
     * files in the document collection.
     * @return
     */
    public String[] allDocs() {
        return documents;
    }// end function allDocs

    /**
     * Get names of two les (in the document collection) file1 and file2 as
     * parameters and returns the exact Jaccard Similarity of the les.
     * @param file1
     * @param file2
     * @return
     */
    public double exactJaccard(String file1, String file2) {
        HashSet<String> terms1 = termDocMatrix.get(file1);
        HashSet<String> terms2 = termDocMatrix.get(file2);

        // intersection between file1 and file2 terms
        terms1.retainAll(terms2);

        // union of file1 and file2 terms
        terms2.addAll(terms1);

        return (double) terms1.size() / terms2.size();
    }// end function exactJaccard

    /**
     * Returns the MinHash the minhash signature of the document
     * named fileName, which is an array of ints.
     * @param fileName
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
     * @param file1
     * @param file2
     * @return
     */
    public double approximateJaccard(String file1, String file2) {
        int[] signature1 = minHashMatrix[getIndex(file1)];
        int[] signature2 = minHashMatrix[getIndex(file2)];

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
     * @return
     */
    public int[][] minHashMatrix() {
        return minHashMatrix;
    }// end function minHashMatrix

    /**
     * Returns the number of terms in the document collection.
     * @return
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
     * @return
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
     * @return
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

    private int getIndex(String file){
        for(int i = 0; i < documents.length; i++){
            if(documents[i].equals(file)){
                return i;
            }// end if string equals file we're looking for
        }// end for loop over list
        return -1;
    }// end function findIndex

    public static void main(String[] args)
    {
        String base_dir = System.getProperty("user.dir") + "\\project2\\F17PA2\\";
        MinHash m = new MinHash(base_dir, 500);
        String file1 = base_dir + "baseball0.txt";
        String file2 = base_dir + "baseball0.txt.copy1";
        System.out.println("Exact Jaccard: " + m.exactJaccard(file1, file2));
        System.out.println("Approx Jaccard: " + m.approximateJaccard(file1, file2));
    }// end main test function

}// end class MinHash
