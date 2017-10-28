import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class MinHash {

    private String[] documents;
    private int numTerms;
    private int numPermutations;
    private HashMap termDocMatrix;

    /**
     * folder is the name of a folder containing our
     * document collection for which we wish to construct MinHash matrix. numPermutations denotes
     * the number of permutations to be used in creating the MinHash matrix.
     * @param folder
     * @param numPermutations
     */
    public MinHash(String folder, int numPermutations) {
        this.numPermutations = numPermutations;
        File[] files = new File(folder).listFiles();
        documents = new String[files.length];
        termDocMatrix = new HashMap<String, HashSet<String>>();
        HashSet allTerms = new HashSet<String>();

        for(int i = 0; i < documents.length; i++) {
            // collect all terms and place them in a term-document Hashmap
            documents[i] = files[i].getAbsolutePath();
            HashSet documentTerms = collectTerms(documents[i]);
            allTerms.addAll(documentTerms);
            termDocMatrix.put(documents[i], documentTerms);
        }// end for loop over all documents

        numTerms = allTerms.size();
        System.out.println(numTerms);
    }

    /**
     * Returns an array of String consisting of all the names of
     * files in the document collection.
     * @return
     */
    public String[] allDocs() {
        return documents;
    }

    /**
     * Get names of two les (in the document collection) file1 and file2 as
     * parameters and returns the exact Jaccard Similarity of the les.
     * @param file1
     * @param file2
     * @return
     */
    public double exactJaccard(String file1, String file2) {
        return 0.0;
    }

    /**
     * Returns the MinHash the minhash signature of the document
     * named fileName, which is an array of ints.
     * @param fileName
     * @return
     */
    int minHashSig(String fileName) {
        return 0;
    }

    /**
     * Returns the MinHash the minhash signature of the document
     * named fileName, which is an array of ints.
     * @param file1
     * @param file2
     * @return
     */
    public double approximateJaccard(String file1, String file2) {
        return 0.0;
    }

    /**
     * Estimates and returns the Jaccard similarity of documents file1 and
     * file2 by comparing the MinHash signatures of file1 and file2.
     * @return
     */
    public int[][] minHashMatrix() {
        return new int[0][0];
    }

    /**
     * Returns the number of terms in the document collection.
     * @return
     */
    public int numTerms() {
        return numTerms;
    }

    /**
     * Returns the number of permutations used to construct the MinHash matrix.
     * @return
     */
    public int numPermutations() {
        return 0;
    }

    public static void main(String[] args){
        MinHash m = new MinHash("project2/articles", 10);
    }

    private HashSet<String> collectTerms(String document){
        HashSet<String> documentTerms = new HashSet<>();
        Scanner lineScanner = null;

        try {
            lineScanner = new Scanner(new File(document));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }// end try-catch FileNotFound

        while (lineScanner.hasNextLine()) {
            Scanner wordScanner = new Scanner(lineScanner.nextLine());

            while (wordScanner.hasNext()) {
                String term = clean(wordScanner.next());

                if(term.length() >= 3 && term != "the"){
                    documentTerms.add(term);
                }// end if this is a relevant term

            }// end while there are still words in the line

        }// end while there are still lines in the file
        return documentTerms;
    }// end function collectTerms

    private String clean(String term){
        // clean a string to prepare it for comparison
        String[] punctuation = {".", ",", ":", ";", "'"};
        String cleaned = term.toLowerCase();

        for(String p : punctuation){
            cleaned = cleaned.replace(p, "");
        }// end foreach over punctuation chars

        return cleaned;
    }// end function clean

}// end class MinHash
