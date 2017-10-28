import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class MinHash {

    private HashSet<String> terms;
    private HashMap<String, HashSet<String>> termDocMatrix;
    private int[][] minHashMatrix;
    private HashFunctionRan[] permutations;

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
        terms = new HashSet<>();
        permutations = new HashFunctionRan[numPermutations];

        for(int i = 0; i < files.length; i++) {
            // collect all terms and place them in a term-document Hashmap
            String document = files[i].getAbsolutePath();
            HashSet documentTerms = collectTerms(document);
            terms.addAll(documentTerms);
            termDocMatrix.put(document, documentTerms);
        }// end for loop over all documents

        for(int i = 0; i < numPermutations; i++){
            permutations[i] = new HashFunctionRan(numTerms());
        }// end for loop creating permutation functions

        String[] documents = allDocs();
        minHashMatrix = new int[documents.length][numPermutations];

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
        Set<String> documents = termDocMatrix.keySet();
        return documents.toArray(new String[documents.size()]);
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
        HashSet<String> union = new HashSet<>();
        union.addAll(terms1);
        union.addAll(terms2);

        return (double) terms1.size() / union.size();
    }// end function exactJaccard

    /**
     * Returns the MinHash the minhash signature of the document
     * named fileName, which is an array of ints.
     * @param fileName
     * @return
     */
    public int[] minHashSig(String fileName) {
        int[] minHashSig = new int[numPermutations()];
        HashSet<String> s = termDocMatrix.get(fileName);
        String[] documentTerms = s.toArray(new String[s.size()]);

        for(int i = 0; i < numPermutations(); i++){
            // find the minimum for each permutation
            if(documentTerms.length == 0){
                // we should't ever get here, but just in case
                minHashSig[i] = numTerms();
                continue;
            }// end if no terms in the document

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
        int[] signature1 = minHashSig(file1);
        int[] signature2 = minHashSig(file2);
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
        return terms.size();
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

                if(term.length() >= 3 && term != "the"){
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

    private class HashFunctionRan
    {
        // create a Random HashFunction for BloomFilterRan
        int a, b, p;

        public HashFunctionRan(int range)
        {
            this.p = getPrime(range);
            this.a = ThreadLocalRandom.current().nextInt(0, p);
            this.b = ThreadLocalRandom.current().nextInt(0, p);
        }// end constructor for HashFunction

        public int hash(String s)
        {
            return hash(s.hashCode());
        }// end function for hashing a string

        private int hash(int x)
        {
            return mod(a*x + b, p);
        }// end function for hashing an integer

        private int getPrime(int n)
        {
            // return the first positive prime of at least size n
            boolean found = false;

            while(!found){
                // while loop until we find a prime >= n
                if(isPrime(n)){
                    // found a prime
                    found = true;
                }else{
                    // did not find prime
                    if(n == 1 || n % 2 == 0){
                        n = n + 1;
                    }else{
                        n = n + 2;
                    }// end if we have an even number

                }// end if this is a prime

            }// end while we haven't found a prime

            return n;
        }// end function getPrime

        private boolean isPrime(int num)
        {
            if ( num > 2 && num % 2 == 0 ) {
                return false;
            }// end if number > 2 and even
            int top = (int) Math.sqrt(num) + 1;
            for(int i = 3; i < top; i+=2){
                if(num % i == 0){
                    return false;
                }// end if we found a divisor, not a prime
            }// end for loop checking if prime
            return true;
        }// end function isPrime

        public int mod(int x, int y)
        {
            int result = x % y;
            if (result < 0){
                result += y;
            }// end if result is negative
            return result;
        }// end function mod

    }// end class HashFunctionRan

    public static void main(String[] args)
    {
        String base_dir = System.getProperty("user.dir") + "\\project2\\articles\\";
        MinHash m = new MinHash(base_dir, 300);
        String file1 = base_dir + "baseball1.txt";
        String file2 = base_dir + "baseball2.txt";
        System.out.println("Exact Jaccard: " + m.exactJaccard(file1, file2));
        System.out.println("Approx Jaccard: " + m.approximateJaccard(file1, file2));
    }// end main test function

}// end class MinHash
