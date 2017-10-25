public class MinHash {
    /**
     * folder is the name of a folder containing our
     * document collection for which we wish to construct MinHash matrix. numPermutations denotes
     * the number of permutations to be used in creating the MinHash matrix.
     * @param folder
     * @param numPermutations
     */
    MinHash(String folder, int numPermutations) {

    }

    /**
     * Returns an array of String consisting of all the names of
     * files in the document collection.
     * @return
     */
    String[] allDocs() {
        return null;
    }

    /**
     * Get names of two les (in the document collection) file1 and file2 as
     * parameters and returns the exact Jaccard Similarity of the les.
     * @param file1
     * @param file2
     * @return
     */
    double exactJaccard(String file1, String file2) {
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
    double approximateJaccard(String file1, String file2) {
        return 0.0;
    }

    /**
     * Estimates and returns the Jaccard similarity of documents file1 and
     * file2 by comparing the MinHash signatures of file1 and file2.
     * @return
     */
    int[][] minHashMatrix() {
        return new int[][];
    }

    /**
     * Returns the number of terms in the document collection.
     * @return
     */
    int numTerms() {
        return 0;
    }

    /**
     * Returns the number of permutations used to construct the MinHash matrix.
     * @return
     */
    int numPermutations() {
        return 0;
    }
}
