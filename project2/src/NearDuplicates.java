import java.util.ArrayList;

/**
 *  class puts together MinHash and LSH to detect near duplicates in a document collection
 */
public class NearDuplicates {
    /**
     *
     * @param folder name of the the folder containing documents
     * @param numPermutations Number of Permutations to be used for MinHash
     * @param s Similarity threshold
     * @param docName Name of a document from the collection
     * @return computes the list of documents that are more than s-similar to docName,
     *          by calling the method nearDuplicates. Note that this list may contain some False Positives|
     *          Documents that are less than s-similar to docName.
     */
    public ArrayList<String> nearDuplicateDetector(String folder, int numPermutations, int s, String docName) {
        ArrayList<String> nearDuplicates = new ArrayList<>();
        return nearDuplicates;
    }
}
