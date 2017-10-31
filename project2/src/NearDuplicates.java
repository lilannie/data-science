import java.util.ArrayList;

/**
 *  class puts together MinHash and LSH to detect near duplicates in a document collection
 */
public class NearDuplicates {

    static final double DIFFERENCE_THRESHOLD = 0.02;
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
    public ArrayList<String> nearDuplicateDetector(String folder, int numPermutations, double s, String docName) {
        MinHash m = new MinHash(folder, numPermutations);
        double optimalBands = 0;
        double optimalChars;

        do {
            optimalBands++;
            optimalChars = numPermutations / optimalBands;
        }
        while(!isClose(Math.pow(1/optimalBands, 1/optimalChars), s));

        System.out.println("Optimal Bands: " + optimalBands);
        System.out.println("Optimal value: " + Math.pow(1/optimalBands, 1/optimalChars));

        LSH l = new LSH(m.minHashMatrix(), m.allDocs(), (int) optimalBands);
        return l.nearDuplicatesOf(docName);
    }// end function nearDuplicateDetector

    public static void main(String[] args)
    {
       //String base_dir = System.getProperty("user.dir") + "\\project2\\space\\";
        String base_dir = System.getProperty("user.dir") + "/project2/space/";

        NearDuplicates n = new NearDuplicates();
        String file = base_dir + "space-0.txt";
        ArrayList<String> nearDuplicates = n.nearDuplicateDetector(base_dir,200, 0.5, file);
        System.out.println(nearDuplicates);
        System.out.println(nearDuplicates.size());
    }// end main test function

    private boolean isClose(double a, double b){
        return Math.abs(a-b) <= DIFFERENCE_THRESHOLD;
    }// end function isClose()

}// end class NearDuplicates
