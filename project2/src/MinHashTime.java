import java.util.HashMap;

public class MinHashTime {
    /**
     * Gets name of a folder, number of permutations to be used as parameters, and creates an
     * instance of MinHash.
     *
     * For every pair of les in the folder compute the exact Jaccard Similarity ; Report the time
     * taken (in seconds) for this task.
     *
     * Compute the MinHashMatrix and use this matrix to estimate Jaccard Similarity of every
     * pair of documents in the collection. Report the time taken for this task.
     * Break the time into two parts; time taken to compute MinHash matrix and
     * time taken to compute similarities between every pair.
     *
     * @param folder
     * @param numPermutations
     * @return
     */
    public void timer(String folder, int numPermutations)
    {
        long start = System.currentTimeMillis();
        MinHash m = new MinHash(folder, numPermutations);
        long end = System.currentTimeMillis();
        double secondsTaken = (double) (end - start) / 1000;
        System.out.printf("Time taken to compute minhash matrix: %.3f seconds\n", secondsTaken);

        String[] documents = m.allDocs();
        start = System.currentTimeMillis();
        for(int i = 0; i < documents.length; i++) {
            for (int j = i + 1; j < documents.length; j++) {
                m.exactJaccard(documents[i], documents[j]);
                m.approximateJaccard(documents[i], documents[j]);
            }// end for loop over all documents
        }// end for loop over documents

        end = System.currentTimeMillis();
        secondsTaken = (double) (end - start) / 1000;
        System.out.printf("Time taken to compute similarities between each pair: %.3f seconds\n", secondsTaken);
    }// end function timer

    public static void main(String[] args){
        MinHashTime t = new MinHashTime();
        //String base_dir = System.getProperty("user.dir") + "\\project2\\space\\";
        String base_dir = System.getProperty("user.dir") + "/project2/space/";
        t.timer(base_dir, 600);
    }// end main test function

}// end class MinHashTime
