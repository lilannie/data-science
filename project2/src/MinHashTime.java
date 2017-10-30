import java.util.Arrays;

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
        MinHash m = new MinHash(folder, numPermutations);
        String[] documents = m.allDocs();

        long start = System.currentTimeMillis();
        for(String documentA : documents){

            for(String documentB : documents){

                if(!documentA.equals(documentB)){
                    m.exactJaccard(documentA, documentB);
                }// end if the two documents differ

            }// end inner document for loop

        }// end outer document for loop

        long end = System.currentTimeMillis();
        long secondsTaken = (end - start) * 1000;
        System.out.println("Time taken to compute similarities between each pair: " + secondsTaken);

        start = System.currentTimeMillis();
        m.minHashMatrix();
        end = System.currentTimeMillis();

        secondsTaken = (end - start) * 1000;
        System.out.println("Time taken to compute minhash matrix: " + secondsTaken);
    }// end function timer

    public static void main(String[] args){
        MinHashTime t = new MinHashTime();
        String base_dir = System.getProperty("user.dir") + "\\project2\\F17PA2\\";
        t.timer(base_dir, 500);
    }
}// end class MinHashTime
