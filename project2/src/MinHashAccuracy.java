public class MinHashAccuracy {
    /**
     * This method will create an instance of MinHash.
     * For every pair of les in the document collection, compute exact Jaccard Similarity and
     * approximate Jaccard similarity (obtained by calling methods exactJaccard and
     * approximateJaccard from the class MinHash).
     *
     * Reports the number of pairs for which exact and approximate similarities
     * differ by more then errorParam
     * @param folder
     * @param errorParam
     * @return
     */
    public int accuracy(String folder, int numPermutations, double errorParam) {
        int numError = 0;
        MinHash m = new MinHash(folder, numPermutations);
        String[] documents = m.allDocs();

        for(String documentA : documents){

            for(String documentB : documents){

                if(!documentA.equals(documentB)){
                    double exact = m.exactJaccard(documentA, documentB);
                    double approximate = m.approximateJaccard(documentA, documentB);

                    if(Math.abs(exact - approximate) > errorParam){
                        numError++;
                    }// end if the difference is greater than the error parameter

                }// end if the two documents differ

            }// end inner document for loop

        }// end outer document for loop

        return numError;
    }// end function accuracy

    public static void main(String[] args)
    {
        String base_dir = System.getProperty("user.dir") + "\\project2\\space\\";
        MinHashAccuracy m = new MinHashAccuracy();
        System.out.println(m.accuracy(base_dir, 200, 0.04));
    }// end main test function

}// end class MinHashAccuracy
