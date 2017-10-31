import java.util.ArrayList;
import java.util.HashMap;

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
        ArrayList<Pair> pairs = new ArrayList<>();

        int pairCount = 0;
        for(int i = 0; i < documents.length; i++){

            for(int j = 0; j < documents.length; j++){
                Pair p = new Pair(documents[i], documents[j]);

                if(!pairs.contains(p) && !documents[i].equals(documents[j])){
                    double exact = m.exactJaccard(documents[i], documents[j]);
                    double approximate = m.approximateJaccard(documents[i], documents[j]);

                    if(Math.abs(exact - approximate) > errorParam){
                        numError++;
                    }// end if the difference is greater than the error parameter

                    pairs.add(p);
                    System.out.println(pairCount++);
                }// end if we havent seen this pair yet
            }// end for loop over all documents
        }// end for loop over all documents

        return numError;
    }// end function accuracy

    public static void main(String[] args)
    {
        String base_dir = System.getProperty("user.dir") + "\\project2\\space\\";
        MinHashAccuracy m = new MinHashAccuracy();
        System.out.println(m.accuracy(base_dir, 400, 0.04));
       // System.out.println(m.accuracy(base_dir, 800, 0.07));
       // System.out.println(m.accuracy(base_dir, 800, 0.09));
    }// end main test function

    private class Pair {
        private final String left;
        private final String right;

        public Pair(String left, String right) {
            this.left = left;
            this.right = right;
        }// end pair constructor

        public String getLeft() { return left; }
        public String getRight() { return right; }

        @Override
        public int hashCode() { return left.hashCode() ^ right.hashCode(); }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Pair)) return false;
            Pair pairo = (Pair) o;
            return (this.left.equals(pairo.getLeft()) && this.right.equals(pairo.getRight())) ||
                    (this.left.equals(pairo.getRight()) && this.right.equals(pairo.getLeft())) ;
        }// end boolean equals

    }// end class pair

}// end class MinHashAccuracy
