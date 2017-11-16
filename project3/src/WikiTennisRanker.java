import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Look at the attached graph named wikiTennis.txt. Run your page rank algorithm on this
 * graph. Compute the page ranks with c = 0:01 as approximation factor. Output top 10 pages
 * with highest page rank, highest in-degree and highest out-degree. Compute the following
 * sets: Top 100 pages as per page rank, top 100 pages as per in-degree and top 100 pages as
 * per out degree. For each pair of the sets, compute Jaccard Similarity. Repeat the same with
 * c = 0:005 as approximation factor.
 */
public class WikiTennisRanker {

    public static void main(String[] args) {
    	double approx = 0.01;
    	int pages = 10;
    	
    	PageRank pr = new PageRank("wikiTennis.txt", approx);
    	
    	System.out.println("Highest page rank:");
    	String[] topPageRank = pr.topKPageRank(pages);
    	prettyPrint(topPageRank);
    	
    	System.out.println("Highest in-degree:");
    	String[] topInDegree = pr.topKInDegree(pages);
    	prettyPrint(topInDegree);
    	
    	System.out.println("Highest out-degree:");
    	String[] topOutDegree = pr.topKOutDegree(pages);
    	prettyPrint(topOutDegree);
    	
    	// print out the jaccard similarties between the lists
    	System.out.printf("Jaccard page rank vs. in-degree: %.2f\n", jaccard(topPageRank, topInDegree));
    	System.out.printf("Jaccard page rank vs. out-degree: %.2f\n", jaccard(topPageRank, topOutDegree));
    	System.out.printf("Jaccard in-degree vs. out-degree: %.2f\n", jaccard(topInDegree, topOutDegree));
    }// end function main
    
    /**
     * This method computes jaccard similarity between two sets
     * @param listA String[] - First list
     * @param listB String[] - Second list
     * @return double - Exact Jaccard similarity between the two lists
     */
    public static double jaccard(String[] listA, String[] listB) {
    	Set<String> setA = new HashSet<String>(Arrays.asList(listA));
    	Set<String> setB = new HashSet<String>(Arrays.asList(listB));
    	
    	Set<String> union = new HashSet<String>();
    	Set<String> intersect = setA;
    	
    	// compute union and intersection of both lists
    	intersect.retainAll(setB);
    	union.addAll(setA);
    	union.addAll(setB);
    	    	
    	return (double) intersect.size() / union.size();
    }// end function jaccard
    
    /**
     * This method prints out a string array formatted nicely
     * @param arr String[] - String array to be printed
     */
    public static void prettyPrint(String[] arr) {
    	for(int i = 0; i < arr.length; i++) {
    		System.out.println((i+1) + ": " + arr[i]);
    	}
    	System.out.println();
    }// end function prettyPrint
    
}// end class WikiTennisRanker
