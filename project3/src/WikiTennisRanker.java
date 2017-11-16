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

	static final double approx = 0.01;
	static final int top = 10;

    public static void main(String[] args) {
    	//PageRank p = new PageRank("wikiTennis.txt", approx);
    	PageRank p = new PageRank("project3/wikiTennis.txt", approx);
    	
    	System.out.println("Highest page rank:");
    	String[] topPageRank = p.topKPageRank(top);
    	p.prettyPrint(topPageRank);
    	
    	System.out.println("Highest in-degree:");
    	String[] topInDegree = p.topKInDegree(top);
    	p.prettyPrint(topInDegree);
    	
    	System.out.println("Highest out-degree:");
    	String[] topOutDegree = p.topKOutDegree(top);
    	p.prettyPrint(topOutDegree);
    	
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

}// end class WikiTennisRanker
