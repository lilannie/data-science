import java.io.IOException;

/**
 * Write a program named MyWikiRanker. For this, pick a set of words representing a topic of
 * your choice. Choose an appropriate seed url and form a graph over 100 vertices. Compute the page
 * rank each page in your graph, and output top page rank vertices.
 */
public class MyWikiRanker {

    static final int top = 10;
    static final int maxVertices = 100;
    static final boolean isWeighted = true;
    static final double approx = 0.01;

    public static void main(String[] args) throws IOException, InterruptedException {
        // initialize MyWikiRanker
        String[] keywords = { "beach", "swimming" };
        String fileName = "wikiCustom.txt";
        String seedUrl = "/wiki/Beach";

        // crawl our seed url
        WikiCrawler w = new WikiCrawler(seedUrl, keywords, maxVertices, fileName, isWeighted);
        w.crawl();

        // rank the pages and print out the top
        PageRank p = new PageRank(fileName, approx);
        p.prettyPrint(p.topKPageRank(top));
    }// end main function

}// end class MyWikiRanker
