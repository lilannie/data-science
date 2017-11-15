public class PageRank {
    String fileName = "";
    double approximation = 0.0;
    int numEdges = 0;

    /**
     *
     * @param fileName Name of a file that contains the edges of the graph. You may assume that the first line of
     *                 this graph lists the number of vertices, and every line (except first) lists one edge. You may
     *                 assume that each vertex is represented as string, and every edge of the graph appears exactly
     *                 once and the graph has no self loops.
     * @param approximation approximation parameter for pagerank.
     */
    public PageRank(String fileName, double approximation) {
        this.fileName = fileName;
        this.approximation = approximation;
    }

    /**
     *
     * @param vertex name of vertex of the graph
     * @return its page rank
     */
    public double pageRankOf(String vertex) {

        return 0.0;
    }

    /**
     *
     * @param vertex name of vertex of the graph
     * @return its out degree
     */
    public double outDegreeOf(String vertex) {

        return 0.0;
    }

    /**
     *
     * @param vertex name of vertex of the graph
     * @return its in degree
     */
    public double inDegreeOf(String vertex) {

        return 0.0;
    }

    /**
     *
     * @return number of edges of the graph.
     */
    public int numEdges() {
        return this.numEdges;
    }

    /**
     *
     * @param k parameter
     * @return an array (of strings) of pages with top k page ranks.
     */
    public String[] topKPageRank(int k) {

        return new String[0];
    }

    /**
     *
     * @param k parameter
     * @return returns an array (of strings) of pages with top k in degree.
     */
    public String[] topKInDegree(int k) {

        return new String[0];
    }

    /**
     *
     * @param k parameter
     * @return returns an array (of strings) of pages with top k out degree.
     */
    public String[] topKOutDegree(int k) {

        return new String[0];
    }
}
