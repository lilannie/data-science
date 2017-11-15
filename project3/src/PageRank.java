import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class PageRank {
    final double basis = 0.85;

    double approximation = 0.0;
    int numVertexes = -1;
    int numEdges = 0;

    HashMap<String, Integer> vertexIndices = new HashMap<>();
    HashMap<String, Integer> inDegree = new HashMap<>();
    HashMap<String, HashSet<String>> graph = new HashMap<>();

    /**
     * @param fileName Name of a file that contains the edges of the graph. You may assume that the first line of
     *                 this graph lists the number of vertices, and every line (except first) lists one edge. You may
     *                 assume that each vertex is represented as string, and every edge of the graph appears exactly
     *                 once and the graph has no self loops.
     *
     * @param approximation approximation parameter for pagerank.
     */
    public PageRank(String fileName, double approximation) {
        this.approximation = approximation;

        initializeGraph(fileName);
        computePageRankVector();
    }

    private void initializeGraph(String fileName) {
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(fileName);
            br = new BufferedReader(fr);
            String line;

            while ((line = br.readLine()) != null) {
                if (this.numVertexes < 0) {
                    this.numVertexes = Integer.parseInt(line);
                    continue;
                }

                String[] vertices = line.split(" ");
                addEdge(vertices[0], vertices[1]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addEdge(String from, String to) {
        if (!graph.containsKey(from)) {
            graph.put(from, new HashSet<>());
        }

        graph.get(from).add(to);

        int inDegreeCount = 1;
        if (inDegree.containsKey(to)) {
            inDegreeCount += inDegree.get(to);
        }
        inDegree.put(to, inDegreeCount);
    }

    private void computePageRankVector() {

    }

    /**
     * @param vertex name of vertex of the graph
     * @return its page rank
     */
    public double pageRankOf(String vertex) {

        return 0.0;
    }

    /**
     * @param vertex name of vertex of the graph
     * @return its out degree
     */
    public double outDegreeOf(String vertex) {

        return 0.0;
    }

    /**
     * @param vertex name of vertex of the graph
     * @return its in degree
     */
    public double inDegreeOf(String vertex) {

        return 0.0;
    }

    /**
     * @return number of edges of the graph.
     */
    public int numEdges() {
        return this.numEdges;
    }

    /**
     * @param k parameter
     * @return an array (of strings) of pages with top k page ranks.
     */
    public String[] topKPageRank(int k) {

        return new String[0];
    }

    /**
     * @param k parameter
     * @return returns an array (of strings) of pages with top k in degree.
     */
    public String[] topKInDegree(int k) {

        return new String[0];
    }

    /**
     * @param k parameter
     * @return returns an array (of strings) of pages with top k out degree.
     */
    public String[] topKOutDegree(int k) {

        return new String[0];
    }

    public static void main(String[] args) {
        PageRank pr = new PageRank("WikiTennisGraph.txt", 0.0);
    }
}
