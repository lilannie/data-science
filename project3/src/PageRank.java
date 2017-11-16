import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * This class will have methods to compute page rank of nodes/pages of a web graph. This class
 * should have following methods and constructors.
 *
 * @author Annie Steenson
 */
public class PageRank {
    private final double basis = 0.85;

    private double approximation = 0.0;
    // Each vertex represents a page
    private int numVertices = -1;
    // Each edge represents a link from one page to another page
    private int numEdges = 0;

    // Adjacency list to represent Graph
    private HashMap<String, HashSet<String>> graph = new HashMap<>();
    // String - Vertex, Integer - inDegree
    private HashMap<String, Integer> inDegree = new HashMap<>();

    // Store previous step's pageRank vector
    private HashMap<String, Double> pageRank = null;
    // Store next step's pageRank vector
    private HashMap<String, Double> nextStepPageRank = new HashMap<>();
    // Store default next step's pageRank vector
    private HashMap<String, Double> defaultNextStepPageRank = new HashMap<>();

    // Used for returning topKPageRank
    private PriorityQueue<Map.Entry<String, Double>> pageRankSorted = null;
    // Used for returning topKOutDegree
    private PriorityQueue<Map.Entry<String, HashSet<String>>> outDegreeSorted = null;
    // Used for returning topKInDegree
    private PriorityQueue<Map.Entry<String, Integer>> inDegreeSorted = null;

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

        initializePriorityQueues();
        initializeGraph(fileName);
        computePageRankVector();
    }

    /**
     * This helper method initializes the priority queues with each's necessary Comparator.
     * I put this code into a function rather than in the class itself to keep the code readable.
     */
    private void initializePriorityQueues() {
        pageRankSorted = new PriorityQueue<>(new Comparator<Map.Entry<String, Double>>() {
            @Override
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                if (o1.getValue() - o2.getValue() > 0) return 1;
                else if (o1.getValue() - o2.getValue() < 0) return -1;
                return 0;
            }
        });

        outDegreeSorted = new PriorityQueue<>(new Comparator<Map.Entry<String, HashSet<String>>>() {
            @Override
            public int compare(Map.Entry<String, HashSet<String>> o1, Map.Entry<String, HashSet<String>> o2) {
                if (o1.getValue().size() - o2.getValue().size() > 0) return 1;
                else if (o1.getValue().size() - o2.getValue().size() < 0) return -1;
                return 0;
            }
        });

        inDegreeSorted = new PriorityQueue<>(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                if (o1.getValue() - o2.getValue() > 0) return 1;
                else if (o1.getValue() - o2.getValue() < 0) return -1;
                return 0;
            }
        });
    }

    /**
     * This helper method reads the file from fileName.
     * It initializes the adjacency list with the edges from fileName.
     * It also initializes the HashMaps used in the pageRank algorithm.
     *
     * @param fileName String with address to file with edges in WebGraph
     */
    private void initializeGraph(String fileName) {
        System.out.println("initializeGraph");
        BufferedReader br = null;
        FileReader fr = null;

        try {
            fr = new FileReader(fileName);
            br = new BufferedReader(fr);
            String line;

            while ((line = br.readLine()) != null) {
                if (this.numVertices < 0) {
                    this.numVertices = Integer.parseInt(line);

                    continue;
                }

                String[] vertices = line.split(" ");
                addEdge(vertices[0], vertices[1]);
                addVertex(vertices[0]);
                addVertex(vertices[1]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method adds an edge to the adjacency list.
     * It also increments the class's numEdges instance variable.
     *
     * @param from String - vector name
     * @param to String - vector name
     */
    private void addEdge(String from, String to) {
        if (!graph.containsKey(from)) {
            graph.put(from, new HashSet<>());
        }

        graph.get(from).add(to);

        // Count in degree
        int inDegreeCount = 1;
        if (inDegree.containsKey(to)) {
            inDegreeCount += inDegree.get(to);
        }
        inDegree.put(to, inDegreeCount);

        this.numEdges++;
    }

    /**
     * This method adds a vertex to the page rank vectors
     * @param vertex String - name of vertex
     */
    private void addVertex(String vertex) {
        if (!defaultNextStepPageRank.containsKey(vertex)) {
            // Populate page rank vectors
            nextStepPageRank.put(vertex, 1.0/this.numVertices);
            defaultNextStepPageRank.put(vertex, (1.0 - this.basis)/this.numVertices);
        }
    }

    /**
     * This method computes the final page rank vector.
     * The final page rank vector will be in the instance variable 'pagerank' when this method completes.
     */
    private void computePageRankVector() {
        System.out.println("computePageRankVector");
        while (!isConverged()) {
            computeNextStepPageRank();
        }
    }

    /**
     * This method is a sub routine of computerPageRankVector().
     * This method computes the next step page rank, Pn+1()
     * Based on the algorithm from the "Pagerank" notes provided by the professor.
     */
    private void computeNextStepPageRank() {
        pageRank = nextStepPageRank;
        nextStepPageRank = (HashMap<String, Double>) defaultNextStepPageRank.clone();

        Set<String> pages = pageRank.keySet();
        // For every page p of the graph G
        for (String p: pages) {
            double previousRank = pageRank.get(p);  // Get Pn(p)
            HashSet<String> linksInPage = graph.get(p);
            if(linksInPage == null)
                continue;

            switch (linksInPage.size()) {
                // if Number of links in p is equals zero then
                case 0: {
                    double anchor = this.basis * (previousRank / this.numVertices);

                    // TODO do we apply the anchor to page p's nextPageRank as well?
                    // For every page q in the graph G set
                    for (String q: pages) {
                        double pageRank = nextStepPageRank.get(q);
                        nextStepPageRank.put(q, pageRank + anchor);
                    }

                    break;
                }

                // if Number of links in p is not equal to zero then
                default: {
                    double anchor = this.basis * (previousRank / linksInPage.size());

                    // TODO do we apply the anchor to page p's nextPageRank as well?
                    // Set Q be the set of all pages that are linked from p
                    // For every page q 2 Q set
                    for (String q: linksInPage) {
                        double pageRank = nextStepPageRank.get(q);
                        nextStepPageRank.put(q, pageRank + anchor);
                    }

                    break;
                }
            }
        }
    }

    /**
     * This method checks if the average difference of
     * the previous pageRank and the nextStepPageRank are <= approximation threshold.
     * @return boolean
     */
    private boolean isConverged() { // TODO check if this is correct
        if (pageRank == null) return false;

//        double dotProduct = 0;
//        double pageRank_L2 = 0;
//        double nextStepPageRank_L2 = 0;

        double euclidean_dis = 0.0;

        for (String page: pageRank.keySet()) {
            double pageRank_val = pageRank.get(page);
            double nextStepPageRank_val = nextStepPageRank.get(page);

            euclidean_dis += Math.pow((nextStepPageRank_val - pageRank_val), 2); // TODO check pow function
//            dotProduct += nextStepPageRank.get(page) * pageRank_val;
//            pageRank_L2 += Math.pow(pageRank.get(page), 2);
//            pageRank_L2 += Math.pow(nextStepPageRank.get(page), 2);
        }

        return Math.sqrt(euclidean_dis) <= approximation;
    }

    /**
     * @param vertex name of vertex of the graph
     * @return its page rank
     */
    public double pageRankOf(String vertex) {
        return pageRank.get(vertex);
    }

    /**
     * @param vertex name of vertex of the graph
     * @return int - its out degree
     */
    public int outDegreeOf(String vertex) {
        return graph.get(vertex).size();
    }

    /**
     * @param vertex name of vertex of the graph
     * @return int - its in degree
     */
    public int inDegreeOf(String vertex) {
        return inDegree.get(vertex);
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
        String[] topK = new String[k];

        pageRankSorted.addAll(pageRank.entrySet());

        for (int i = 0; i < k; i++) {
            Map.Entry<String, Double> entry = pageRankSorted.poll();
            topK[i] = entry.getKey();
        }

        return topK;
    }

    /**
     * @param k parameter
     * @return returns an array (of strings) of pages with top k in degree.
     */
    public String[] topKInDegree(int k) {
        String[] topK = new String[k];

        inDegreeSorted.addAll(inDegree.entrySet());

        for (int i = 0; i < k; i++) {
            Map.Entry<String, Integer> entry = inDegreeSorted.poll();
            topK[i] = entry.getKey();
        }

        return topK;
    }

    /**
     * @param k parameter
     * @return returns an array (of strings) of pages with top k out degree.
     */
    public String[] topKOutDegree(int k) {
        String[] topK = new String[k];

        outDegreeSorted.addAll(graph.entrySet());

        for (int i = 0; i < k; i++) {
            Map.Entry<String, HashSet<String>> entry = outDegreeSorted.poll();
            topK[i] = entry.getKey();
        }

        return topK;
    }

    /**
     * This method prints out a string array formatted nicely
     * @param arr String[] - String array to be printed
     */
    public void prettyPrint(String[] arr) {
        for(int i = 0; i < arr.length; i++) {
            System.out.println((i+1) + ": " + arr[i]);
        }
        System.out.println();
    }// end function prettyPrint

    public static void main(String[] args) {
        PageRank pr = new PageRank("WikiTennisGraph.txt", 0.0);
        System.out.println(pr.numEdges());

        String[] topK = pr.topKPageRank(10);
        System.out.println(topK);
        System.out.println(pr.topKInDegree(10));
        System.out.println(pr.topKOutDegree(10));

        String topPage = topK[0];
        System.out.println(pr.pageRankOf(topPage));
        System.out.println(pr.inDegreeOf(topPage));
        System.out.println(pr.outDegreeOf(topPage));

    }
}
