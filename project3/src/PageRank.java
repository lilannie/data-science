public class PageRank {
    String fileName = "";
    double approximation = 0.0;
    int numEdges = 0;

    public PageRank(String fileName, double approximation) {
        this.fileName = fileName;
        this.approximation = approximation;
    }

    public double pageRankOf(String vertex) {

        return 0.0;
    }

    public double outDegreeOf(String vertex) {

        return 0.0;
    }

    public double inDegreeOf(String vertex) {

        return 0.0;
    }

    public int numEdges() {
        return this.numEdges;
    }

    public String[] topKPageRank(int k) {

        return new String[0];
    }

    public String[] topKInDegree(int k) {

        return new String[0];
    }

    public String[] topKOutDegree(int k) {

        return new String[0];
    }
}
