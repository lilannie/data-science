public class Edge {
    // create an Edge of a directed graph (<start>, <end>) pair
    String start;
    String end;

    public Edge(String start, String end){
        this.start = start;
        this.end = end;
    } // end Edge constructor

    @Override
    public String toString(){
        return start + " " + end;
    }// end function toString()

    @Override
    public boolean equals(Object o){
        Edge e = (Edge) o;
        return this.start.equals(e.start) && this.end.equals(e.end);
    } // end function equals()

} // end class Edge