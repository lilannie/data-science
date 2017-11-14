import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

public class WeightedQueue<T>
{
    int maxWeight = 0;      // max weight of all items in the queue
    Queue<T> queue;

    public WeightedQueue(){
        // traverse the web graph starting at seed_url
        queue = new PriorityQueue<>();
    }// end constructor WeightedQueue

    public void add(Tuple t){
        if(t.weight > maxWeight)
    }// end function add

    public T extract(){

    }// end function extract

    class Tuple {
        T item;
        int weight;

        public Tuple(T item, int weight) {
            this.item = item;
            this.weight = weight;
        }// end constructor for Tuple

        @Override
        public boolean equals(Object o){
            Tuple t = (Tuple) o;
            return this.item.equals(t.item) && this.weight == t.weight;
        }// end function equals()

        @Override
        public String toString(){
            return "(" + item + "," + weight + ")";
        }// end function toString()

    }// end class Tuple

}// end class WeightedQueue




