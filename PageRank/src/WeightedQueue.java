import java.util.HashMap;
import java.util.PriorityQueue;

@SuppressWarnings({"unchecked", "serial"})
public class WeightedQueue<T> extends PriorityQueue<T>
{
    HashMap<T, Integer> elementLookup;

    public WeightedQueue(){
    	super();
        elementLookup = new HashMap<>();
    }// end constructor WeightedQueue
    
    @Override
	public boolean add(T item){
    	Tuple<T> t = (Tuple<T>) item;
		
    	if(!elementLookup.containsKey(t.item)) {
			super.add((T) t);
			elementLookup.put(t.item, 1);
			return true;
		}// end if this item is not in the queue
		
		return false;
    }// end function add

	public T extract(){
    	Tuple<T> t = (Tuple<T>) super.poll();
    	elementLookup.remove(t.item);
    	return (T) t;
    }// end function extract
    
    public static void main(String[] args) {
    	WeightedQueue<Tuple<Integer>> q = new WeightedQueue<Tuple<Integer>>();
    	
    	// add items to the weighted queue
    	int counter = 0;
    	q.add(new Tuple<Integer>(1, 5, counter++));
    	q.add(new Tuple<Integer>(2, 3, counter++));
    	q.add(new Tuple<Integer>(5, 7, counter++));
    	q.add(new Tuple<Integer>(21, 5, counter++));
    	q.add(new Tuple<Integer>(36, 4, counter++));
    	
    	// extract items from weighted queue and print results
    	System.out.println(q.extract());
    	System.out.println(q.extract());
    	
    	// test adding a duplicate item
    	q.add(new Tuple<Integer>(21, 9, counter++));
    	
    	// extract items from weighted queue and print results
    	System.out.println(q.extract());
    	System.out.println(q.extract());
    	System.out.println(q.extract());
    }// end main test class

}// end class WeightedQueue




