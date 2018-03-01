@SuppressWarnings("unchecked")
public class Tuple<T> implements Comparable<Tuple<T>> 
{
	T item;
	double weight;
	long timestamp;
	
	public Tuple(T item, double weight, int timestamp) {
	    this.item = item;
	    this.weight = weight;
	    this.timestamp = timestamp;
	}// end constructor for Tuple
	
	public Tuple(T item) {
		this(item, 0, 0);
	}// end constructor for Tuple
	
	@Override
	public boolean equals(Object obj){
	    Tuple<T> t = (Tuple<T>) obj;
	    return this.item.equals(t.item);
	}// end function equals()
	
	@Override
	public String toString(){
	    return "<" + item + "," + (int) weight + ">";
	}// end function toString()
	
	@Override
	public int compareTo(Tuple<T> t) {
		if (t.weight == this.weight) {
			return (int) (this.timestamp - t.timestamp);
		} else {
			return (int) (t.weight - this.weight);
		}// end if these two tuples have equal weights
	}// end function compareTo

}// end class Tuple