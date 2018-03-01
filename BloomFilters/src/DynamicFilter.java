import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

/**
 * @author Annie Steenson
 */
public class DynamicFilter extends BloomFilter {
    LinkedList<BloomFilter> filters = new LinkedList<>();

    /**
     * Instantiates a DynamicFilter
     * The size starts at 1000
     * @param bitsPerElement int
     */
    public DynamicFilter(int bitsPerElement) {
        this.bitsPerElement = bitsPerElement;
        filters.add(new BloomFilterRan(1000, bitsPerElement));
    }

    /**
     * Adds a String element to the filter
     * When the the bloom filter is full it adds another filter with twice the size of the last filled filter
     * @param s String
     */
    @Override
    public void add(String s) {
        BloomFilter filter = filters.getLast();

        if (filter.dataSize >= filter.filterSize) {
            filter = new BloomFilterRan(filter.filterSize*2, bitsPerElement);
            filters.add(filter);
        }

        filter.add(s);
    }

    /**
     * Returns true if a hash value of the String s is set to true in the filter --> could be a false positive
     * Returns false if the String s is not in any of the filters
     * @param s String
     * @return boolean
     */
    @Override
    public boolean appears(String s) {
        for (BloomFilter f: filters) {
            if (f.appears(s)) {
                return true;
            }
        }
        return false;
    }
}
