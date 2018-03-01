import java.util.BitSet;
import java.util.LinkedList;

/**
 * @author Annie Steenson
 * This is an abstract class that holds all add and appears logic for a basic bloom filter
 * Each filter holds a LinkedList of hash functions.
 * The filter is stored as BitSet[]
 */
public abstract class BloomFilter {
    int filterSize = 0;
    int dataSize = 0;
    int numHashes = 0;

    int bitsPerElement = 0;
    int setSize = 0;

    LinkedList<HashFunction> functions = new LinkedList<>();
    BitSet[] filter = null;

    /**
     * Adds a String element to the filter
     * @param s String
     */
    public void add(String s) {
        for (HashFunction f: this.functions) {
            int hash = f.hash(s.toLowerCase());
            BitSet b = new BitSet();
            b.set(1);
            filter[hash] = b;
        }
        dataSize++;
    }

    /**
     * Returns true if a hash value of the String s is set to true in the filter --> could be a false positive
     * Returns false if the String s is not in the filter
     * @param s String
     * @return boolean
     */
    public boolean appears(String s) {
        for (HashFunction f: this.functions) {
            int hash = f.hash(s.toLowerCase());
            BitSet b = filter[hash];
            if (b == null || !b.get(1))
                return false;
        }

        return true;
    }

    /**
     * Returns the size of the filter
     * @return int
     */
    public int filerSize() {
        return this.filterSize;
    }

    /**
     * Returns the amount of data points added to the filter
     * @return int
     */
    public int dataSize() {
        return this.dataSize;
    }

    /**
     * Returns the number of hash functions used
     * @return int
     */
    public int numHashes() {
        return this.numHashes;
    }
}
