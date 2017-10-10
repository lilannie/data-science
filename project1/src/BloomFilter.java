import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;

public abstract class BloomFilter {
    int filterSize = 0;
    int dataSize = 0;
    int numHashes = 0;
    int bitsPerElement = 0;
    int setSize = 0;
    LinkedList<HashFunction> functions = new LinkedList<>();
    LinkedList<BitSet[]> filters = new LinkedList<>();

    public void add(String s) {
        Iterator<BitSet[]> i = filters.iterator();
        for (HashFunction f: this.functions) {
            BitSet[] filter = i.next();
            int hash = f.hash(s.toLowerCase());
            BitSet b = new BitSet();
            b.set(0);
            filter[hash] = b;
        }
        dataSize++;
    }

    public boolean appears(String s) {
        Iterator<BitSet[]> i = filters.iterator();

        for (HashFunction f: this.functions) {
            BitSet[] filter = i.next();
            int hash = f.hash(s.toLowerCase());
            BitSet b = filter[hash];
            if (!b.get(0))
                return false;
        }

        return true;
    }

    public int filerSize() {
        return this.filterSize;
    }

    public int dataSize() {
        return this.dataSize;
    }

    public int numHashes() {
        return this.numHashes;
    }
}
