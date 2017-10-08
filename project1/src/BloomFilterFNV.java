public class BloomFilterFNV implements BloomFilter{
    int filterSize = 0;
    int dataSize = 0;
    int numHashes = 0;

    public BloomFilterFNV(int setSize, int bitsPerElement) {
        this.filterSize = setSize * bitsPerElement;
    }

    public void add(String s) {

        dataSize++;
    }

    public boolean appears(String s) {

        return false;
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
