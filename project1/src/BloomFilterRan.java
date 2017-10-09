import java.util.BitSet;

public class BloomFilterRan extends BloomFilter {
    /**
     * @param setSize
     * @param bitsPerElement
     */
    public BloomFilterRan(int setSize, int bitsPerElement) {
        this.filterSize = setSize * bitsPerElement;
        this.bitsPerElement = bitsPerElement;

        this.numHashes = (int) (Math.log(2)*filterSize)/setSize;

        for (int i = 0; i < numHashes; i++) {

            functions.add(new HashFunction() {
                @Override
                public int hash(String s) {
                    return 0;
                }
            });

            filters.add(new BitSet[filterSize]);
        }
    }
}
