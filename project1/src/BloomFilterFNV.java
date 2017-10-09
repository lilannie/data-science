import java.util.BitSet;

public class BloomFilterFNV extends BloomFilter {
    /**
     * @param setSize
     * @param bitsPerElement
     */
    public BloomFilterFNV(int setSize, int bitsPerElement) {
        this.filterSize = setSize * bitsPerElement;
        this.bitsPerElement = bitsPerElement;

        this.numHashes = (int) (Math.log(2) * filterSize) / setSize;

        for (int i = 0; i < numHashes; i++) {

            functions.add(new HashFunction() {
                int offset_basis;
                int FNV_prime;

                @Override
                public int hash(String s) {
                    int hash = offset_basis;
                    for (char c : s.toCharArray()) {
                        // Todo make sure c is a byte
                        hash = hash ^ c;
                        hash = hash * FNV_prime;
                    }
                    return hash;
                }
            });

            filters.add(new BitSet[filterSize]);
        }
    }
}