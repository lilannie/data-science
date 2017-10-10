import java.math.BigInteger;
import java.util.BitSet;

public class BloomFilterFNV extends BloomFilter {

    /**
     * @param setSize
     * @param bitsPerElement
     */
    public BloomFilterFNV(int setSize, int bitsPerElement) {
        this.filterSize = setSize * bitsPerElement;
        this.setSize = setSize;
        this.bitsPerElement = bitsPerElement;

        this.numHashes = (int) (Math.log(2) * filterSize) / setSize;

        for (int i = 0; i < numHashes; i++) {

            functions.add(new HashFunction() {
                private final BigInteger INIT64  = new BigInteger("cbf29ce484222325", 16);
                private final BigInteger PRIME64 = new BigInteger("100000001b3",      16);
                private final BigInteger MOD64   = new BigInteger("2").pow(64);

                @Override
                public int hash(String s) {
                    byte[] data = s.getBytes();
                    BigInteger hash = INIT64;

                    for (byte b : data) {
                        hash = hash.xor(BigInteger.valueOf((int) b & 0xff));
                        hash = hash.multiply(PRIME64).mod(MOD64);
                    }

                    return hash.intValue();
                }
            });

            filters.add(new BitSet[filterSize]);
        }
    }
}