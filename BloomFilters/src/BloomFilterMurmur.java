import java.util.BitSet;
import java.util.Random;

/**
 * @author Annie Steenson
 */
public class BloomFilterMurmur extends BloomFilter {

    /**
     * Instantiate a BloomFilter that uses Murmur hash functions
     * @param setSize Size of data that will be put in the filter
     * @param bitsPerElement Amount of bits per each element in the set
     */
    public BloomFilterMurmur(int setSize, int bitsPerElement) {
        this.filterSize = setSize * bitsPerElement;
        this.setSize = setSize;
        this.bitsPerElement = bitsPerElement;

        this.filter = new BitSet[filterSize];

        this.numHashes = (int) (Math.log(2)*filterSize)/setSize;

        for (int i = 0; i < numHashes; i++) {
            functions.add(new HashFunction() {
                int seed = new Random().nextInt();

                /**
                 * Uses Murmur hash function to hash a String
                 * @param text String
                 * @return int Hashvalue
                 */
                @Override
                public int hash( final String text) {
                    final byte[] bytes = text.getBytes();
                    return hash64( bytes, bytes.length);
                }

                /**
                 * Returns a hashed value of the byte[] data. This hash function uses a randomly generated seed
                 * @param data byte[]
                 * @param length int length of data
                 * @return int
                 */
                private int hash64(final byte[] data, int length) {
                    final long m = 0xc6a4a7935bd1e995L;
                    final int r = 47;

                    long h = (seed & 0xffffffffL)^(length * m);

                    int length8 = length/8;

                    for (int i=0; i<length8; i++) {
                        final int i8 = i*8;
                        long k =  ((long)data[i8+0]&0xff)      +(((long)data[i8+1]&0xff)<<8)
                                +(((long)data[i8+2]&0xff)<<16) +(((long)data[i8+3]&0xff)<<24)
                                +(((long)data[i8+4]&0xff)<<32) +(((long)data[i8+5]&0xff)<<40)
                                +(((long)data[i8+6]&0xff)<<48) +(((long)data[i8+7]&0xff)<<56);

                        k *= m;
                        k ^= k >>> r;
                        k *= m;

                        h ^= k;
                        h *= m;
                    }

                    switch (length%8) {
                        case 7: h ^= (long)(data[(length&~7)+6]&0xff) << 48;
                        case 6: h ^= (long)(data[(length&~7)+5]&0xff) << 40;
                        case 5: h ^= (long)(data[(length&~7)+4]&0xff) << 32;
                        case 4: h ^= (long)(data[(length&~7)+3]&0xff) << 24;
                        case 3: h ^= (long)(data[(length&~7)+2]&0xff) << 16;
                        case 2: h ^= (long)(data[(length&~7)+1]&0xff) << 8;
                        case 1: h ^= (long)(data[length&~7]&0xff);
                            h *= m;
                    };

                    h ^= h >>> r;
                    h *= m;
                    h ^= h >>> r;

                    return Math.abs((int) h) % filterSize;
                }
            });
        }
    }
}
