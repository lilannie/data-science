import java.math.BigInteger;
import java.util.BitSet;
import java.util.Random;

public class BloomFilterFNV extends BloomFilter {

    /**
     * Instaniate a BloomFilter that uses FNV hash functions
     * @param setSize Size of data that will be put in the filter
     * @param bitsPerElement Amount of bits per each element in the set
     */
    public BloomFilterFNV(int setSize, int bitsPerElement) {
        this.setSize = setSize;
        this.bitsPerElement = bitsPerElement;

        this.filterSize = setSize * bitsPerElement;
        this.filter = new BitSet[filterSize];

        this.numHashes = (int) (Math.log(2) * filterSize) / setSize;
        boolean reverse = true;

        for (int i = 0; i < numHashes; i++) {
            Random rand = new Random();

            if (reverse) {
                functions.add(new HashFunction() {
                    private final BigInteger INIT64  = new BigInteger("cbf29ce484222325", 16);
                    private final BigInteger PRIME64 = new BigInteger("100000001b3",      16);
                    private final BigInteger MOD64   = new BigInteger("2").pow(64);
                    int randSwitch1 = -1;
                    int randSwitch2 = -1;

                    @Override
                    public int hash(String s) {
                        // Pre-processing to create k-hash values
                        byte[] data = s.getBytes();

                        // Set randomSwitch index if not already set
                        if (randSwitch1 < 0) {
                            randSwitch1 = rand.nextInt(data.length-1);
                            randSwitch2 = rand.nextInt(data.length-1);
                        }

                        byte temp = data[randSwitch1];
                        data[randSwitch1] = data[randSwitch2];
                        data[randSwitch2] = temp;

                        BigInteger hash = INIT64;
                        for (int i = data.length-1; i >= 0; i--) {
                            hash = hash.xor(BigInteger.valueOf((int) data[i] & 0xff));
                            hash = hash.multiply(PRIME64).mod(MOD64);
                        }
                        return Math.abs(hash.intValue()) % filterSize;
                    }
                });
            }
            else {
                functions.add(new HashFunction() {
                    private final BigInteger INIT64  = new BigInteger("cbf29ce484222325", 16);
                    private final BigInteger PRIME64 = new BigInteger("100000001b3",      16);
                    private final BigInteger MOD64   = new BigInteger("2").pow(64);
                    int randSwitch1 = -1;
                    int randSwitch2 = -1;

                    @Override
                    public int hash(String s) {
                        // Pre-processing to create k-hash values
                        byte[] data = s.getBytes();

                        // Set randomSwitch index if not already set
                        if (randSwitch1 < 0) {
                            randSwitch1 = rand.nextInt(data.length-1);
                            randSwitch2 = rand.nextInt(data.length-1);
                        }

                        byte temp = data[randSwitch1];
                        data[randSwitch1] = data[randSwitch2];
                        data[randSwitch2] = temp;

                        BigInteger hash = INIT64;
                        for (byte b : data) {
                            hash = hash.xor(BigInteger.valueOf((int) b & 0xff));
                            hash = hash.multiply(PRIME64).mod(MOD64);
                        }

                        return Math.abs(hash.intValue()) % filterSize;
                    }
                });
            }

            reverse = !reverse;
        }
    }
}