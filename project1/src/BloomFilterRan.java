import java.math.BigInteger;
import java.util.BitSet;
import java.util.Random;

public class BloomFilterRan extends BloomFilter {
    /**
     * @param setSize
     * @param bitsPerElement
     */
    public BloomFilterRan(int setSize, int bitsPerElement) {
        this.filterSize = setSize * bitsPerElement;
        this.setSize = setSize;
        this.bitsPerElement = bitsPerElement;

        this.numHashes = (int) (Math.log(2)*filterSize)/setSize;

        for (int i = 0; i < numHashes; i++) {

            functions.add(new HashFunction() {
                int prime = -1;
                int a;
                int b;
                @Override
                public int hash(String s) {
                    int hashCode = s.hashCode();
                    Random rand = new Random(); // generate a random number
                    if (prime < 0) {
                        prime = rand.nextInt(bitsPerElement*setSize);

                        while (!isPrime(prime)) {
                            prime = rand.nextInt(bitsPerElement*setSize);
                        }
                        a = rand.nextInt(prime);
                        b = rand.nextInt(prime);
                    }

                    return ((a*hashCode)+ b) % prime;
                }

                /**
                 * Checks to see if the requested value is prime.
                 */
                private boolean isPrime(int inputNum){
                    if (inputNum <= 3 || inputNum % 2 == 0)
                        return inputNum == 2 || inputNum == 3; //this returns false if number is <=1 & true if number = 2 or 3
                    int divisor = 3;
                    while ((divisor <= Math.sqrt(inputNum)) && (inputNum % divisor != 0))
                        divisor += 2; //iterates through all possible divisors
                    return inputNum % divisor != 0; //returns true/false
                }
            });

            filters.add(new BitSet[filterSize]);
        }
    }


}
