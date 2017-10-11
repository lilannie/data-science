import java.util.BitSet;
import java.util.Random;

/**
 * @author Annie Steenson
 */
public class BloomFilterRan extends BloomFilter {
    /**
     * Instantiate a BloomFilter that uses random hash functions to assign filtered indexes
     * The filter size will be set to a prime number p that is at least (setSize*bitsPerElement)
     * @param setSize Size of data that will be put in the filter
     * @param bitsPerElement Amount of bits per each element in the set
     */
    public BloomFilterRan(int setSize, int bitsPerElement) {
        this.filterSize = setSize * bitsPerElement;
        getPrimeFilterSize();
        this.setSize = setSize;
        this.bitsPerElement = bitsPerElement;

        this.filter = new BitSet[filterSize];

        this.numHashes = (int) (Math.log(2)*filterSize)/setSize;

        for (int i = 0; i < numHashes; i++) {
            functions.add(new HashFunction() {
                int a = -1;
                int b = -1;

                /**
                 * This random hash function hashes a String s using two random numbers chosen from {0... filterSize-1}
                 * @param s String
                 * @return int
                 */
                @Override
                public int hash(String s) {
                    int hashCode = s.hashCode();
                    if (a < 0) {
                        Random rand = new Random(); // generate a random number
                        this.a = rand.nextInt(filterSize);
                        this.b = rand.nextInt(filterSize);
                    }
                    return Math.abs((a * hashCode)+ b) % filterSize;
                }
            });
        }
    }

    /**
     * This helper function adds to the filterSize until it is a prime number
     * This ensures we get the next smallest prime number that is greater than (setSize*bitsPerElement)
     */
    private void getPrimeFilterSize() {
        while (isPrime(filterSize)) {
            filterSize++;
        }
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
}
