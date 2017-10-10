import java.util.BitSet;
import java.util.Iterator;
import java.util.Random;

public class DynamicFilter extends BloomFilter {
    /**
     * @param bitsPerElement
     */
    public DynamicFilter(int bitsPerElement) {
        this.setSize = 1000;
        this.filterSize = setSize * bitsPerElement;
        this.bitsPerElement = bitsPerElement;

        addHashFunction();
    }

    @Override
    public void add(String s) {
        // TODO What should filtersize return?
        if (dataSize >= filterSize) {
            setSize = setSize * 2;
            filterSize = setSize * bitsPerElement;
            addHashFunction();
        }

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

    private void addHashFunction() {
        functions.add(new HashFunction() {
            int prime = -1;
            int a;
            int b;

            @Override
            public int hash(String s) {
                int hashCode = s.hashCode();
                Random rand = new Random(); // generate a random number
                if (prime < 0) {
                    prime = rand.nextInt(bitsPerElement * setSize);

                    while (!isPrime(prime)) {
                        prime = rand.nextInt(bitsPerElement * setSize);
                    }
                    a = rand.nextInt(prime);
                    b = rand.nextInt(prime);
                }

                return ((a * hashCode) + b) % prime;
            }

            /**
             * Checks to see if the requested value is prime.
             */
            private boolean isPrime(int inputNum) {
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
