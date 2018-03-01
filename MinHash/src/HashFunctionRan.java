import java.util.concurrent.ThreadLocalRandom;

/**
 * Hash function to represent a permutation / one-to-one function
 */
public class HashFunctionRan
{
    // create a Random HashFunction for BloomFilterRan
    int a, b, p;

    /**
     * @param range Create a hash function with a prime p of at least size range
     */
    public HashFunctionRan(int range)
    {
        this.p = getPrime(range);
        this.a = ThreadLocalRandom.current().nextInt(0, p);
        this.b = ThreadLocalRandom.current().nextInt(0, p);
    }// end constructor for HashFunction

    /**
     *
     * @param arr integer array
     * @return int hash value
     */
    public int hash(int[] arr) {
        String arrString = "";
        for (int i = 0; i < arr.length; i++) {
            arrString += arr[i];
        }
        return hash(arrString);
    }

    /**
     *
     * @param String s
     * @return hash value of string s
     */
    public int hash(String s) {
        return hash(s.hashCode());
    }// end function for hashing a string

    /**
     *
     * @param int x
     * @return hash value of int x
     */
    private int hash(int x)
    {
        return (a*x + b) % p;
    }// end function for hashing an integer

    /**
     *
     * @param int n
     * @return prime number of at least n
     */
    private int getPrime(int n) {
        // return the first positive prime of at least size n
        boolean found = false;

        while(!found){
            // while loop until we find a prime >= n
            if(isPrime(n)){
                // found a prime
                found = true;
            }else{
                // did not find prime
                if(n == 1 || n % 2 == 0){
                    n = n + 1;
                }else{
                    n = n + 2;
                }// end if we have an even number

            }// end if this is a prime

        }// end while we haven't found a prime

        return n;
    }// end function getPrime

    /**
     *
     * @param num
     * @return true of num is prime, false otherwise
     */
    private boolean isPrime(int num) {
        if ( num > 2 && num % 2 == 0 ) {
            return false;
        }// end if number > 2 and even
        int top = (int) Math.sqrt(num) + 1;
        for(int i = 3; i < top; i+=2){
            if(num % i == 0){
                return false;
            }// end if we found a divisor, not a prime
        }// end for loop checking if prime
        return true;
    }// end function isPrime

}// end class HashFunctionRan
