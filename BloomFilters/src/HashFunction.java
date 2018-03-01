/**
 * @author Annie Steenson
 * This interface is used to create Hash functions for each BloomFilter
 */
public interface HashFunction {
    public int hash(String s);
}
