public interface BloomFilter {
    public void add(String s);

    public boolean appears(String s);

    public int filerSize();

    public int dataSize();

    public int numHashes();
}
