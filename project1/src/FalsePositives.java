public class FalsePositives {
    BloomFilterFNV fnv;
    BloomFilterMurmur murmur;
    BloomFilterRan ran;
    DynamicFilter dynamic;

    public FalsePositives() {
        fnv = new BloomFilterFNV(0, 0);
        murmur = new BloomFilterMurmur(0, 0);
        ran = new BloomFilterRan(0,0);
        dynamic = new DynamicFilter(0);
    }

    public void testFnv() { }

    public void testMurmur() { }

    public void testRan() { }

    public void testDynamic() { }

    public static void main(String[] args) {
        FalsePositives fp = new FalsePositives();
        fp.testFnv();
        fp.testMurmur();
        fp.testRan();
        fp.testDynamic();
    }
}
