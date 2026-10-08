package HiBench;
import org.hibench.sparkbench.sql.datagen.SqlZipfCore;
import org.hibench.sparkbench.sql.datagen.SqlZipfian;
/** Historical jar is a test fixture only; compare kernels without materializing huge datasets. */
public final class SqlZipfReference {
  public static void main(String[] args) {
    for (long pages : new long[]{120, 12000, 120000, 1200000, 12000000, 10000000, 100000000, 120000000}) {
      Zipfian original = new Zipfian(pages, 0.5);
      original.setupZipf(pages * 40, 0.1);
      ZipfCore oldKernel = original.createZipfCore();
      SqlZipfian current = new SqlZipfian(pages, 0.5);
      current.setupZipf(pages * 40, 0.1);
      SqlZipfCore newKernel = current.createSqlZipfCore();
      for (int seed : new int[]{1,2,17}) {
        oldKernel.setRandSeed(seed); newKernel.setRandSeed(seed);
        for (int i=0;i<20000;i++) {
          long expected=oldKernel.next(),actual=newKernel.next();
          if(expected!=actual) throw new AssertionError("Zipf mismatch at " + pages + "/" + seed + "/" + i);
        }
      }
      System.out.println("HIBENCH_SQL_ZIPF_VERIFIED pages=" + pages + " draws=60000");
    }
  }
}
