import java.util.ArrayList;
import java.util.List;
import org.apache.mahout.math.Vector;
import org.apache.mahout.clustering.kmeans.GenKMeansDataset;

/** Read-only reference runner for the pre-migration JAR, never a production generator. */
public final class GaussianOriginalReference {
  public static void main(String[] args) throws Exception {
    int n = 100001, dimensions = 3, clusters = 5;
    double[][][] params = new double[clusters][dimensions][2];
    for (int k = 0; k < clusters; k++) for (int d = 0; d < dimensions; d++) {
      params[k][d][0] = k * 100 + d * 10;
      params[k][d][1] = 1 + k + d;
    }
    GenKMeansDataset.GaussianSampleGenerator generator =
        new GenKMeansDataset.GaussianSampleGenerator(new byte[16]);
    generator.setGenParams(n, dimensions, params, 0, 1000);
    List<Vector> rows = new ArrayList<>();
    generator.produceSamples(rows);
    if (rows.size() != n) throw new AssertionError("Original sample count");
    double sum = 0, sumSq = 0; int tail = 0, index = 0;
    for (int k = 0; k < clusters; k++) {
      int count = n / clusters + (k < n % clusters ? 1 : 0);
      for (int i = 0; i < count; i++) {
        Vector row = rows.get(index++);
        if (row.size() != dimensions) throw new AssertionError("Original dimension");
        for (int d = 0; d < dimensions; d++) {
          double z = (row.get(d) - params[k][d][0]) / params[k][d][1];
          sum += z; sumSq += z*z; if (Math.abs(z) > 2) tail++;
        }
      }
    }
    int values = n * dimensions;
    double mean = sum / values;
    System.out.println("{\"rows\":"+n+",\"dimensions\":"+dimensions+
      ",\"clusters\":"+clusters+",\"standardized_mean\":"+mean+
      ",\"standardized_variance\":"+(sumSq/values-mean*mean)+
      ",\"two_sigma_tail_fraction\":"+((double)tail/values)+"}");
  }
}
