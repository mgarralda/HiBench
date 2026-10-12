/*
Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package HiBench;
import org.hibench.sparkbench.common.datagen.LegacyZipfCore;
import org.hibench.sparkbench.common.datagen.LegacyZipfian;
/** Historical jar is a test fixture only; compare kernels without materializing huge datasets. */
public final class SqlZipfReference {
  public static void main(String[] args) {
    for (long pages : new long[]{120, 12000, 120000, 1200000, 12000000, 10000000, 100000000, 120000000}) {
      Zipfian original = new Zipfian(pages, 0.5);
      original.setupZipf(pages * 40, 0.1);
      ZipfCore oldKernel = original.createZipfCore();
      LegacyZipfian current = new LegacyZipfian(pages, 0.5);
      current.setupZipf(pages * 40, 0.1);
      LegacyZipfCore newKernel = current.createLegacyZipfCore();
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
