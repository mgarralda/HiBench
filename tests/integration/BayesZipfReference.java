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
import org.hibench.sparkbench.common.datagen.LegacyZipfian;
import org.hibench.sparkbench.common.datagen.LegacyZipfCore;
public final class BayesZipfReference {
  public static void main(String[] args) {
    for(long pages:new long[]{25000,30000,100000,500000,1000000,20000000}) {
      Zipfian old=new Zipfian(479623,.9);old.setupZipf(pages*760,.1);
      LegacyZipfian modern=new LegacyZipfian(479623,.9);modern.setupZipf(pages*760,.1);
      ZipfCore a=old.createZipfCore();LegacyZipfCore b=modern.createLegacyZipfCore();
      for(int slot:new int[]{1,2,17}) {
        java.util.Random seeds=new java.util.Random(slot);seeds.nextLong();seeds.nextLong();long seed=seeds.nextLong();
        a.setRandSeed(seed);b.setRandSeed(seed);
        for(int i=0;i<20000;i++) if(a.next()!=b.next()) throw new AssertionError("Word Zipf mismatch");
      }
      System.out.println("HIBENCH_BAYES_ZIPF_VERIFIED pages="+pages+" draws=60000");
    }
  }
}
