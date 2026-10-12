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
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.PrintWriter;
import org.hibench.sparkbench.common.datagen.LegacyZipfian;
import org.hibench.sparkbench.common.datagen.WebLinkGenerator;
/** Frozen HtmlCore pure-link formulas compared against the shared kernel. Test only. */
public final class WebLinkReference {
  public static void main(String[] args) throws Exception {
    ArrayList<long[]> fixture=new ArrayList<>();
    for(long pages:new long[]{50,5000,500000,5000000,30000000,50000000}) {
      Zipfian old=new Zipfian(pages,.5);old.setupZipf(pages*40,.1);
      LegacyZipfian modern=new LegacyZipfian(pages,.5);modern.setupZipf(pages*40,.1);
      for(int slot:new int[]{1,2,17}) {
        ZipfCore kernel=old.createZipfCore();
        Random seeds=new Random(slot);seeds.nextLong();seeds.nextLong();kernel.setRandSeed(seeds.nextLong());
        Random length=new Random(seeds.nextLong());
        WebLinkGenerator current=new WebLinkGenerator(slot,modern.createLegacyZipfCore());
        for(int row=0;row<1000;row++) {
          double g;do {g=length.nextGaussian();} while(g < -10.0 || g > (Short.MAX_VALUE-800)/80.0);
          int n=(int)Math.floor(.05*Math.round(800+80*g));long[] expected=new long[n];
          for(int i=0;i<n;i++)expected[i]=kernel.next();
          if(!Arrays.equals(expected,current.nextLinks()))throw new AssertionError("Pure-link mismatch");
          if(pages==50 && slot<=2 && row<25)for(long dst:expected)fixture.add(new long[]{25L*(slot-1)+row,dst});
        }
      }
      System.out.println("HIBENCH_PAGERANK_LINKS_VERIFIED pages="+pages+" batches=3000");
    }
    try(PrintWriter out=new PrintWriter(args[0],"UTF-8")) {
      out.print("{\"pages\":50,\"partitions\":2,\"seed\":1,\"edges\":[");
      for(int i=0;i<fixture.size();i++) {if(i>0)out.print(",");long[] x=fixture.get(i);out.print("["+x[0]+","+x[1]+"]");}
      out.println("]}");
    }
  }
}
