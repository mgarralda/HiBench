/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hibench.sparkbench.common.datagen;
import java.util.Random;
/** Historical HtmlCore pure-link subset: no URL draws precede each link batch. */
public final class WebLinkGenerator {
  private final Random length;
  private final LegacyZipfCore links;
  public WebLinkGenerator(int slot, LegacyZipfCore kernel) {
    Random seeds=new Random(slot);
    seeds.nextLong(); seeds.nextLong();
    links=kernel.copy(); links.setRandSeed(seeds.nextLong());
    length=new Random(seeds.nextLong());
  }
  public long[] nextLinks() {
    double g;
    do { g=length.nextGaussian(); } while(g < -10.0 || g > (Short.MAX_VALUE-800)/80.0);
    long[] result=new long[(int)Math.floor(.05*Math.round(800+80*g))];
    for(int i=0;i<result.length;i++) result[i]=links.next();
    return result;
  }
}
