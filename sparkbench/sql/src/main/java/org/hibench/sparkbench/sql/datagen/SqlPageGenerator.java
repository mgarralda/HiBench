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

package org.hibench.sparkbench.sql.datagen;
import java.util.Random;

/** The SQL subset of historical HtmlCore: URL lengths, Gaussian link counts and Zipf targets. */
public final class SqlPageGenerator {
    private final Random urls, page;
    private final SqlZipfCore links;
    public SqlPageGenerator(int slot, SqlZipfCore kernel) {
        Random seeds = new Random(slot);
        urls = new Random(seeds.nextLong());
        seeds.nextLong(); // Historical external-link RNG, unused in SQL generation.
        links = kernel.copy(); links.setRandSeed(seeds.nextLong());
        page = new Random(seeds.nextLong());
    }
    public String nextUrl() {
        int size = page.nextInt(91) + 10;
        char[] value = new char[size];
        for (int i=0; i<size; i++) value[i] = (char) ('a' + urls.nextInt(26));
        return new String(value);
    }
    public long[] nextLinks() {
        double gaussian;
        do { gaussian = page.nextGaussian(); }
        while (gaussian < -10.0 || gaussian > (Short.MAX_VALUE - 800) / 80.0);
        int length = (int) Math.round(800 + 80 * gaussian);
        long[] ids = new long[(int) Math.floor(0.05 * length)];
        for (int i=0; i<ids.length; i++) ids[i] = links.next();
        return ids;
    }
}
