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

import java.io.Serializable;
import java.util.Random;

public class SqlZipfCore implements Serializable {

	private static final long serialVersionUID = 2483499022100222872L;

	public long elems, zelems;
	public double exponent, scale;

	public int gran, divider;
	public long mask, limit;

	public int[] buckIndex;
	public long[] zbuck, xbuck, ybuck;	// bucks represented by three arrays
	
	public Random rand;

	public SqlZipfCore() {
		rand = new Random();
	}

	public void setRandSeed(long seed) {
		if (null==rand) {
			rand = new Random(seed);
		} else {
			rand.setSeed(seed);
		}
	}

	public SqlZipfCore copy() {
        SqlZipfCore c = new SqlZipfCore();
        c.elems=elems; c.zelems=zelems; c.exponent=exponent; c.scale=scale;
        c.gran=gran; c.divider=divider; c.mask=mask; c.limit=limit;
        c.buckIndex=buckIndex; c.zbuck=zbuck; c.xbuck=xbuck; c.ybuck=ybuck;
        return c;
    }
    public long simpleNext() {

		long v = (long) Math.floor(rand.nextDouble() * zelems);

//		count++;
		int start = 0, end = zbuck.length-2, mid;
		while (start != end) {
			mid = (start + end) / 2;
			if (v >= zbuck[mid+1]) {
				start = mid + 1;
			} else {
				end = mid;
			}
//			count++;
		}
		return xbuck[start] + (v - zbuck[start]) / ybuck[start];
	}

	public long next() {

		long v = (long) Math.floor(rand.nextDouble() * zelems);
		
		long X = (v + limit) >> divider;
		int ipart = 63 - Long.numberOfLeadingZeros(X >> gran);
		int i = (int) ((ipart << gran) + (mask & (X >> ipart)));

//		count++;
		int start = buckIndex[i], end = buckIndex[i+1], mid;
		while (start != end) {
			mid = (start + end) / 2;
			if (v >= zbuck[mid+1]) {
				start = mid + 1;
			} else {
				end = mid;
			}
//			count++;
		}
		return xbuck[start] + (v - zbuck[start]) / ybuck[start];
	}
}
