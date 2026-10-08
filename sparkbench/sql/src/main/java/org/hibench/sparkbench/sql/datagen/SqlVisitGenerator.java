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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;



/***
 * Util used to generate random user visit records
 * @author lyi2
 *
 */
public class SqlVisitGenerator {
	private Random rand;
	private String delim = ",";
	private String[] uagents, ccodes, skeys;
	private long urls, dateRange;
	private Date date;
	private SimpleDateFormat dateForm;
	
    public SqlVisitGenerator(int seed, long numUrls) {
        rand = new Random(seed);
        date = new Date();
        dateRange = 1335830400000L; // Original 2012-05-01 boundary, frozen to UTC.
        dateForm = new SimpleDateFormat("yyyy-MM-dd", java.util.Locale.ROOT);
        dateForm.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
        urls = numUrls;
        uagents = SqlVocabulary.userAgents;
        ccodes = SqlVocabulary.countryCodes;
        skeys = SqlVocabulary.searchKeys;
    }

	private String nextCountryCode() {
		return ccodes[rand.nextInt(ccodes.length)];
	}
	
	private String nextUserAgent() {
		return uagents[rand.nextInt(uagents.length)];
	}
	
	private String nextSearchKey () {
		return skeys[rand.nextInt(skeys.length)];
	}

	private String nextTimeDuration() {
		return Integer.toString(rand.nextInt(10)+1);
	}

	private String nextIp() {
		return Integer.toString(rand.nextInt(254)+1)
				+ "." +  Integer.toString(rand.nextInt(255))
				+ "." +  Integer.toString(rand.nextInt(255))
				+ "." +  Integer.toString(rand.nextInt(254)+1);
	}
	
	private String nextDate() {
		date.setTime((long) Math.floor(rand.nextDouble() * dateRange));
		return dateForm.format(date);
	}
	
	private String nextProfit() {
		return Float.toString(rand.nextFloat());
	}

	public long nextUrlId() {
		return (long) Math.floor(rand.nextDouble()*urls);
	}

	/***
	 * set the randseed of random generator
	 * @param randSeed
	 */
	public void fireRandom(int randSeed) {
		rand.setSeed(randSeed);
	}

	public String nextAccess(String url) {
		return(nextIp() + delim +
			url + delim +
			nextDate() + delim +
			nextProfit() + delim +
			nextUserAgent() + delim +
			nextCountryCode() + delim +
			nextSearchKey() + delim +
			nextTimeDuration());
	}
	
	public String debug() {
		return
		"[delim: " + delim + "] " +
		"[urls: " + urls + "]";
	}
}
