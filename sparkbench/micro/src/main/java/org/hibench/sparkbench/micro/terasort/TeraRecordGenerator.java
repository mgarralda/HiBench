/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hibench.sparkbench.micro.terasort;

/** Hadoop 3.3.6 TeraGen record algorithm, without a MapReduce runtime. */
public final class TeraRecordGenerator {
  private Unsigned16 random;
  private long expected = -1;
  public byte[] next(long row) {
    if (random == null || row != expected) random = Random16.skipAhead(new Unsigned16(row));
    Random16.nextRand(random);
    byte[] record = new byte[100];
    generateRecord(record, random, new Unsigned16(row));
    expected = row + 1;
    return record;
  }
  static void generateRecord(byte[] recBuf, Unsigned16 rand, 
                                     Unsigned16 recordNumber) {
    /* generate the 10-byte key using the high 10 bytes of the 128-bit
     * random number
     */
    for(int i=0; i < 10; ++i) {
      recBuf[i] = rand.getByte(i);
    }

    /* add 2 bytes of "break" */
    recBuf[10] = 0x00;
    recBuf[11] = 0x11;

    /* convert the 128-bit record number to 32 bits of ascii hexadecimal
     * as the next 32 bytes of the record.
     */
    for (int i = 0; i < 32; i++) {
      recBuf[12 + i] = (byte) recordNumber.getHexDigit(i);
    }

    /* add 4 bytes of "break" data */
    recBuf[44] = (byte) 0x88;
    recBuf[45] = (byte) 0x99;
    recBuf[46] = (byte) 0xAA;
    recBuf[47] = (byte) 0xBB;

    /* add 48 bytes of filler based on low 48 bits of random number */
    for(int i=0; i < 12; ++i) {
      recBuf[48+i*4] = recBuf[49+i*4] = recBuf[50+i*4] = recBuf[51+i*4] =
        (byte) rand.getHexDigit(20 + i);
    }

    /* add 4 bytes of "break" data */
    recBuf[96] = (byte) 0xCC;
    recBuf[97] = (byte) 0xDD;
    recBuf[98] = (byte) 0xEE;
    recBuf[99] = (byte) 0xFF;
  }


}
