/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package be.uantwerpen.adrem.hadoop.util;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Provides easy access to Writable 2 dimensional int arrays.
 */
public class IntMatrixWritable extends ArrayWritable {
  
  public static final IntMatrixWritable EmptyImw = new IntMatrixWritable(new IntArrayWritable[0]);
  
  public IntMatrixWritable() {
    super(IntArrayWritable.class);
  }
  
  public IntMatrixWritable(IntArrayWritable... iw) {
    this();
    set(iw);
  }
  
  public int[][] toIntMatrix() {
    Writable[] writables = get();
    int[][] tids = new int[writables.length][];
    for (int tidPartIx = 0; tidPartIx < writables.length; tidPartIx++) {
      Writable[] iaw = ((IntArrayWritable) writables[tidPartIx]).get();
      tids[tidPartIx] = new int[iaw.length];
      for (int j = 0; j < iaw.length; j++) {
        tids[tidPartIx][j] = ((IntWritable) iaw[j]).get();
      }
    }
    for (int[] partTids : tids) {
      Arrays.sort(partTids);
    }
    return tids;
  }
  
  @Override
  public Writable[] get() {
    Writable[] orig = super.get();
    IntArrayWritable[] good = new IntArrayWritable[orig.length];
    for (int i = 0; i < orig.length; i++) {
      good[i] = (IntArrayWritable) orig[i];
    }
    return good;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String s : super.toStrings()) {
      sb.append(s).append(" ");
    }
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    IntMatrixWritable other = (IntMatrixWritable) obj;
    Writable[] iw1 = this.get();
    Writable[] iw2 = other.get();
    if (iw1.length != iw2.length) return false;
    for (int i = 0; i < iw1.length; i++)
      if (!iw1[i].equals(iw2[i])) return false;
    return true;
  }
  
  @Override
  public int hashCode() {
    int hashCode = 0;
    for (Writable i : get()) {
      hashCode += i.hashCode();
    }
    return hashCode;
  }
}