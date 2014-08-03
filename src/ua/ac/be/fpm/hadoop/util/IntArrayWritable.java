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
package ua.ac.be.fpm.hadoop.util;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Provides easy access to IntArrayWritables
 */
public class IntArrayWritable extends ArrayWritable {
  
  public final static IntArrayWritable EmptyIaw = new IntArrayWritable(new IntWritable[0]);
  
  public static IntArrayWritable of(int[] is) {
    if (is == null) {
      return EmptyIaw;
    }
    IntWritable[] iw = new IntWritable[is.length];
    for (int i = 0; i < iw.length; i++) {
      iw[i] = new IntWritable(is[i]);
    }
    return new IntArrayWritable(iw);
  }
  
  public static IntArrayWritable of(Integer i) {
    IntWritable[] iw = new IntWritable[1];
    iw[0] = new IntWritable(i);
    return new IntArrayWritable(iw);
  }
  
  public static IntArrayWritable of(String string) {
    String[] splits = string.split(" ");
    IntWritable[] iw = new IntWritable[splits.length];
    int i = 0;
    for (String split : splits) {
      iw[i++] = new IntWritable(Integer.parseInt(split));
    }
    return new IntArrayWritable(iw);
  }
  
  public IntArrayWritable() {
    super(IntWritable.class);
  }
  
  public IntArrayWritable(IntWritable[] iw) {
    this();
    set(iw);
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
    IntArrayWritable other = (IntArrayWritable) obj;
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