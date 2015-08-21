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
package be.uantwerpen.adrem.eclat.util;

import static be.uantwerpen.adrem.util.Tools.intersect;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import be.uantwerpen.adrem.hadoop.util.IntArrayWritable;
import be.uantwerpen.adrem.hadoop.util.IntMatrixWritable;

/**
 * Implementation of a Set Reporter that writes an itemset to the output by it's prefix, the extension and the tids. The
 * extension and the tid list are represented by one IntArrayWritable.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PrefixItemTIDsReporter implements SetReporter {
  
  public static final Text ShortKey = new Text("s");
  private final Context context;
  private final int prefixLength;
  private final List<Item> singletons;
  private final Map<Integer,Integer> orderMap;
  
  public PrefixItemTIDsReporter(Context context, int prefixLength, List<Item> singletons,
      Map<Integer,Integer> orderMap) {
    this.context = context;
    this.prefixLength = prefixLength;
    this.singletons = singletons;
    this.orderMap = orderMap;
  }
  
  @Override
  public void report(int[] itemset, int support) {
    StringBuilder sb = new StringBuilder();
    if (itemset.length < prefixLength) {
      // System.out.println("Found a short fis:" + Arrays.toString(itemset));
      try {
        context.write(ShortKey, new IntMatrixWritable(IntArrayWritable.of(itemset), IntArrayWritable.of(support)));
      } catch (Exception e) {
        e.printStackTrace();
      }
      return;
    }
    int prefixStrLength = 0;
    int lastItem = -1;
    for (int item : itemset) {
      prefixStrLength = sb.length() - 1;
      sb.append(item).append(" ");
      lastItem = item;
    }
    sb.setLength(prefixStrLength);
    
    Text key = new Text(sb.toString());
    
    TidList tids = computeTids(itemset);
    
    IntArrayWritable[] iaw = new IntArrayWritable[tids.tids.length + 1];
    
    for (int i = 1; i < iaw.length; i++) {
      iaw[i] = IntArrayWritable.of(tids.tids[i - 1]);
    }
    iaw[0] = IntArrayWritable.of(lastItem);
    
    try {
      context.write(key, new IntMatrixWritable(iaw));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void close() {
  
  }
  
  private TidList computeTids(int[] itemset) {
    final TidList firstTids = singletons.get(orderMap.get(itemset[0])).getTids();
    // int[] tids = Arrays.copyOf(firstTids, firstTids.length);
    TidList tids = firstTids;
    
    for (int i = 1; i < itemset.length; i++) {
      Item item = singletons.get(orderMap.get(itemset[i]));
      tids = intersect(tids, item.getTids());
    }
    return tids;
  }
  
}