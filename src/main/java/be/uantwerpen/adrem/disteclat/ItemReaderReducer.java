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
package be.uantwerpen.adrem.disteclat;

import static be.uantwerpen.adrem.disteclat.DistEclatDriver.OSingletonsDistribution;
import static be.uantwerpen.adrem.disteclat.DistEclatDriver.OSingletonsOrder;
import static be.uantwerpen.adrem.disteclat.DistEclatDriver.OSingletonsTids;
import static be.uantwerpen.adrem.util.FIMOptions.NUMBER_OF_MAPPERS_KEY;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.Integer.parseInt;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import be.uantwerpen.adrem.hadoop.util.IntArrayWritable;
import be.uantwerpen.adrem.hadoop.util.IntMatrixWritable;

/**
 * Reducer for the first cycle for DistEclat. It receives the complete set of frequent singletons from the different
 * mappers. The frequent singletons are sorted on ascending frequency and distributed among a number of map-tasks.
 * 
 * <pre>
 * {@code
 * Original Input Per Mapper:
 * 
 * 1 2                                      | Mapper 1
 * 1                                        | Mapper 1
 * 
 * 1 2 3                                    | Mapper 2
 * 1 2                                      | Mapper 2
 * 
 * 1 2                                      | Mapper 3
 * 2 3                                      | Mapper 3
 * 
 * 
 * 
 * Example Phase=1, MinSup=1:
 * ==========================
 * 
 * Input:
 * Text                   Iterable<IntArrayWritable>
 * (Prefix)               (<[Mapper ID, Item, Partial TIDs...]>)
 * ""                     <[1,1,0,1],[1,2,0],[2,1,0,1],[2,2,0,1],[2,3,0],[3,1,0],[3,2,0,1],[3,3,1]>
 * 
 * Output:
 * 
 * ==> OSingletonsOrder
 * IntWritable            Writable (Text)
 * (Empty)                (Sorted items)
 *                        1 2 3
 * 
 * 
 * ==> OSingletonsDistribution
 * IntWritable            Writable (Text)
 * (MapperId)             (Assigned singletons)
 * 1                      1
 * 2                      2
 * 3                      3
 * 
 * ==> OSingletonsTids
 * IntWritable            Writable (IntMatrixWritable)
 * (Singleton)            (Partial TIDs)
 * 1                      [[0,1],[0,1],[0]]
 * 2                      [[0],[0,1],[0,1]]
 * 3                      [[],[0],[1]]
 * }
 * </pre>
 */
public class ItemReaderReducer extends Reducer<Text,IntArrayWritable,IntWritable,Writable> {
  
  public static final Text EmptyKey = new Text("");
  
  private int numberOfMappers;
  private final Map<Integer,MutableInt> itemSupports = newHashMap();
  private MultipleOutputs<IntWritable,Writable> mos;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    mos = new MultipleOutputs<IntWritable,Writable>(context);
    numberOfMappers = parseInt(conf.get(NUMBER_OF_MAPPERS_KEY, "1"));
  }
  
  @Override
  public void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
      throws IOException, InterruptedException {
    Map<Integer,IntArrayWritable[]> map = newHashMap();
    for (IntArrayWritable iaw : values) {
      Writable[] w = iaw.get();
      int mapperId = ((IntWritable) w[0]).get();
      int item = ((IntWritable) w[1]).get();
      IntArrayWritable[] tidList = map.get(item);
      if (tidList == null) {
        tidList = new IntArrayWritable[numberOfMappers];
        Arrays.fill(tidList, new IntArrayWritable(new IntWritable[0]));
        map.put(item, tidList);
        itemSupports.put(item, new MutableInt());
      }
      IntWritable[] copy = Arrays.copyOf((IntWritable[]) tidList[mapperId].get(),
          itemSupports.get(item).intValue() + w.length - 2);
      for (int i = w.length - 1, ix = copy.length - 1; i >= 2; i--) {
        copy[ix--] = (IntWritable) w[i];
      }
      tidList[mapperId] = new IntArrayWritable(copy);
      itemSupports.get(item).add(w.length - 2);
    }
    
    // should only get one, otherwise duplicated item in database
    for (Entry<Integer,IntArrayWritable[]> entry : map.entrySet()) {
      final Integer item = entry.getKey();
      final IntArrayWritable[] tids = entry.getValue();
      
      // write the item with the tidlist
      mos.write(OSingletonsTids, new IntWritable(item), new IntMatrixWritable(tids));
    }
    
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    List<Integer> sortedSingletons = getSortedSingletons();
    
    context.setStatus("Writing Singletons");
    writeSingletonsOrders(sortedSingletons);
    
    context.setStatus("Distributing Singletons");
    writeSingletonsDistribution(sortedSingletons);
    
    context.setStatus("Finished");
    mos.close();
  }
  
  /**
   * Gets the list of singletons in ascending order of frequency.
   * 
   * @return the sorted list of singletons
   */
  private List<Integer> getSortedSingletons() {
    List<Integer> items = newArrayList(itemSupports.keySet());
    
    Collections.sort(items, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return itemSupports.get(o1).compareTo(itemSupports.get(o2));
      }
    });
    
    return items;
  }
  
  /**
   * Writes the singletons order to the file OSingletonsOrder.
   * 
   * @param sortedSingletons
   *          the sorted singletons
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeSingletonsOrders(List<Integer> sortedSingletons) throws IOException, InterruptedException {
    StringBuilder builder = new StringBuilder();
    for (Integer singleton : sortedSingletons) {
      builder.append(singleton).append(" ");
    }
    
    Text order = new Text(builder.substring(0, builder.length() - 1));
    mos.write(OSingletonsOrder, EmptyKey, order);
  }
  
  /**
   * Writes the singletons distribution to file OSingletonsDistribution. The distribution is obtained using Round-Robin
   * allocation.
   * 
   * @param sortedSingletons
   *          the sorted list of singletons
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeSingletonsDistribution(List<Integer> sortedSingletons) throws IOException, InterruptedException {
    int end = Math.min(numberOfMappers, sortedSingletons.size());
    
    Text mapperId = new Text();
    Text assignedItems = new Text();
    
    // Round robin assignment
    for (int ix = 0; ix < end; ix++) {
      StringBuilder sb = new StringBuilder();
      for (int ix1 = ix; ix1 < sortedSingletons.size(); ix1 += numberOfMappers) {
        sb.append(sortedSingletons.get(ix1)).append(" ");
      }
      
      mapperId.set("" + ix);
      assignedItems.set(sb.substring(0, sb.length() - 1));
      mos.write(OSingletonsDistribution, mapperId, assignedItems);
    }
  }
}