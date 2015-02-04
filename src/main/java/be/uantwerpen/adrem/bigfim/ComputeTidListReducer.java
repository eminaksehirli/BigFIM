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
package ua.ac.be.fpm.bigfim;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static ua.ac.be.fpm.hadoop.util.IntArrayWritable.EmptyIaw;
import static ua.ac.be.fpm.hadoop.util.IntMatrixWritable.EmptyImw;
import static ua.ac.be.fpm.util.FIMOptions.MIN_SUP_KEY;
import static ua.ac.be.fpm.util.FIMOptions.NUMBER_OF_MAPPERS_KEY;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
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

import ua.ac.be.fpm.hadoop.util.IntArrayWritable;
import ua.ac.be.fpm.hadoop.util.IntMatrixWritable;

/**
 * Reducer for the second phase of BigFIM. This reducer combines partial tid lists received from different mappers for a
 * given itemset. The complete tid list is obtained by multiplying the mapper id with an offset representing the maximal
 * size of a sub database then adding the sub tid.
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
 * IntArrayWritable       IntMatrixWritable
 * (Prefix Group or Item) (Partial TIDs)
 * []                     []
 * [1]                    [[0,1],[0,1],[0]]
 * [2]                    [[0],[0,1],[0,1]]
 * [3]                    [[],[0],[1]]
 * []                     []
 * 
 * 
 * Example Phase=2, MinSup=1:
 * ==========================
 * 
 * Input:
 * Text                   Iterable<IntArrayWritable>
 * (Prefix)               (<[Mapper ID, Item, Partial TIDs...]>)
 * 1                      <[1,2,0],[2,2,0,1],[2,3,0],[3,2,0]>
 * 2                      <[2,3,0],[3,3,1]>
 * 
 * Output:
 * 
 * ==> Bucket 0
 * IntArrayWritable       IntMatrixWritable
 * (Prefix Group or Item) (Partial TIDs)
 * [1]                    []                | New prefix group
 * [2]                    [[0],[0,1],[0]]
 * [3]                    [[],[0],[]]
 * []                     []                | End of prefix group
 * 
 * ==> Bucket 1
 * IntArrayWritable       IntMatrixWritable
 * (Prefix Group or Item) (Partial TIDs)
 * [2]                    []                | New prefix group
 * [3]                    [[],[0],[1]]
 * []                     []                | End of prefix group
 * }
 * </pre>
 */
public class ComputeTidListReducer extends Reducer<Text,IntArrayWritable,IntArrayWritable,IntMatrixWritable> {
  
  // max = 1 gb
  private static int MAX_FILE_SIZE = 1000000000;
  public static int MAX_NUMBER_OF_TIDS = (int) ((MAX_FILE_SIZE / 4) * 0.7);
  
  private int minSup;
  
  private List<MutableInt> bucketSizes;
  
  private MultipleOutputs<IntArrayWritable,IntMatrixWritable> mos;
  private int numberOfMappers;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
    numberOfMappers = conf.getInt(NUMBER_OF_MAPPERS_KEY, 1);
    bucketSizes = newArrayListWithCapacity(numberOfMappers);
    for (int i = 0; i < numberOfMappers; i++) {
      bucketSizes.add(new MutableInt());
    }
    
    mos = new MultipleOutputs<IntArrayWritable,IntMatrixWritable>(context);
    
  }
  
  @Override
  public void reduce(Text prefix, Iterable<IntArrayWritable> values, Context context) throws IOException,
      InterruptedException {
    
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
      }
      
      IntWritable[] newTidArray;
      if (tidList[mapperId] == null) {
        newTidArray = new IntWritable[w.length - 2];
        for (int i = 2; i < w.length; i++) {
          newTidArray[i - 2] = (IntWritable) w[i];
        }
      } else {
        // This is the case when the buffer size is exceeded in the mapper.
        final int oldSize = tidList[mapperId].get().length;
        int newSize = oldSize + w.length - 2;
        newTidArray = Arrays.copyOf((IntWritable[]) tidList[mapperId].get(), newSize);
        for (int i = 0; i < w.length - 2; i++) {
          newTidArray[oldSize + i] = (IntWritable) w[i + 2];
        }
      }
      tidList[mapperId] = new IntArrayWritable(newTidArray);
    }
    
    int totalTids = 0;
    for (Iterator<IntArrayWritable[]> it = map.values().iterator(); it.hasNext();) {
      IntArrayWritable[] tidLists = it.next();
      int itemSupport = 0;
      for (IntArrayWritable tidList : tidLists) {
        itemSupport += tidList.get().length;
      }
      if (itemSupport >= minSup) {
        totalTids += itemSupport;
      } else {
        it.remove();
      }
    }
    if (totalTids > 0) {
      assignToBucket(prefix, map, totalTids);
    }
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
  }
  
  private void assignToBucket(Text key, Map<Integer,IntArrayWritable[]> map, int totalTids) throws IOException,
      InterruptedException {
    int lowestBucket = getLowestBucket();
    if (!checkLowestBucket(lowestBucket, totalTids)) {
      bucketSizes.add(new MutableInt());
      lowestBucket = bucketSizes.size() - 1;
    }
    bucketSizes.get(lowestBucket).add(totalTids);
    
    String baseOutputPath = "bucket-" + lowestBucket;
    mos.write(IntArrayWritable.of(key.toString()), EmptyImw, baseOutputPath);
    for (Entry<Integer,IntArrayWritable[]> entry : map.entrySet()) {
      IntArrayWritable owKey = IntArrayWritable.of(entry.getKey());
      IntMatrixWritable owValue = new IntMatrixWritable(entry.getValue());
      mos.write(owKey, owValue, baseOutputPath);
    }
    mos.write(EmptyIaw, EmptyImw, baseOutputPath);
  }
  
  private static boolean checkLowestBucket(int lowestBucket, int totalTids) {
    return (lowestBucket + totalTids) <= MAX_NUMBER_OF_TIDS;
  }
  
  private int getLowestBucket() {
    double min = Integer.MAX_VALUE;
    int id = -1;
    int ix = 0;
    for (MutableInt bucketSize : bucketSizes) {
      int bs = bucketSize.intValue();
      if (bs < min) {
        min = bs;
        id = ix;
      }
      ix++;
    }
    return id;
  }
  
}