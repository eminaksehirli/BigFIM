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
package ua.ac.be.fpm.disteclat;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static ua.ac.be.fpm.hadoop.util.IntArrayWritable.EmptyIaw;
import static ua.ac.be.fpm.hadoop.util.IntMatrixWritable.EmptyImw;
import static ua.ac.be.fpm.util.FIMOptions.MIN_SUP_KEY;
import static ua.ac.be.fpm.util.FIMOptions.NUMBER_OF_MAPPERS_KEY;
import static ua.ac.be.fpm.util.FIMOptions.OUTPUT_DIR_KEY;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import ua.ac.be.fpm.eclat.util.PrefixItemTIDsReporter;
import ua.ac.be.fpm.hadoop.util.IntArrayWritable;
import ua.ac.be.fpm.hadoop.util.IntMatrixWritable;

/**
 * Reducer class for the second cycle of DistEclat. It receives complete prefix groups and the items in the groups with
 * their respective ids. For each of the groups it assigns computation of the supersets (Eclat phase) to different
 * buckets (Mappers), such that a new group is assigned to the bucket with the lowest workload.
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
 * Example MinSup=1, K=2:
 * ======================
 * 
 * Input:
 * Text                   Iterable<IntMatrixWritable>
 * (Prefix)               (<[[Item], [Partial TIDs...]]>)
 * 1                      <[[2],[0],[0,1],[0]],[[3],[],[0],[]]>
 * 2                      <[[3],[],[0],[1]]>
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
public class PrefixComputerReducer extends Reducer<Text,IntMatrixWritable,IntArrayWritable,IntMatrixWritable> {
  
  // max = 1 gb
  private static int MAX_FILE_SIZE = 1000000000;
  private static int MAX_NUMBER_OF_TIDS = (int) ((MAX_FILE_SIZE / 4) * 0.7);
  
  private int minSup;
  
  private List<MutableInt> bucketSizes;
  
  private PrintStream shortFIsOut;
  private MultipleOutputs<IntArrayWritable,IntMatrixWritable> mos;
  
  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    
    createShortFIsFile(conf);
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
    int numberOfMappers = conf.getInt(NUMBER_OF_MAPPERS_KEY, 1);
    bucketSizes = newArrayListWithCapacity(numberOfMappers);
    for (int i = 0; i < numberOfMappers; i++) {
      bucketSizes.add(new MutableInt());
    }
    
    mos = new MultipleOutputs<IntArrayWritable,IntMatrixWritable>(context);
    
  }
  
  @Override
  public void reduce(Text prefix, Iterable<IntMatrixWritable> values, Context context) throws IOException,
      InterruptedException {
    
    if (prefix.equals(PrefixItemTIDsReporter.ShortKey)) {
      printShorts(values);
    }
    Map<Integer,IntArrayWritable[]> map = newHashMap();
    
    int size = 0;
    for (IntMatrixWritable value : values) {
      
      if (size > 0) {
        throw new IllegalStateException("More than one tid list for a prefix");
      }
      
      final Writable[] writables = value.get();
      IntArrayWritable[] iaws = (IntArrayWritable[]) writables;
      // This is a hack, first element of the array is a 1-length array that keeps the id of the item.
      final Writable[] itemNameArray = iaws[0].get();
      int item = ((IntWritable) itemNameArray[0]).get();
      
      IntArrayWritable[] newIaws = new IntArrayWritable[iaws.length - 1];
      for (int i = 1; i < iaws.length; i++) {
        newIaws[i - 1] = iaws[i];
      }
      map.put(item, newIaws);
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
  
  private void printShorts(Iterable<IntMatrixWritable> values) {
    for (IntMatrixWritable imw : values) {
      IntArrayWritable[] iaw = (IntArrayWritable[]) imw.get();
      final IntArrayWritable is = iaw[0];
      Writable[] itemset = is.get();
      shortFIsOut.print("1\t");
      for (Writable item : itemset) {
        shortFIsOut.print(((IntWritable) item).get() + " ");
      }
      
      final Writable[] support = iaw[1].get();
      shortFIsOut.println("(" + ((IntWritable) support[0]).get() + ")");
    }
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    shortFIsOut.flush();
    shortFIsOut.close();
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
  
  private void createShortFIsFile(Configuration conf) throws IOException {
    Path path = new Path(conf.getStrings(OUTPUT_DIR_KEY)[0] + Path.SEPARATOR + DistEclatDriver.OShortFIs);
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    shortFIsOut = new PrintStream(fs.create(path));
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