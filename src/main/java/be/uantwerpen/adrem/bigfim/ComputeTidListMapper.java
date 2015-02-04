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

import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;
import static ua.ac.be.fpm.bigfim.Tools.convertLineToSet;
import static ua.ac.be.fpm.bigfim.Tools.createCandidates;
import static ua.ac.be.fpm.bigfim.Tools.readItemsetsFromFile;
import static ua.ac.be.fpm.util.FIMOptions.DELIMITER_KEY;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ua.ac.be.fpm.hadoop.util.IntArrayWritable;

/**
 * Mapper class for the second phase of BigFIM. Each mapper receives a sub part (horizontal cut) of the dataset and
 * computes partial tidlists of the given itemsets in the sub database. The size of the sub database depends on the
 * number of mappers and the size of the original dataset.
 * 
 * <pre>
 * {@code
 *  Original Input Per Mapper:
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
 * LongWritable   Text
 * (Offset)       (Transaction)
 * 0              "1 2"                     | Mapper 1
 * 4              "1"                       | Mapper 1
 * 
 * 6              "1 2 3"                   | Mapper 2
 * 12             "1 2"                     | Mapper 2
 * 
 * 16             "1 2"                     | Mapper 3
 * 20             "2 3"                     | Mapper 3
 * 
 * Output:
 * Text           IntArrayWritable
 * (Prefix)       ([Mapper ID, Item, Partial Tids...])
 * ""             [1,1,0,1]                 | Mapper 1
 * ""             [1,2,0]                   | Mapper 1
 * 
 * ""             [2,1,0,1]                 | Mapper 2
 * ""             [2,2,0,1]                 | Mapper 2
 * ""             [2,3,0]                   | Mapper 2
 * 
 * ""             [3,1,0]                   | Mapper 3
 * ""             [3,2,0,1]                 | Mapper 3
 * ""             [3,3,1]                   | Mapper 3
 * 
 * 
 * 
 * Example Phase=2, MinSup=1:
 * ==========================
 * 
 * Itemsets:
 * 1
 * 2
 * 3
 * 
 * Itemsets of length+1:
 * 1 2
 * 1 3
 * 2 3
 * 
 * Input:
 * LongWritable   Text
 * (Offset)       (Transaction)
 * 0              "1 2"                     | Mapper 1
 * 4              "1"                       | Mapper 1
 * 
 * 6              "1 2 3"                   | Mapper 2
 * 12             "1 2"                     | Mapper 2
 * 
 * 16             "1 2"                     | Mapper 3
 * 20             "2 3"                     | Mapper 3
 * 
 * Output:
 * Text           IntArrayWritable
 * (Prefix)       ([Mapper ID, Item, Partial Tids...])
 * "1"            [1,2,0]                   | Mapper 1
 * 
 * "1"            [2,2,0,1]                 | Mapper 2
 * "1"            [2,3,0]                   | Mapper 2
 * "2"            [2,3,0]                   | Mapper 2
 * 
 * "1"            [3,2,0]                   | Mapper 3
 * "2"            [3,3,1]                   | Mapper 3
 * }
 * </pre>
 */
public class ComputeTidListMapper extends Mapper<LongWritable,Text,Text,IntArrayWritable> {
  
  private static int TIDS_BUFFER_SIZE = 100000;
  
  static class Trie {
    public int id;
    public List<Integer> tids;
    public Map<Integer,Trie> children;
    
    public Trie(int id) {
      this.id = id;
      tids = newLinkedList();
      children = newHashMap();
    }
    
    public Trie getChild(int id) {
      Trie child = children.get(id);
      if (child == null) {
        child = new Trie(id);
        children.put(id, child);
      }
      return child;
    }
    
    public void addTid(int tid) {
      tids.add(tid);
    }
  }
  
  private final IntArrayWritable iaw;
  
  private Set<Integer> singletons;
  private Trie countTrie;
  
  private int phase = 1;
  
  private int counter;
  private int tidCounter = 0;
  private int id;
  
  private String delimiter;
  
  public ComputeTidListMapper() {
    iaw = new IntArrayWritable();
    singletons = null;
    countTrie = new Trie(-1);
    counter = 0;
    id = -1;
    phase = 1;
  }
  
  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    delimiter = conf.get(DELIMITER_KEY, " ");
    
    Path[] localCacheFiles = getLocalCacheFiles(conf);
    
    if (localCacheFiles != null) {
      String filename = localCacheFiles[0].toString();
      List<SortedSet<Integer>> itemsets = readItemsetsFromFile(filename);
      Collection<SortedSet<Integer>> candidates = createCandidates(itemsets);
      singletons = Tools.getSingletonsFromSets(candidates);
      
      countTrie = initializeCountTrie(candidates);
      
      phase = itemsets.get(0).size() + 1;
    }
    id = context.getTaskAttemptID().getTaskID().getId();
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    List<Integer> items = convertLineToSet(line, phase == 1, singletons, delimiter);
    reportItemTids(context, items);
    counter++;
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    if (tidCounter != 0) {
      doRecursiveReport(context, new StringBuilder(), 0, countTrie);
    }
  }
  
  private static Trie initializeCountTrie(Collection<SortedSet<Integer>> candidates) {
    Trie countTrie = new Trie(-1);
    for (SortedSet<Integer> candidate : candidates) {
      Trie trie = countTrie;
      for (int item : candidate) {
        trie = trie.getChild(item);
      }
    }
    return countTrie;
  }
  
  private IntWritable[] createIntWritableWithIdSet(int numberOfTids) {
    IntWritable[] iw = new IntWritable[numberOfTids + 2];
    iw[0] = new IntWritable(id);
    return iw;
  }
  
  private void reportItemTids(Context context, List<Integer> items) throws IOException, InterruptedException {
    if (items.size() < phase) {
      return;
    }
    
    if (phase == 1) {
      for (Integer item : items) {
        countTrie.getChild(item).addTid(counter);
        tidCounter++;
      }
    } else {
      doRecursiveTidAdd(context, items, 0, countTrie);
    }
    if (tidCounter >= TIDS_BUFFER_SIZE) {
      System.out.println("Tids buffer reached, reporting " + tidCounter + " partial tids");
      doRecursiveReport(context, new StringBuilder(), 0, countTrie);
      tidCounter = 0;
    }
  }
  
  private void doRecursiveTidAdd(Context context, List<Integer> items, int ix, Trie trie) throws IOException,
      InterruptedException {
    for (int i = ix; i < items.size(); i++) {
      Trie recTrie = trie.children.get(items.get(i));
      if (recTrie != null) {
        if (recTrie.children.isEmpty()) {
          recTrie.addTid(counter);
          tidCounter++;
        } else {
          doRecursiveTidAdd(context, items, i + 1, recTrie);
        }
      }
    }
  }
  
  private void doRecursiveReport(Context context, StringBuilder builder, int depth, Trie trie) throws IOException,
      InterruptedException {
    int length = builder.length();
    for (Trie recTrie : trie.children.values()) {
      if (recTrie != null) {
        if (depth + 1 == phase) {
          if (recTrie.tids.isEmpty()) {
            continue;
          }
          Text key = new Text(builder.substring(0, Math.max(0, builder.length() - 1)));
          IntWritable[] iw = createIntWritableWithIdSet(recTrie.tids.size());
          int i1 = 1;
          iw[i1++] = new IntWritable(recTrie.id);
          for (int tid : recTrie.tids) {
            iw[i1++] = new IntWritable(tid);
          }
          iaw.set(iw);
          context.write(key, iaw);
          recTrie.tids.clear();
        } else {
          builder.append(recTrie.id + " ");
          doRecursiveReport(context, builder, depth + 1, recTrie);
        }
      }
      builder.setLength(length);
    }
  }
}