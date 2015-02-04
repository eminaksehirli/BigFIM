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
package be.uantwerpen.adrem.bigfim;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;
import static be.uantwerpen.adrem.bigfim.Tools.convertLineToSet;
import static be.uantwerpen.adrem.bigfim.Tools.createCandidates;
import static be.uantwerpen.adrem.bigfim.Tools.getSingletonsFromSets;
import static be.uantwerpen.adrem.bigfim.Tools.readItemsetsFromFile;
import static be.uantwerpen.adrem.util.FIMOptions.DELIMITER_KEY;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for Apriori phase of BigFIM. Each mapper receives a sub part (horizontal cut) of the dataset and
 * combines a list of base itemsets in candidates of length+1 for its sub database. The latter are counted in the map
 * function. If no base itemsets are specified, all singletons are counted. The size of the sub part is depending on the
 * number of mappers and the size of the original dataset.
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
 * Text           IntWritable
 * (Itemset)      (Support)
 * "1"            2                         | Mapper 1
 * "2"            1                         | Mapper 1
 * 
 * "1"            2                         | Mapper 2
 * "2"            2                         | Mapper 2
 * "3"            1                         | Mapper 2
 * 
 * "1"            1                         | Mapper 3
 * "2"            2                         | Mapper 3
 * "3"            1                         | Mapper 3
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
 * 0              "1 2"             | Mapper 1
 * 4              "1"               | Mapper 1
 * 
 * 6              "1 2 3"           | Mapper 2
 * 12             "1 2"             | Mapper 2
 * 
 * 16             "1 2"             | Mapper 3
 * 20             "2 3"             | Mapper 3
 * 
 * Output:
 * Text           IntWritable
 * (Itemset)      (Support)
 * "1 2"          1                 | Mapper 1
 * 
 * "1 2"          2                 | Mapper 2
 * "1 3"          1                 | Mapper 2
 * "2 3"          1                 | Mapper 2
 * 
 * "1 2"          1                 | Mapper 3
 * "2 3"          1                 | Mapper 3
 * }
 * </pre>
 */
public class AprioriPhaseMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
  
  static class Trie {
    final int id;
    int support;
    final Map<Integer,Trie> children;
    
    public Trie(int id) {
      this.id = id;
      this.support = 0;
      children = newHashMap();
    }
    
    public void incrementSupport() {
      support += 1;
    }
    
    public Trie getChild(int id) {
      Trie child = children.get(id);
      if (child == null) {
        child = new Trie(id);
        children.put(id, child);
      }
      return child;
    }
    
    @Override
    public String toString() {
      return "[" + id + "(" + support + "):" + children + "]";
    }
    
  }
  
  private Set<Integer> singletons;
  private Trie countTrie;
  
  private int phase = 1;
  private String delimiter;
  
  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    delimiter = conf.get(DELIMITER_KEY, " ");
    
    Path[] localCacheFiles = getLocalCacheFiles(conf);
    countTrie = new Trie(-1);
    if (localCacheFiles != null) {
      String filename = localCacheFiles[0].toString();
      List<SortedSet<Integer>> itemsets = readItemsetsFromFile(filename);
      Collection<SortedSet<Integer>> candidates = createCandidates(itemsets);
      singletons = getSingletonsFromSets(candidates);
      
      phase = itemsets.get(0).size() + 1;
      
      countTrie = initializeCountTrie(candidates);
      
      System.out.println("Singletons: " + singletons.size());
      System.out.println("Words: " + candidates.size());
    }
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    List<Integer> items = convertLineToSet(line, phase == 1, singletons, delimiter);
    incrementSubSets(items);
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    recReport(context, new StringBuilder(), countTrie);
  }
  
  private static Trie initializeCountTrie(Collection<SortedSet<Integer>> candidates) {
    Trie countTrie = new Trie(-1);
    for (SortedSet<Integer> candidate : candidates) {
      Trie trie = countTrie;
      Iterator<Integer> it = candidate.iterator();
      while (it.hasNext()) {
        trie = trie.getChild(it.next());
      }
    }
    return countTrie;
  }
  
  private void recReport(Context context, StringBuilder builder, Trie trie) throws IOException, InterruptedException {
    int length = builder.length();
    for (Entry<Integer,Trie> entry : trie.children.entrySet()) {
      Trie recTrie = entry.getValue();
      builder.append(recTrie.id + " ");
      if (recTrie.children.isEmpty()) {
        if (recTrie.support != 0) {
          Text key = new Text(builder.substring(0, builder.length() - 1));
          IntWritable value = new IntWritable(recTrie.support);
          context.write(key, value);
        }
      } else {
        recReport(context, builder, recTrie);
      }
      builder.setLength(length);
    }
  }
  
  private void incrementSubSets(List<Integer> items) {
    if (items.size() < phase) {
      return;
    }
    
    if (phase == 1) {
      for (int i = 0; i < items.size(); i++) {
        Trie recTrie = countTrie.getChild(items.get(i));
        recTrie.incrementSupport();
      }
      return;
    }
    
    doRecursiveCount(items, 0, countTrie);
  }
  
  private void doRecursiveCount(List<Integer> items, int ix, Trie trie) {
    for (int i = ix; i < items.size(); i++) {
      Trie recTrie = trie.children.get(items.get(i));
      if (recTrie != null) {
        if (recTrie.children.isEmpty()) {
          recTrie.incrementSupport();
        } else {
          doRecursiveCount(items, i + 1, recTrie);
        }
      }
    }
  }
}