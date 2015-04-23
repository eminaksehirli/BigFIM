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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.Integer.parseInt;
import static java.lang.Integer.valueOf;
import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;
import static be.uantwerpen.adrem.disteclat.DistEclatDriver.OSingletonsOrder;
import static be.uantwerpen.adrem.disteclat.DistEclatDriver.OSingletonsTids;
import static be.uantwerpen.adrem.util.FIMOptions.MIN_SUP_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.PREFIX_LENGTH_KEY;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import be.uantwerpen.adrem.eclat.EclatMiner;
import be.uantwerpen.adrem.eclat.util.Item;
import be.uantwerpen.adrem.eclat.util.PrefixItemTIDsReporter;
import be.uantwerpen.adrem.eclat.util.SetReporter;
import be.uantwerpen.adrem.hadoop.util.IntMatrixWritable;

/**
 * Mapper class for the second cycle of DistEclat. It receives a list of singletons for which it has to create X-FIs
 * seeds by growing the lattice tree downwards. The ordering is retrieved through the distributed cache. *
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
 * LongWritable   Text
 * (Offset)       (Assigned singletons)
 * 0              "\t1"                     | Mapper 1
 * 
 * 3              "\t2"                     | Mapper 2
 * 
 * 6              "\t3"                     | Mapper 3
 * 
 * Output:
 * Text           IntMatrixWritable
 * (Prefix)       ([[Item][Partial TIDs]])
 * "1"            [[2],[0],[0,1],[0]]       | Mapper 1
 * "1"            [[3],[],[0],[]]          | Mapper 1
 * 
 * "2"            [[3],[],[0],[1]]          | Mapper 2
 * }
 * </pre>
 */
public class PrefixComputerMapper extends Mapper<LongWritable,Text,Text,IntMatrixWritable> {
  
  private List<Item> singletons;
  private Map<Integer,Integer> orderMap;
  private int minSup;
  private int prefixLength;
  
  @Override
  public void setup(Context context) throws IOException {
    try {
      Configuration conf = context.getConfiguration();
      
      minSup = conf.getInt(MIN_SUP_KEY, -1);
      prefixLength = conf.getInt(PREFIX_LENGTH_KEY, 1);
      
      Path[] localCacheFiles = getLocalCacheFiles(conf);
      
      for (Path path : localCacheFiles) {
        String pathString = path.toString();
        if (pathString.contains(OSingletonsTids)) {
          System.out.println("[PrefixComputerMapper]: Reading singletons");
          singletons = readTidLists(conf, path);
        } else if (pathString.contains(OSingletonsOrder)) {
          System.out.println("[PrefixComputerMapper]: Reading singleton orders");
          orderMap = readSingletonsOrder(path);
        }
      }
      
      sortSingletons();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] split = value.toString().split("\t");
    String items = split[1];
    
    // if the prefix length is 1, just report the singletons, otherwise use
    // Eclat to find X-FIs seeds
    EclatMiner miner = new EclatMiner();
    SetReporter reporter = new PrefixItemTIDsReporter(context, prefixLength, singletons, orderMap);
    
    miner.setSetReporter(reporter);
    miner.setMaxSize(prefixLength);
    
    for (String itemStr : items.split(" ")) {
      final int itemIx = orderMap.get(Integer.valueOf(itemStr));
      final Item item = singletons.get(itemIx);
      assert (item.id == parseInt(itemStr));
      List<Item> extensions = singletons.subList(itemIx + 1, singletons.size());
      miner.mineRecByPruning(item, extensions, minSup);
    }
  }
  
  /**
   * Sorts the singletons using the orderings retrieved from file
   */
  private void sortSingletons() {
    Collections.sort(singletons, new Comparator<Item>() {
      @Override
      public int compare(Item o1, Item o2) {
        Integer o1Rank = orderMap.get(o1.id);
        Integer o2Rank = orderMap.get(o2.id);
        return o1Rank.compareTo(o2Rank);
      }
    });
  }
  
  /**
   * Reads the singleton items with their tid lists from the specified file.
   * 
   * @param conf
   * @param path
   * @return
   * @throws IOException
   * @throws URISyntaxException
   */
  private static List<Item> readTidLists(Configuration conf, Path path) throws IOException, URISyntaxException {
    SequenceFile.Reader r = new SequenceFile.Reader(FileSystem.get(new URI("file:///"), conf), path, conf);
    
    List<Item> items = newArrayList();
    
    IntWritable key = new IntWritable();
    IntMatrixWritable value = new IntMatrixWritable();
    
    while (r.next(key, value)) {
      final int[][] tids = value.toIntMatrix();
      int support = 0;
      
      for (int[] partTids : tids) {
        if (partTids != null) {
          support += partTids.length;
        }
      }
      
      items.add(new Item(key.get(), support, tids));
    }
    r.close();
    
    return items;
  }
  
  /**
   * Reads the singletons ordering from file.
   * 
   * @param path
   * @return
   * @throws IOException
   */
  private static Map<Integer,Integer> readSingletonsOrder(Path path) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
    
    String order = reader.readLine().trim();
    reader.close();
    
    Map<Integer,Integer> orderMap = newHashMap();
    String[] split = order.split(" ");
    int ix = 0;
    for (String item : split) {
      orderMap.put(valueOf(item), ix++);
    }
    return orderMap;
  }
}