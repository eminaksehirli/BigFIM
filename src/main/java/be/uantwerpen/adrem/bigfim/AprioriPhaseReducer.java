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

import static be.uantwerpen.adrem.util.FIMOptions.MIN_SUP_KEY;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Reducer class for Apriori phase of BigFIM. This reducer combines the supports of length+1 candidates from different
 * mappers and writes the sets with their cumulated supports when frequent.
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
 * Text           Iterable<IntWritable>
 * (Itemset)      (<Support in sub databases>)
 * 1              <2,2,1>
 * 2              <1,2,2>
 * 3              <1,1>
 * 
 * Output:
 * Text           Writable
 * (Itemset)      (Total support)
 * "1"            "5"
 * "2"            "5"
 * "3"            "2"
 * 
 * 
 * 
 * Example Phase=2, MinSup=1:
 * ==========================
 * 
 * Input:
 * Text           Iterable<IntWritable>
 * (Itemset)      (<Support in sub databases>)
 * 1 2            <1,2,1>
 * 1 3            <1>
 * 2 3            <1,1>
 * 
 * Output:
 * Text           Writable
 * (Itemset)      (Total support)
 * "1 2"          "4"
 * "1 3"          "1"
 * "2 3"          "2"
 * }
 * </pre>
 */
public class AprioriPhaseReducer extends Reducer<Text,IntWritable,Text,Writable> {
  
  private static class Itemset {
    String itemset;
    int support;
    
    public Itemset(String itemset, int support) {
      this.itemset = itemset;
      this.support = support;
    }
  }
  
  private static final String EMPTY_KEY = "";
  
  public static final String COUNTER_GROUPNAME = "AprioriPhase";
  public static final String COUNTER_NRPREFIXGROUPS = "NumberOfPrefixGroups";
  public static final String COUNTER_NRLARGEPREFIXGROUPS = "NumberOfLargePrefixGroups";
  
  public static final int MAXCANDIDATESSIZE = 100;
  
  private final Map<String,MutableInt> map = newHashMap();
  
  private int currTrieGroupSize;
  
  private int minSup;
  
  private String baseTGDir;
  private int tgIndex;
  
  private String currPrefix = "NOPREFIXYET";
  private final List<Itemset> itemsetsForPrefix = newArrayList();
  
  private MultipleOutputs<Text,Writable> mos;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    currTrieGroupSize = 0;
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
    
    getBaseTGDir(conf);
    getTgIndex(conf);
    
    mos = new MultipleOutputs<Text,Writable>(context);
  }
  
  private void getTgIndex(Configuration conf) {
    try {
      Path path = new Path(baseTGDir);
      FileSystem fs = path.getFileSystem(new Configuration());
      
      if (!fs.exists(path)) {
        tgIndex = 0;
        return;
      }
      
      int largestIx = 0;
      for (FileStatus file : fs.listStatus(path)) {
        String tmp = file.getPath().toString();
        if (!tmp.contains("trieGroup")) {
          continue;
        }
        tmp = tmp.substring(tmp.lastIndexOf('/'), tmp.length());
        int ix = Integer.parseInt(tmp.split("-")[1]);
        largestIx = Math.max(largestIx, ix);
        tgIndex += 1;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private void getBaseTGDir(Configuration conf) {
    try {
      Path path = new Path(conf.get("mapred.output.dir"));
      FileSystem fs = path.getFileSystem(new Configuration());
      
      String dir = fs.listStatus(path)[0].getPath().toString();
      dir = dir.substring(dir.indexOf('/'), dir.length());// strip of file:/
      dir = dir.substring(0, dir.lastIndexOf("/_temporary"));// strip of _temporary
      
      String aprioriPhase = dir.substring(dir.lastIndexOf('/') + 1, dir.length());
      int end = aprioriPhase.indexOf('-');
      end = end > 0 ? end : aprioriPhase.length();
      aprioriPhase = aprioriPhase.substring(2, end);
      
      baseTGDir = dir.substring(0, dir.lastIndexOf('/')) + "/tg" + aprioriPhase;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sup = getSupport(values);
    if (sup < minSup) {
      return;
    }
    
    String itemset = key.toString();
    String prefix = getPrefix(itemset);
    
    if (isSingleton(prefix)) {
      String baseOutputPath = baseTGDir + "/trieGroup-" + 0;
      mos.write(key, new Text(sup + ""), baseOutputPath);
    } else {
      if (!prefix.equals(currPrefix)) {
        String baseOutputPath = "fis";
        if (itemsetsForPrefix.size() > 1) {
          int size = StringUtils.countMatches(itemset, " ") + 1;
          currTrieGroupSize += (size * itemsetsForPrefix.size());
          if (currTrieGroupSize > MAXCANDIDATESSIZE) {
            currTrieGroupSize = 0;
            tgIndex++;
          }
          baseOutputPath = baseTGDir + "/trieGroup-" + tgIndex;
        }
        for (Itemset itemsetToWrite : itemsetsForPrefix) {
          mos.write(new Text(itemsetToWrite.itemset), new Text(itemsetToWrite.support + ""), baseOutputPath);
        }
        itemsetsForPrefix.clear();
      }
      currPrefix = prefix;
      itemsetsForPrefix.add(new Itemset(itemset, sup));
    }
    updatePGInfo(itemset, sup);
  }
  
  private static String getPrefix(String itemset) {
    int ixEnd = itemset.lastIndexOf(' ');
    if (ixEnd == -1) {
      return "";
    }
    return itemset.substring(0, ixEnd);
  }
  
  private boolean isSingleton(String prefix) {
    return prefix.equals("");
  }
  
  private int getSupport(Iterable<IntWritable> values) {
    int sup = 0;
    for (IntWritable localSup : values) {
      sup += localSup.get();
    }
    return sup;
  }
  
  private void updatePGInfo(String key, int sup) {
    int ix = key.lastIndexOf(' ');
    if (ix == -1) {
      getFromMap(EMPTY_KEY).add(sup);
      return;
    }
    getFromMap(key.substring(0, ix)).add(sup);
  }
  
  private MutableInt getFromMap(String key) {
    MutableInt i = map.get(key);
    if (i == null) {
      i = new MutableInt();
      map.put(key, i);
    }
    return i;
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    updatePGCounters(context);
    mos.close();
  }
  
  private void updatePGCounters(Context context) {
    int nrPG = map.size();
    int largePG = 0;
    for (Entry<String,MutableInt> m : map.entrySet()) {
      if (m.getValue().intValue() > ComputeTidListReducer.MAX_NUMBER_OF_TIDS) {
        largePG++;
      }
    }
    
    context.getCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS).setValue(nrPG);
    context.getCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS).setValue(largePG);
  }
}