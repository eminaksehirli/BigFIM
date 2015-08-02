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
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

//TODO UPDATE THIS EXPLANATION
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
public class AprioriPhaseReducer extends Reducer<Text,Text,Text,Writable> {
  
  public static final String COUNTER_GROUPNAME = "AprioriPhase";
  public static final String COUNTER_NRPREFIXGROUPS = "NumberOfPrefixGroups";
  public static final String COUNTER_NRLARGEPREFIXGROUPS = "NumberOfLargePrefixGroups";
  
  public static final int MAXCANDIDATESSIZE = 10000;
  
  private final Map<String,MutableInt> map = newHashMap();
  
  private int currTrieGroupSize = 0;
  
  private int minSup;
  
  private String baseDir;
  private int tgIndex;
  private int fisIndex;
  
  private MultipleOutputs<Text,Writable> mos;
  private String aprioriPhase;
  private String baseOutputPathFis;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
    
    getBaseDirs(conf);
    getTgIndex(conf);
    getFisIndex(conf);
    baseOutputPathFis = baseDir + "/shortfis/fis-" + aprioriPhase + "-" + fisIndex;
    
    mos = new MultipleOutputs<Text,Writable>(context);
  }
  
  private void getTgIndex(Configuration conf) {
    try {
      Path path = new Path(baseDir + "/tg" + aprioriPhase);
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
        tgIndex = Math.max(largestIx, ix) + 1;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private void getFisIndex(Configuration conf) {
    try {
      Path path = new Path(baseDir + "/smallfis");
      FileSystem fs = path.getFileSystem(new Configuration());
      
      if (!fs.exists(path)) {
        fisIndex = 0;
        return;
      }
      
      int largestIx = 0;
      for (FileStatus file : fs.listStatus(path)) {
        String tmp = file.getPath().toString();
        if (!tmp.contains("fis")) {
          continue;
        }
        tmp = tmp.substring(tmp.lastIndexOf('/'), tmp.length());
        int ix = Integer.parseInt(tmp.split("-")[2]);
        fisIndex = Math.max(largestIx, ix) + 1;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private void getBaseDirs(Configuration conf) {
    try {
      Path path = new Path(conf.get("mapred.output.dir"));
      FileSystem fs = path.getFileSystem(new Configuration());
      
      String dir = fs.listStatus(path)[0].getPath().toString();
      dir = dir.substring(dir.indexOf('/'), dir.length());// strip of file:/
      dir = dir.substring(0, dir.lastIndexOf("/_temporary"));// strip of _temporary
      
      aprioriPhase = dir.substring(dir.lastIndexOf('/') + 1, dir.length());
      int end = aprioriPhase.indexOf('-');
      end = end > 0 ? end : aprioriPhase.length();
      aprioriPhase = aprioriPhase.substring(2, end);
      
      baseDir = dir.substring(0, dir.lastIndexOf('/'));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Map<String,MutableInt> supports = getSupports(values);
    removeLowSupports(supports);
    
    if (supports.isEmpty()) {
      return;
    }
    
    String prefix = key.toString();
    
    String baseOutputPath = null;
    if (supports.size() > 1) {
      baseOutputPath = baseDir + "/tg" + aprioriPhase + "/trieGroup-"
          + (prefix.isEmpty() ? 0 : getOutputDirIx(prefix, supports));
      updatePGInfo(prefix, supports);
    }
    
    for (Entry<String,MutableInt> entry : supports.entrySet()) {
      String itemset = prefix.isEmpty() ? entry.getKey() : prefix + " " + entry.getKey();
      Text textItemset = new Text(itemset);
      Text textSupport = new Text(entry.getValue().intValue() + "");
      if (baseOutputPath != null) {
        mos.write(textItemset, textSupport, baseOutputPath);
      }
      mos.write(textItemset, textSupport, baseOutputPathFis);
    }
  }
  
  private int getOutputDirIx(String prefix, Map<String,MutableInt> supports) {
    int size = (StringUtils.countMatches(prefix, " ") + 2) * supports.size();
    currTrieGroupSize += size;
    if (currTrieGroupSize > MAXCANDIDATESSIZE) {
      currTrieGroupSize = size;
      tgIndex++;
    }
    return tgIndex;
  }
  
  private Map<String,MutableInt> getSupports(Iterable<Text> values) {
    Map<String,MutableInt> supports = newHashMap();
    for (Text extensionAndSupport : values) {
      String[] split = extensionAndSupport.toString().split(" ");
      String extension = split[0];
      int localSup = Integer.parseInt(split[1]);
      MutableInt support = supports.get(extension);
      if (support == null) {
        support = new MutableInt(0);
        supports.put(extension, support);
      }
      support.add(localSup);
    }
    return supports;
  }
  
  private void removeLowSupports(Map<String,MutableInt> supports) {
    for (String extension : newHashSet(supports.keySet())) {
      if (supports.get(extension).intValue() < minSup) {
        supports.remove(extension);
      }
    }
  }
  
  private void updatePGInfo(String prefix, Map<String,MutableInt> supports) {
    int totSupport = 0;
    for (MutableInt support : supports.values()) {
      totSupport += support.intValue();
    }
    getFromMap(prefix).add(totSupport);
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