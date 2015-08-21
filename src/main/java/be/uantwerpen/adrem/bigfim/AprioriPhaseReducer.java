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

import static be.uantwerpen.adrem.disteclat.DistEclatDriver.OShortFIs;
import static be.uantwerpen.adrem.hadoop.util.Tools.createPath;
import static be.uantwerpen.adrem.hadoop.util.Tools.getJobAbsoluteOutputDir;
import static be.uantwerpen.adrem.util.FIMOptions.MIN_SUP_KEY;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Integer.parseInt;
import static java.lang.Math.max;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import be.uantwerpen.adrem.hadoop.util.Tools.NameStartsWithFilter;

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
 * Text           Iterable<Text>
 * (Prefix)       (<Item + Support in sub databases>)
 * ""             <"1 2", "1 2", "1 1", "2 1", "2 2", "2 2", "3 1", "3 1">
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
 * Text           Iterable<Text>
 * (Prefix)       (<Support in sub databases>)
 * "1"            <"2 1", "2 2", "2 1", "3 1">
 * "2"            <"3 1", "3 1">
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
    
    getBaseDirs(context);
    tgIndex = getLargestIndex(conf, new Path(createPath(baseDir, "tg" + aprioriPhase)), "trieGroup", 1) + 1;
    fisIndex = getLargestIndex(conf, new Path(createPath(baseDir, OShortFIs)), "fis-" + aprioriPhase, 2) + 1;
    baseOutputPathFis = createPath(baseDir, OShortFIs, "fis-" + aprioriPhase + "-" + fisIndex);
    
    mos = new MultipleOutputs<Text,Writable>(context);
  }
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Map<String,MutableInt> supports = getSupports(values);
    removeLowSupports(supports);
    
    if (supports.isEmpty()) {
      return;
    }
    
    String prefix = key.toString();
    
    writeShortFis(prefix, supports);
    if (!supports.isEmpty()) {
      writeTrieGroup(prefix, supports);
      updatePGInfo(prefix, supports);
    }
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    updatePGCounters(context);
    mos.close();
  }
  
  private void getBaseDirs(Context context) {
    try {
      String dir = getJobAbsoluteOutputDir(context);
      baseDir = dir.isEmpty() ? "tmp" : dir;
      
      Path path = new Path(context.getConfiguration().get("mapred.output.dir"));
      FileSystem fs = path.getFileSystem(context.getConfiguration());
      
      if (fs.getFileStatus(path) != null) {
        aprioriPhase = fs.getFileStatus(path).getPath().getName().split("-")[0].substring(2);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private int getLargestIndex(Configuration conf, Path path, String prefix, int index) {
    int largestIx = -1;
    try {
      FileSystem fs = path.getFileSystem(conf);
      for (FileStatus file : fs.listStatus(path, new NameStartsWithFilter(prefix))) {
        largestIx = max(largestIx, parseInt(file.getPath().getName().split("-")[index]));
      }
    } catch (NumberFormatException e) {} catch (IOException e) {}
    return largestIx;
  }
  
  private Map<String,MutableInt> getSupports(Iterable<Text> values) {
    Map<String,MutableInt> supports = newHashMap();
    for (Text extensionAndSupport : values) {
      String[] split = extensionAndSupport.toString().split(" ");
      getFromMap(supports, split[0]).add(parseInt(split[1]));
    }
    return supports;
  }
  
  private static MutableInt getFromMap(Map<String,MutableInt> map, String key) {
    MutableInt i = map.get(key);
    if (i == null) {
      i = new MutableInt();
      map.put(key, i);
    }
    return i;
  }
  
  private void removeLowSupports(Map<String,MutableInt> supports) {
    for (String extension : newHashSet(supports.keySet())) {
      if (supports.get(extension).intValue() < minSup) {
        supports.remove(extension);
      }
    }
  }
  
  private void writeTrieGroup(String prefix, Map<String,MutableInt> supports) throws IOException, InterruptedException {
    String baseOutputPath = createPath(baseDir, "tg" + aprioriPhase,
        "trieGroup-" + (prefix.isEmpty() ? 0 : getOutputDirIx(prefix, supports)));
    for (Entry<String,MutableInt> entry : supports.entrySet()) {
      String itemset = prefix.isEmpty() ? entry.getKey() : prefix + " " + entry.getKey();
      mos.write(new Text(itemset), new Text(entry.getValue().intValue() + ""), baseOutputPath);
    }
  }
  
  private void writeShortFis(String prefix, Map<String,MutableInt> supports) throws IOException, InterruptedException {
    StringBuilder builder = new StringBuilder();
    if (!prefix.isEmpty()) {
      builder.append(prefix.replace(" ", "|"));
      builder.append("|");
    }
    for (Entry<String,MutableInt> entry : supports.entrySet()) {
      builder.append(entry.getKey());
      builder.append("(");
      builder.append(entry.getValue().intValue());
      builder.append(")$");
    }
    mos.write(new Text("" + supports.size()), new Text(builder.substring(0, builder.length() - 1)), baseOutputPathFis);
  }
  
  private int getOutputDirIx(String prefix, Map<String,MutableInt> supports) {
    int size = getMaxNumberOfCandidates(supports.size());
    if (currTrieGroupSize != 0 && currTrieGroupSize + size > MAXCANDIDATESSIZE) {
      currTrieGroupSize = 0;
      tgIndex++;
    }
    currTrieGroupSize += size;
    return tgIndex;
  }
  
  private void updatePGInfo(String prefix, Map<String,MutableInt> supports) {
    int totSupport = 0;
    for (MutableInt support : supports.values()) {
      totSupport += support.intValue();
    }
    getFromMap(map, prefix).add(totSupport);
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
  
  private int getMaxNumberOfCandidates(int n) {
    int max = 0;
    for (int i = n - 1; i > 0; i--) {
      max += i;
    }
    return max;
  }
}