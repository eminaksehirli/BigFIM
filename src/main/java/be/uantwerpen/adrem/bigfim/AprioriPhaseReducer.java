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
import static be.uantwerpen.adrem.util.FIMOptions.MIN_SUP_KEY;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

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
  
  private static final String EMPTY_KEY = "";
  
  public static final String COUNTER_GROUPNAME = "AprioriPhase";
  public static final String COUNTER_NRPREFIXGROUPS = "NumberOfPrefixGroups";
  public static final String COUNTER_NRLARGEPREFIXGROUPS = "NumberOfLargePrefixGroups";
  
  private final Map<String,MutableInt> map = newHashMap();
  
  private int minSup;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
  }
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sup = 0;
    for (IntWritable localSup : values) {
      sup += localSup.get();
    }
    
    if (sup >= minSup) {
      context.write(key, new Text(sup + ""));
      updatePGInfo(key.toString(), sup);
    }
    
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
  public void cleanup(Context context) {
    updatePGCounters(context);
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