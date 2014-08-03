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
package ua.ac.be.fpm.eclat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for Eclat phase of BigFIM and DistEclat. It cumulates all itemsets counts per level and reports the
 * global level counts.
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
 * Text                   Iterable<LongWritable>
 * (Level)                (<Number Frequent itemsets in per level>)
 * "2"                    <1,2>
 * "3"                    <1>
 * 
 * Output                 LongWritable
 * (Level)                (Cumulative number of frequent itemsets per level)
 * "2"                    3
 * "3"                    1
 * "Total"                4
 * }
 * </pre>
 */
public class EclatMinerReducerSetCount extends Reducer<Text,LongWritable,Text,LongWritable> {
  
  private long total = 0;
  
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long levelTotal = 0;
    for (LongWritable lw : values) {
      levelTotal += lw.get();
    }
    total += levelTotal;
    context.write(key, new LongWritable(levelTotal));
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    System.out.println("Total: " + total);
    context.write(new Text("Total"), new LongWritable(total));
  }
}