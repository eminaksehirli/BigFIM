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
package be.uantwerpen.adrem.eclat;

import org.apache.hadoop.io.LongWritable;

import be.uantwerpen.adrem.eclat.util.ItemsetLengthCountReporter;
import be.uantwerpen.adrem.eclat.util.SetReporter;

/**
 * Mapper class for the Eclat phase of BigFIM and DistEclat. This mapper mines the frequent itemsets for the specified
 * prefixes (subtree) and reports the number of frequent sets found per level.
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
 * IntArrayWritable       IntMatrixWritable
 * (Prefix Group or Item) (Partial TIDs)
 * [1]                    []                | Mapper 1
 * [2]                    [[0],[0,1],[0]]   | Mapper 1
 * [3]                    [[],[0],[]]       | Mapper 1
 * []                     []                | Mapper 1
 * 
 * [2]                    []                | Mapper 2
 * [3]                    [[],[0],[1]]      | Mapper 2
 * []                     []                | Mapper 2
 * 
 * Output:
 * Text                   LongWritable
 * (Level)                (Number Frequent itemsets in per level)
 * "2"                    2                 | Mapper 1
 * "3"                    1                 | Mapper 1
 * 
 * "2"                    1                 | Mapper 2
 * }
 * </pre>
 */
public class EclatMinerMapperSetCount extends EclatMinerMapperBase<LongWritable> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new ItemsetLengthCountReporter(context);
  }
}