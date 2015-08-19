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

import static be.uantwerpen.adrem.bigfim.AprioriPhaseReducer.COUNTER_GROUPNAME;
import static be.uantwerpen.adrem.bigfim.AprioriPhaseReducer.COUNTER_NRLARGEPREFIXGROUPS;
import static be.uantwerpen.adrem.bigfim.AprioriPhaseReducer.COUNTER_NRPREFIXGROUPS;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.easymock.EasyMock.createMock;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.easymock.EasyMock;
import org.junit.Test;

import be.uantwerpen.adrem.FIMTestCase;

public class AprioriPhaseReducerTest extends FIMTestCase {
  
  // @formatter:off
  private final static Object[][] dataOnePrefix = new Object[][] {
    new Object[] {"1", 10},
    new Object[] {"2", 7},
    new Object[] {"3", 11},
    new Object[] {"4", 1},
  };
  
  
  private final static Object[][] dataTwoPrefix = new Object[][] {
    new Object[] {"1 2", 10},
    new Object[] {"1 3", 7},
    new Object[] {"1 4", 11},
    new Object[] {"1 5", 1},
    new Object[] {"2 3", 8},
    new Object[] {"2 4", 9},
    new Object[] {"2 5", 6},
    new Object[] {"4 5", 10},
    new Object[] {"4 6", 9}
  };
  // @formatter:on
  
  private void addToOutput(MultipleOutputs<Text,Text> mos, String prefix, int support)
      throws IOException, InterruptedException {
    mos.write(new Text(prefix), new Text("" + support), "base/tg0/trieGroup-0");
    mos.write(new Text(prefix), new Text("" + support), "fis");
  }
  
  private MultipleOutputs<Text,Text> createMockMos(int minSup, Object[][] dataOnePrefix)
      throws IOException, InterruptedException {
    MultipleOutputs<Text,Text> mos = createMock(MultipleOutputs.class);
    
    for (Object[] prefixAndSupport : dataOnePrefix) {
      if ((Integer) prefixAndSupport[1] >= minSup) {
        addToOutput(mos, (String) prefixAndSupport[0], (Integer) prefixAndSupport[1]);
      }
    }
    mos.close();
    return mos;
  }
  
  private MultipleOutputs<Text,Text> createMultipleOutputsOnePrefix(int minSup)
      throws IOException, InterruptedException {
    return createMockMos(minSup, dataOnePrefix);
  }
  
  private MultipleOutputs<Text,Text> createMultipleOutputsTwoPrefix(int minSup)
      throws IOException, InterruptedException {
    return createMockMos(minSup, dataTwoPrefix);
    
  }
  
  private AprioriPhaseReducer createAprioriPhaseReducer(int minSup, MultipleOutputs<Text,Text> mos)
      throws NoSuchFieldException, IllegalAccessException {
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", minSup);
    setField(reducer, "baseDir", "base");
    setField(reducer, "tgIndex", 0);
    setField(reducer, "fisIndex", 0);
    setField(reducer, "mos", mos);
    setField(reducer, "aprioriPhase", "0");
    setField(reducer, "baseOutputPathFis", "fis");
    return reducer;
  }
  
  private Iterable<Text> createReducerInput() {
    return createList("1 1", "1 2", "1 3", "1 4", "2 5", "2 2", "3 2", "3 4", "3 5", "4 1");
  }
  
  private Iterable<Text> createReducerInputTwoPrefix(String prefix) {
    if (prefix.equals("1")) {
      return createList("2 1", "2 2", "2 3", "2 4", "3 2", "3 5", "4 2", "4 4", "4 5", "5 1");
    } else if (prefix.equals("2")) {
      return createList("3 1", "3 3", "3 4", "4 2", "4 3", "4 4", "5 1", "5 5");
    } else if (prefix.equals("4")) {
      return createList("5 5", "5 5", "6 3", "6 3", "6 3");
    }
    return newArrayList();
  }
  
  private Counter createMockCounter(int value) {
    Counter counter = createMock(Counter.class);
    counter.setValue(value);
    return counter;
  }
  
  @Test
  public void reduce_With_Input_Empty_MinSup_1() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 1);
    
    EasyMock.replay(ctx);
    
    reducer.reduce(new Text(""), createList(new String[] {}), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_1() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    AprioriPhaseReducer reducer = createAprioriPhaseReducer(1, createMultipleOutputsOnePrefix(1));
    
    EasyMock.replay(ctx, createMultipleOutputsOnePrefix(1));
    
    reducer.reduce(new Text(""), createReducerInput(), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_5() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    AprioriPhaseReducer reducer = createAprioriPhaseReducer(5, createMultipleOutputsOnePrefix(5));
    
    EasyMock.replay(ctx);
    
    reducer.reduce(new Text(""), createReducerInput(), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_10() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    AprioriPhaseReducer reducer = createAprioriPhaseReducer(10, createMultipleOutputsOnePrefix(10));
    
    EasyMock.replay(ctx);
    EasyMock.verify(ctx);
  }
  
  private Iterable<IntWritable> createList(int... is) {
    List<IntWritable> list = newArrayListWithCapacity(is.length);
    for (int i : is) {
      list.add(new IntWritable(i));
    }
    return list;
  }
  
  private Iterable<Text> createList(String... ts) {
    List<Text> list = newArrayListWithCapacity(ts.length);
    for (String t : ts) {
      list.add(new Text(t));
    }
    return list;
  }
  
  @Test
  public void numberOfPGCounter_One_Group()
      throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMockCounter(1);
    
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS)).andReturn(pgCounter).anyTimes();
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS))
        .andReturn(new Counters().findCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS)).anyTimes();
        
    EasyMock.replay(ctx, pgCounter);
    
    AprioriPhaseReducer reducer = createAprioriPhaseReducer(1, createMultipleOutputsOnePrefix(1));
    reducer.reduce(new Text(""), createReducerInput(), ctx);
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
  }
  
  @Test
  public void numberOfPGCounter_Multiple_Groups()
      throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMockCounter(3);
    
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS)).andReturn(pgCounter).anyTimes();
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS))
        .andReturn(new Counters().findCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS)).anyTimes();
        
    EasyMock.replay(ctx, pgCounter);
    
    AprioriPhaseReducer reducer = createAprioriPhaseReducer(1, createMultipleOutputsTwoPrefix(1));
    for (String prefix : new String[] {"1", "2", "4"}) {
      reducer.reduce(new Text(prefix), createReducerInputTwoPrefix(prefix), ctx);
    }
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
  }
  
  @Test
  public void numberOfLargePGCounter_One_Group_Not_Large()
      throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMockCounter(0);
    
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS))
        .andReturn(new Counters().findCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS)).anyTimes();
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS)).andReturn(pgCounter).anyTimes();
    
    EasyMock.replay(ctx, pgCounter);
    
    int tmp = ComputeTidListReducer.MAX_NUMBER_OF_TIDS;
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = 50;
    
    AprioriPhaseReducer reducer = createAprioriPhaseReducer(1, createMultipleOutputsOnePrefix(1));
    reducer.reduce(new Text(""), createReducerInput(), ctx);
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
    
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = tmp;
  }
  
  @Test
  public void numberOfLargePGCounter_One_Group_Is_Large()
      throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMockCounter(1);
    
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS))
        .andReturn(new Counters().findCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS)).anyTimes();
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS)).andReturn(pgCounter).anyTimes();
    
    EasyMock.replay(ctx, pgCounter);
    
    int tmp = ComputeTidListReducer.MAX_NUMBER_OF_TIDS;
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = 10;
    
    AprioriPhaseReducer reducer = createAprioriPhaseReducer(1, createMultipleOutputsOnePrefix(1));
    reducer.reduce(new Text(""), createReducerInput(), ctx);
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
    
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = tmp;
  }
  
  @Test
  public void numberOfLargePGCounter_Multiple_Groups()
      throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMockCounter(2);
    
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS))
        .andReturn(new Counters().findCounter(COUNTER_GROUPNAME, COUNTER_NRPREFIXGROUPS)).anyTimes();
    EasyMock.expect(ctx.getCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS)).andReturn(pgCounter).anyTimes();
    
    EasyMock.replay(ctx, pgCounter);
    
    int tmp = ComputeTidListReducer.MAX_NUMBER_OF_TIDS;
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = 20;
    
    AprioriPhaseReducer reducer = createAprioriPhaseReducer(1, createMultipleOutputsTwoPrefix(1));
    for (String prefix : new String[] {"1", "2", "4"}) {
      reducer.reduce(new Text(prefix), createReducerInputTwoPrefix(prefix), ctx);
    }
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
    
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = tmp;
  }
}
