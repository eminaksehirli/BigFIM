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

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.easymock.EasyMock.createMock;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.easymock.EasyMock;
import org.junit.Test;

import ua.ac.be.fpm.FIMTestCase;
import ua.ac.be.fpm.bigfim.AprioriPhaseReducer;
import ua.ac.be.fpm.bigfim.ComputeTidListReducer;

public class AprioriPhaseReducerTest extends FIMTestCase {
  
  @Test
  public void reduce_With_Input_Empty_MinSup_1() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1"), createList(new int[] {}), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_1() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("2"), new Text("7"));
    ctx.write(new Text("3"), new Text("11"));
    ctx.write(new Text("4"), new Text("1"));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1"), createList(1, 2, 3, 4), ctx);
    reducer.reduce(new Text("2"), createList(5, 2), ctx);
    reducer.reduce(new Text("3"), createList(2, 4, 5), ctx);
    reducer.reduce(new Text("4"), createList(1), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_5() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("2"), new Text("7"));
    ctx.write(new Text("3"), new Text("11"));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 5);
    
    reducer.reduce(new Text("1"), createList(1, 2, 3, 4), ctx);
    reducer.reduce(new Text("2"), createList(5, 2), ctx);
    reducer.reduce(new Text("3"), createList(2, 4, 5), ctx);
    reducer.reduce(new Text("4"), createList(1), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_10() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("3"), new Text("11"));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 10);
    
    reducer.reduce(new Text("1"), createList(1, 2, 3, 4), ctx);
    reducer.reduce(new Text("2"), createList(5, 2), ctx);
    reducer.reduce(new Text("3"), createList(2, 4, 5), ctx);
    reducer.reduce(new Text("4"), createList(1), ctx);
    
    EasyMock.verify(ctx);
  }
  
  private Iterable<IntWritable> createList(int... is) {
    List<IntWritable> list = newArrayListWithCapacity(is.length);
    for (int i : is) {
      list.add(new IntWritable(i));
    }
    return list;
  }
  
  @Test
  public void numberOfPGCounter_One_Group() throws IOException, InterruptedException, NoSuchFieldException,
      IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMock(Counter.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("2"), new Text("7"));
    ctx.write(new Text("3"), new Text("11"));
    ctx.write(new Text("4"), new Text("1"));
    
    pgCounter.setValue(1);
    
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfPrefixGroups")).andReturn(pgCounter);
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfLargePrefixGroups")).andReturn(
        new Counters().findCounter("AprioriPhase", "NumberOfLargePrefixGroups"));
    
    EasyMock.replay(ctx, pgCounter);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1"), createList(1, 2, 3, 4), ctx);
    reducer.reduce(new Text("2"), createList(5, 2), ctx);
    reducer.reduce(new Text("3"), createList(2, 4, 5), ctx);
    reducer.reduce(new Text("4"), createList(1), ctx);
    
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
  }
  
  @Test
  public void numberOfPGCounter_Multiple_Groups() throws IOException, InterruptedException, NoSuchFieldException,
      IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMock(Counter.class);
    
    ctx.write(new Text("1 2"), new Text("10"));
    ctx.write(new Text("1 3"), new Text("7"));
    ctx.write(new Text("1 4"), new Text("11"));
    ctx.write(new Text("1 5"), new Text("1"));
    
    ctx.write(new Text("2 3"), new Text("8"));
    ctx.write(new Text("2 4"), new Text("9"));
    ctx.write(new Text("2 5"), new Text("6"));
    
    ctx.write(new Text("4 5"), new Text("10"));
    ctx.write(new Text("4 6"), new Text("9"));
    
    pgCounter.setValue(3);
    
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfPrefixGroups")).andReturn(pgCounter);
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfLargePrefixGroups")).andReturn(
        new Counters().findCounter("AprioriPhase", "NumberOfLargePrefixGroups"));
    
    EasyMock.replay(ctx, pgCounter);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1 2"), createList(1, 2, 3, 4), ctx);
    reducer.reduce(new Text("1 3"), createList(2, 5), ctx);
    reducer.reduce(new Text("1 4"), createList(2, 4, 5), ctx);
    reducer.reduce(new Text("1 5"), createList(1), ctx);
    
    reducer.reduce(new Text("2 3"), createList(1, 3, 4), ctx);
    reducer.reduce(new Text("2 4"), createList(2, 3, 4), ctx);
    reducer.reduce(new Text("2 5"), createList(1, 5), ctx);
    
    reducer.reduce(new Text("4 5"), createList(5, 5), ctx);
    reducer.reduce(new Text("4 6"), createList(3, 3, 3), ctx);
    
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
  }
  
  @Test
  public void numberOfLargePGCounter_One_Group_Not_Large() throws IOException, InterruptedException,
      NoSuchFieldException, IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMock(Counter.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("2"), new Text("7"));
    ctx.write(new Text("3"), new Text("11"));
    ctx.write(new Text("4"), new Text("1"));
    
    pgCounter.setValue(0);
    
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfPrefixGroups")).andReturn(
        new Counters().findCounter("AprioriPhase", "NumberOfPrefixGroups"));
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfLargePrefixGroups")).andReturn(pgCounter);
    
    EasyMock.replay(ctx, pgCounter);
    
    int tmp = ComputeTidListReducer.MAX_NUMBER_OF_TIDS;
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = 50;
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1"), createList(1, 2, 3, 4), ctx);
    reducer.reduce(new Text("2"), createList(5, 2), ctx);
    reducer.reduce(new Text("3"), createList(2, 4, 5), ctx);
    reducer.reduce(new Text("4"), createList(1), ctx);
    
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
    
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = tmp;
  }
  
  @Test
  public void numberOfLargePGCounter_One_Group_Is_Large() throws IOException, InterruptedException,
      NoSuchFieldException, IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter pgCounter = createMock(Counter.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("2"), new Text("7"));
    ctx.write(new Text("3"), new Text("11"));
    ctx.write(new Text("4"), new Text("1"));
    
    pgCounter.setValue(1);
    
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfPrefixGroups")).andReturn(
        new Counters().findCounter("AprioriPhase", "NumberOfPrefixGroups"));
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfLargePrefixGroups")).andReturn(pgCounter);
    
    EasyMock.replay(ctx, pgCounter);
    
    int tmp = ComputeTidListReducer.MAX_NUMBER_OF_TIDS;
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = 10;
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1"), createList(1, 2, 3, 4), ctx);
    reducer.reduce(new Text("2"), createList(5, 2), ctx);
    reducer.reduce(new Text("3"), createList(2, 4, 5), ctx);
    reducer.reduce(new Text("4"), createList(1), ctx);
    
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, pgCounter);
    
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = tmp;
  }
  
  @Test
  public void numberOfLargePGCounter_Multiple_Groups() throws IOException, InterruptedException, NoSuchFieldException,
      IllegalAccessException {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    Counter lpgCounter = createMock(Counter.class);
    
    ctx.write(new Text("1 2"), new Text("10"));
    ctx.write(new Text("1 3"), new Text("7"));
    ctx.write(new Text("1 4"), new Text("11"));
    ctx.write(new Text("1 5"), new Text("1"));
    
    ctx.write(new Text("2 3"), new Text("8"));
    ctx.write(new Text("2 4"), new Text("9"));
    ctx.write(new Text("2 5"), new Text("6"));
    
    ctx.write(new Text("4 5"), new Text("10"));
    ctx.write(new Text("4 6"), new Text("9"));
    
    lpgCounter.setValue(2);
    
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfPrefixGroups")).andReturn(
        new Counters().findCounter("AprioriPhase", "NumberOfPrefixGroups"));
    EasyMock.expect(ctx.getCounter("AprioriPhase", "NumberOfLargePrefixGroups")).andReturn(lpgCounter);
    
    EasyMock.replay(ctx, lpgCounter);
    
    int tmp = ComputeTidListReducer.MAX_NUMBER_OF_TIDS;
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = 20;
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1 2"), createList(1, 2, 3, 4), ctx);
    reducer.reduce(new Text("1 3"), createList(2, 5), ctx);
    reducer.reduce(new Text("1 4"), createList(2, 4, 5), ctx);
    reducer.reduce(new Text("1 5"), createList(1), ctx);
    
    reducer.reduce(new Text("2 3"), createList(1, 3, 4), ctx);
    reducer.reduce(new Text("2 4"), createList(2, 3, 4), ctx);
    reducer.reduce(new Text("2 5"), createList(1, 5), ctx);
    
    reducer.reduce(new Text("4 5"), createList(5, 5), ctx);
    reducer.reduce(new Text("4 6"), createList(3, 3, 3), ctx);
    
    reducer.cleanup(ctx);
    
    EasyMock.verify(ctx, lpgCounter);
    
    ComputeTidListReducer.MAX_NUMBER_OF_TIDS = tmp;
  }
}
