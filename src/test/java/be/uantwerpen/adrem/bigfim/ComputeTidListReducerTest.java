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
import static ua.ac.be.fpm.bigfim.ComputeTidListMapperTest.newIAW;
import static ua.ac.be.fpm.hadoop.util.IntArrayWritable.EmptyIaw;
import static ua.ac.be.fpm.hadoop.util.IntMatrixWritable.EmptyImw;
import static ua.ac.be.fpm.util.FIMOptions.MIN_SUP_KEY;
import static ua.ac.be.fpm.util.FIMOptions.NUMBER_OF_MAPPERS_KEY;
import static ua.ac.be.fpm.util.FIMOptions.SUBDB_SIZE;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.easymock.EasyMock;
import org.junit.Test;

import ua.ac.be.fpm.FIMTestCase;
import ua.ac.be.fpm.bigfim.ComputeTidListReducer;
import ua.ac.be.fpm.hadoop.util.IntArrayWritable;
import ua.ac.be.fpm.hadoop.util.IntMatrixWritable;

public class ComputeTidListReducerTest extends FIMTestCase {
  
  private static Iterable<IntArrayWritable> createTestInput_1Item() {
    List<IntArrayWritable> list = newArrayListWithCapacity(2);
    
    list.add(newIAW(0, 0, 0, 1, 2, 4, 7, 9));
    list.add(newIAW(1, 0, 0, 1, 2, 3, 5, 6, 8));
    
    return list;
  }
  
  private static Iterable<IntArrayWritable> createTestInput_NItems() {
    List<IntArrayWritable> list = newArrayListWithCapacity(6);
    
    list.add(newIAW(0, 0, 0, 1, 2, 4, 7, 9));
    list.add(newIAW(1, 0, 0, 1, 2, 3, 5, 6, 8));
    
    list.add(newIAW(0, 1, 1, 2, 3));
    list.add(newIAW(1, 1, 4, 5, 6));
    
    list.add(newIAW(0, 3, 4, 7, 9));
    list.add(newIAW(1, 3, 4, 7, 9));
    
    return list;
  }
  
  private static Iterable<IntArrayWritable> createTestInput_NItems2() {
    List<IntArrayWritable> list = newArrayListWithCapacity(4);
    
    list.add(newIAW(0, 1, 1, 4, 7, 8));
    list.add(newIAW(1, 1, 1, 5, 6, 8));
    
    list.add(newIAW(0, 2, 3, 5, 7));
    list.add(newIAW(1, 2, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    
    return list;
  }
  
  private Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.setInt(MIN_SUP_KEY, 1);
    conf.setInt(NUMBER_OF_MAPPERS_KEY, 2);
    conf.setInt(SUBDB_SIZE, 10);
    return conf;
  }
  
  @Test
  public void One_PG_One_Item() throws Exception {
    MultipleOutputs<IntArrayWritable,IntMatrixWritable> mos = createMock(MultipleOutputs.class);
    
    mos.write(newIAW(1), EmptyImw, "bucket-0");
    mos.write(newIAW(0), new IntMatrixWritable(newIAW(0, 1, 2, 4, 7, 9), newIAW(0, 1, 2, 3, 5, 6, 8)), "bucket-0");
    mos.write(EmptyIaw, EmptyImw, "bucket-0");
    mos.close();
    
    Reducer.Context ctx = createMock(Reducer.Context.class);
    EasyMock.expect(ctx.getConfiguration()).andReturn(createConfiguration()).anyTimes();
    EasyMock.expect(ctx.getTaskAttemptID()).andReturn(new TaskAttemptID()).anyTimes();
    
    EasyMock.replay(ctx, mos);
    
    ComputeTidListReducer reducer = new ComputeTidListReducer();
    reducer.setup(ctx);
    setField(reducer, "mos", mos);
    
    reducer.reduce(new Text("1"), createTestInput_1Item(), ctx);
    reducer.cleanup(ctx);
    
    EasyMock.verify(mos);
  }
  
  @Test
  public void One_PG_N_Items() throws Exception {
    MultipleOutputs<IntArrayWritable,IntMatrixWritable> mos = createMock(MultipleOutputs.class);
    
    mos.write(newIAW(1), EmptyImw, "bucket-0");
    mos.write(newIAW(0), new IntMatrixWritable(newIAW(0, 1, 2, 4, 7, 9), newIAW(0, 1, 2, 3, 5, 6, 8)), "bucket-0");
    mos.write(newIAW(1), new IntMatrixWritable(newIAW(1, 2, 3), newIAW(4, 5, 6)), "bucket-0");
    mos.write(newIAW(3), new IntMatrixWritable(newIAW(4, 7, 9), newIAW(4, 7, 9)), "bucket-0");
    mos.write(EmptyIaw, EmptyImw, "bucket-0");
    mos.close();
    
    Reducer.Context ctx = createMock(Reducer.Context.class);
    EasyMock.expect(ctx.getConfiguration()).andReturn(createConfiguration()).anyTimes();
    EasyMock.expect(ctx.getTaskAttemptID()).andReturn(new TaskAttemptID()).anyTimes();
    
    EasyMock.replay(ctx, mos);
    
    ComputeTidListReducer reducer = new ComputeTidListReducer();
    reducer.setup(ctx);
    setField(reducer, "mos", mos);
    
    reducer.reduce(new Text("1"), createTestInput_NItems(), ctx);
    reducer.cleanup(ctx);
    
    EasyMock.verify(mos);
  }
  
  @Test
  public void N_PG_N_Items() throws Exception {
    MultipleOutputs<IntArrayWritable,IntMatrixWritable> mos = createMock(MultipleOutputs.class);
    
    mos.write(newIAW(1), EmptyImw, "bucket-0");
    mos.write(newIAW(0), new IntMatrixWritable(newIAW(0, 1, 2, 4, 7, 9), newIAW(0, 1, 2, 3, 5, 6, 8)), "bucket-0");
    mos.write(newIAW(1), new IntMatrixWritable(newIAW(1, 2, 3), newIAW(4, 5, 6)), "bucket-0");
    mos.write(newIAW(3), new IntMatrixWritable(newIAW(4, 7, 9), newIAW(4, 7, 9)), "bucket-0");
    mos.write(EmptyIaw, EmptyImw, "bucket-0");
    
    mos.write(newIAW(2), EmptyImw, "bucket-1");
    mos.write(newIAW(1), new IntMatrixWritable(newIAW(1, 4, 7, 8), newIAW(1, 5, 6, 8)), "bucket-1");
    mos.write(newIAW(2), new IntMatrixWritable(newIAW(3, 5, 7), newIAW(1, 2, 3, 4, 5, 6, 7, 8, 9)), "bucket-1");
    mos.write(EmptyIaw, EmptyImw, "bucket-1");
    mos.close();
    
    Reducer.Context ctx = createMock(Reducer.Context.class);
    EasyMock.expect(ctx.getConfiguration()).andReturn(createConfiguration()).anyTimes();
    EasyMock.expect(ctx.getTaskAttemptID()).andReturn(new TaskAttemptID()).anyTimes();
    
    EasyMock.replay(ctx, mos);
    
    ComputeTidListReducer reducer = new ComputeTidListReducer();
    reducer.setup(ctx);
    setField(reducer, "mos", mos);
    
    reducer.reduce(new Text("1"), createTestInput_NItems(), ctx);
    reducer.reduce(new Text("2"), createTestInput_NItems2(), ctx);
    reducer.cleanup(ctx);
    
    EasyMock.verify(mos);
  }
}
