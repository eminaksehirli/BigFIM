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

import static com.google.common.collect.Sets.newHashSet;
import static org.easymock.EasyMock.createMock;

import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.easymock.EasyMock;
import org.junit.Test;

import be.uantwerpen.adrem.FIMTestCase;
import be.uantwerpen.adrem.hadoop.util.IntArrayWritable;
import be.uantwerpen.adrem.hadoop.util.IntMatrixWritable;
import be.uantwerpen.adrem.util.ItemSetTrie;

public class ComputeTidListMapperTest extends FIMTestCase {
  
  private static String[] data = new String[] {"1 2 3 4", "2 3 4", "1 3 5", "1", "3 4 5", "1 3 4 5", "2 5", "1 3 4"};
  private static String[] data2 = new String[] {"2 3 4", "1 3 5", "1", "3 4 5", "1 3 4 5", "2 5", "1 3 4"};
  
  private static Set<Integer> create_Set_1() {
    Set<Integer> set = newHashSet();
    set.add(1);
    set.add(2);
    set.add(3);
    set.add(4);
    set.add(5);
    return set;
  }
  
  private static ItemSetTrie.TidListItemsetTrie create_Count_Trie_Not_Empty() {
    ItemSetTrie.TidListItemsetTrie trie = new ItemSetTrie.TidListItemsetTrie(-1);
    
    ItemSetTrie child1 = trie.getChild(1);
    child1.getChild(2);
    child1.getChild(3);
    child1.getChild(4);
    
    ItemSetTrie child2 = trie.getChild(2);
    child2.getChild(3);
    child2.getChild(5);
    
    ItemSetTrie child3 = trie.getChild(3);
    child3.getChild(4);
    
    ItemSetTrie child4 = trie.getChild(4);
    child4.getChild(5);
    
    return trie;
  }
  
  private static ItemSetTrie.TidListItemsetTrie create_Count_Trie_Not_Empty_2() {
    ItemSetTrie.TidListItemsetTrie trie = new ItemSetTrie.TidListItemsetTrie(-1);
    
    ItemSetTrie child1 = trie.getChild(1);
    child1.getChild(2);
    
    return trie;
  }
  
  public static IntArrayWritable newIAW(int... is) {
    return IntArrayWritable.of(is);
  }
  
  public static IntMatrixWritable newIMW(int... is) {
    return new IntMatrixWritable(newIAW(is));
  }
  
  @Test
  public void phase_1_No_Input() throws Exception {
    ComputeTidListMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    ComputeTidListMapper mapper = new ComputeTidListMapper();
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_1_With_Input() throws Exception {
    ComputeTidListMapper.Context ctx = createMock(Mapper.Context.class);
    
    ctx.write(new Text(""), newIAW(-1, 1, 0, 2, 3, 5, 7));
    ctx.write(new Text(""), newIAW(-1, 2, 0, 1, 6));
    ctx.write(new Text(""), newIAW(-1, 3, 0, 1, 2, 4, 5, 7));
    ctx.write(new Text(""), newIAW(-1, 4, 0, 1, 4, 5, 7));
    ctx.write(new Text(""), newIAW(-1, 5, 2, 4, 5, 6));
    
    EasyMock.replay(ctx);
    
    ComputeTidListMapper mapper = new ComputeTidListMapper();
    setField(mapper, "delimiter", " ");
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_No_Input() throws Exception {
    ComputeTidListMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    ComputeTidListMapper mapper = new ComputeTidListMapper();
    setField(mapper, "countTrie", create_Count_Trie_Not_Empty());
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_No_Input_Empty_Count_Trie() throws Exception {
    ComputeTidListMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    ComputeTidListMapper mapper = new ComputeTidListMapper();
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_With_Input() throws Exception {
    ComputeTidListMapper.Context ctx = createMock(Mapper.Context.class);
    
    ctx.write(new Text("1"), newIAW(-1, 2, 0));
    ctx.write(new Text("1"), newIAW(-1, 3, 0, 2, 5, 7));
    ctx.write(new Text("1"), newIAW(-1, 4, 0, 5, 7));
    ctx.write(new Text("2"), newIAW(-1, 3, 0, 1));
    ctx.write(new Text("2"), newIAW(-1, 5, 6));
    ctx.write(new Text("3"), newIAW(-1, 4, 0, 1, 4, 5, 7));
    ctx.write(new Text("4"), newIAW(-1, 5, 4, 5));
    
    EasyMock.replay(ctx);
    
    ComputeTidListMapper mapper = createMapper(2, create_Count_Trie_Not_Empty());
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_With_Input_2() throws Exception {
    ComputeTidListMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    ComputeTidListMapper mapper = createMapper(2, create_Count_Trie_Not_Empty_2());
    
    for (int i = 0; i < data2.length; i++) {
      mapper.map(new LongWritable(i), new Text(data2[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_With_Input_Empty_Count_Trie() throws Exception {
    ComputeTidListMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    ComputeTidListMapper mapper = createMapper(2, new ItemSetTrie.TidListItemsetTrie(-1));
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  private static ComputeTidListMapper createMapper(int phase, final ItemSetTrie.TidListItemsetTrie trie)
      throws Exception {
    ComputeTidListMapper mapper = new ComputeTidListMapper();
    setField(mapper, "singletons", create_Set_1());
    setField(mapper, "phase", phase);
    setField(mapper, "countTrie", trie);
    setField(mapper, "delimiter", " ");
    return mapper;
  }
}
