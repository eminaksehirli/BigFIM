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
import be.uantwerpen.adrem.util.ItemSetTrie;

public class AprioriPhaseMapperTest extends FIMTestCase {
  
  private static String[] data = new String[] {"1 2 3 4", "2 3 4", "1 3 5", "1", "3 4 5", "1 3 4 5", "2 5", "1 3 4"};
  
  private static Set<Integer> create_Set_1() {
    Set<Integer> set = newHashSet();
    set.add(1);
    set.add(2);
    set.add(3);
    set.add(4);
    set.add(5);
    return set;
  }
  
  private static ItemSetTrie.SupportCountItemsetTrie create_Count_Trie_Empty() {
    return new ItemSetTrie.SupportCountItemsetTrie(-1);
  }
  
  private static ItemSetTrie.SupportCountItemsetTrie create_Count_Trie_Not_Empty() {
    ItemSetTrie.SupportCountItemsetTrie trie = new ItemSetTrie.SupportCountItemsetTrie(-1);
    
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
  
  @Test
  public void phase_1_No_Input() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = createMapper(1, create_Count_Trie_Empty());
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_1_With_Input() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    ctx.write(new Text(""), new Text("1 5"));
    ctx.write(new Text(""), new Text("2 3"));
    ctx.write(new Text(""), new Text("3 6"));
    ctx.write(new Text(""), new Text("4 5"));
    ctx.write(new Text(""), new Text("5 4"));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = createMapper(1, create_Count_Trie_Empty());
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_No_Input() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = createMapper(2, create_Count_Trie_Not_Empty());
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_No_Input_Empty_Count_Trie() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = createMapper(2, create_Count_Trie_Empty());
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_With_Input() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    ctx.write(new Text("1"), new Text("2 1"));
    ctx.write(new Text("1"), new Text("3 4"));
    ctx.write(new Text("1"), new Text("4 3"));
    ctx.write(new Text("2"), new Text("3 2"));
    ctx.write(new Text("2"), new Text("5 1"));
    ctx.write(new Text("3"), new Text("4 5"));
    ctx.write(new Text("4"), new Text("5 2"));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = createMapper(2, create_Count_Trie_Not_Empty());
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_With_Input_Empty_Count_Trie() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = createMapper(2, create_Count_Trie_Empty());
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  public static AprioriPhaseMapper createMapper(int phase, final ItemSetTrie.SupportCountItemsetTrie trie)
      throws Exception {
    AprioriPhaseMapper mapper = new AprioriPhaseMapper();
    setField(mapper, "singletons", create_Set_1());
    setField(mapper, "phase", phase);
    setField(mapper, "countTrie", trie);
    setField(mapper, "delimiter", " ");
    return mapper;
  }
}
