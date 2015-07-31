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
package be.uantwerpen.adrem.util;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

import java.util.List;
import java.util.Map;

public abstract class ItemSetTrie {
  
  public final int id;
  public final Map<Integer,ItemSetTrie> children;
  
  public ItemSetTrie(int id) {
    this.id = id;
    this.children = newHashMap();
  }
  
  public abstract void addTid(int tid);
  
  public abstract ItemSetTrie getChild(int id);
  
  public static class SupportCountItemsetTrie extends ItemSetTrie {
    
    public int support;
    
    public SupportCountItemsetTrie(int id) {
      super(id);
      this.support = 0;
    }
    
    @Override
    public ItemSetTrie getChild(int id) {
      ItemSetTrie child = children.get(id);
      if (child == null) {
        child = new SupportCountItemsetTrie(id);
        children.put(id, child);
      }
      return child;
    }
    
    @Override
    public void addTid(int tid) {
      support++;
    }
    
    @Override
    public String toString() {
      return "[" + id + "(support: " + support + "):" + children + "]";
    }
  }
  
  public static class TidListItemsetTrie extends ItemSetTrie {
    
    public List<Integer> tids;
    
    public TidListItemsetTrie(int id) {
      super(id);
      this.tids = newArrayList();
    }
    
    @Override
    public ItemSetTrie getChild(int id) {
      ItemSetTrie child = children.get(id);
      if (child == null) {
        child = new TidListItemsetTrie(id);
        children.put(id, child);
      }
      return child;
    }
    
    @Override
    public void addTid(int tid) {
      this.tids.add(tid);
    }
    
    @Override
    public String toString() {
      return "[" + id + "(current tid count: " + this.tids.size() + "):" + children + "]";
    }
  }
}
