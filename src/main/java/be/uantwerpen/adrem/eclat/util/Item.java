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
package be.uantwerpen.adrem.eclat.util;

/**
 * Representation of an item in the dataset. It contains the id, the support and the tid list.
 */
public class Item {
  
  public final int id;
  public final int support;
  final TidList tidList;
  
  public Item(int id, int support, int[] tids) {
    this(id, support, new int[][] {tids});
  }
  
  public Item(int id, int support, int[][] tids) {
    this(id, support, new TidList(tids));
  }
  
  public Item(int id, int support, TidList tids) {
    this.id = id;
    this.support = support;
    this.tidList = tids;
  }
  
  /**
   * Returns the tid list (i.e., the transaction identifiers containing the item) as an array of integers.
   * 
   * @return the tid list
   */
  public TidList getTids() {
    return tidList;
  }
  
  @Override
  public String toString() {
    return id + " (" + support + ")" + " [" + tidList.size() + "]";
  }
  
  @Override
  public int hashCode() {
    return id;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Item other = (Item) obj;
    if (id != other.id) return false;
    if (support != other.support) return false;
    if (!tidList.equals(other.tidList)) return false;
    return true;
  }
  
}