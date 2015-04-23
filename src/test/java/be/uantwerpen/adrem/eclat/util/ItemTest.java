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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import be.uantwerpen.adrem.eclat.util.Item;
import be.uantwerpen.adrem.eclat.util.TidList;

public class ItemTest {
  
  @Test
  public void getFreq_Freq_TidsSize_Equal() {
    Item item = new Item(1, 5, new int[] {0, 1, 2, 3, 4});
    assertEquals(5, item.support);
    
    item = new Item(1, 2, new int[] {0, 1});
    assertEquals(2, item.support);
  }
  
  @Test
  public void getFreq_Freq_TidsSize_Not_Equal() {
    Item item = new Item(1, 5, new int[] {0, 1});
    assertEquals(5, item.support);
    
    item = new Item(1, 2, new int[] {0, 1, 2, 3});
    assertEquals(2, item.support);
  }
  
  @Test
  public void getTids_Freq_TidsSize_Equal() {
    Item item = new Item(1, 5, new int[][] {{0, 1, 2, 3, 4}});
    assertEquals(new TidList(new int[][] {{0, 1, 2, 3, 4}}), item.getTids());
    
    item = new Item(1, 2, new int[] {0, 1});
    assertEquals(new TidList(new int[][] {{0, 1}}), item.getTids());
  }
  
  @Test
  public void item_Can_Be_Constructed_With_Different_Types_Of_TidList() {
    final TidList expected = new TidList(new int[][] {{0, 1}});
    
    Item item = new Item(1, 2, new int[] {0, 1});
    assertEquals(expected, item.getTids());
    
    item = new Item(1, 2, new int[][] {{0, 1}});
    assertEquals(expected, item.getTids());
    
    item = new Item(1, 2, new TidList(new int[][] {{0, 1}}));
    assertEquals(expected, item.getTids());
  }
  
  @Test
  public void getTids_Freq_TidsSize_Not_Equal() {
    Item item = new Item(1, 5, new int[] {0, 1});
    assertEquals(new TidList(new int[][] {{0, 1}}), item.getTids());
  }
}
