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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static be.uantwerpen.adrem.bigfim.Tools.ItemDelimiter;
import static be.uantwerpen.adrem.bigfim.Tools.convertLineToSet;
import static be.uantwerpen.adrem.bigfim.Tools.createCandidates;
import static be.uantwerpen.adrem.bigfim.Tools.getSingletonsFromSets;
import static be.uantwerpen.adrem.bigfim.Tools.readItemsetsFromFile;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import org.junit.Test;

public class ToolsTest {
  
  private static String[] inputData = new String[] {"1 2 3 4\t8", "4 5 6 7\t9", "2 4 5 6\t1"};
  private static String[] inputLines = new String[] {"1 5 6", "4 6 7", "2 5 6"};
  
  private void writeToFile(File in, String[] lines) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(in));
    for (String line : lines) {
      writer.write(line + "\n");
    }
    writer.close();
  }
  
  private void testTheItemsets(List<SortedSet<Integer>> itemsets) {
    Iterator<SortedSet<Integer>> it = itemsets.iterator();
    Set<Integer> set;
    
    set = newHashSet(it.next());
    assertTrue(set.remove(1));
    assertTrue(set.remove(2));
    assertTrue(set.remove(3));
    assertTrue(set.remove(4));
    assertTrue(set.isEmpty());
    
    set = newHashSet(it.next());
    assertTrue(set.remove(4));
    assertTrue(set.remove(5));
    assertTrue(set.remove(6));
    assertTrue(set.remove(7));
    assertTrue(set.isEmpty());
    
    set = newHashSet(it.next());
    assertTrue(set.remove(2));
    assertTrue(set.remove(4));
    assertTrue(set.remove(5));
    assertTrue(set.remove(6));
    assertTrue(set.isEmpty());
    
    assertFalse(it.hasNext());
  }
  
  private void testTheSingletons(List<List<Integer>> sets) {
    Iterator<List<Integer>> it = sets.iterator();
    Set<Integer> set;
    
    set = newHashSet(it.next());
    assertTrue(set.remove(1));
    assertTrue(set.remove(5));
    assertTrue(set.remove(6));
    assertTrue(set.isEmpty());
    
    set = newHashSet(it.next());
    assertTrue(set.remove(4));
    assertTrue(set.remove(6));
    assertTrue(set.remove(7));
    assertTrue(set.isEmpty());
    
    set = newHashSet(it.next());
    assertTrue(set.remove(2));
    assertTrue(set.remove(5));
    assertTrue(set.remove(6));
    assertTrue(set.isEmpty());
    
    assertFalse(it.hasNext());
  }
  
  private void testTheSingletonsSubSet(List<List<Integer>> sets) {
    Iterator<List<Integer>> it = sets.iterator();
    Set<Integer> set;
    
    set = newHashSet(it.next());
    assertTrue(set.remove(6));
    assertTrue(set.isEmpty());
    
    set = newHashSet(it.next());
    assertTrue(set.remove(4));
    assertTrue(set.remove(6));
    assertTrue(set.remove(7));
    assertTrue(set.isEmpty());
    
    set = newHashSet(it.next());
    assertTrue(set.remove(6));
    assertTrue(set.isEmpty());
    
    assertFalse(it.hasNext());
  }
  
  private Set<SortedSet<Integer>> getItemsets() {
    Set<SortedSet<Integer>> itemsets = new HashSet<SortedSet<Integer>>();
    
    SortedSet<Integer> s1 = newTreeSet();
    s1.add(1);
    s1.add(5);
    s1.add(7);
    itemsets.add(s1);
    
    SortedSet<Integer> s2 = newTreeSet();
    s2.add(1);
    s2.add(5);
    s2.add(6);
    itemsets.add(s2);
    
    SortedSet<Integer> s3 = newTreeSet();
    s3.add(2);
    s3.add(4);
    s3.add(5);
    itemsets.add(s3);
    
    return itemsets;
  }
  
  private void testSingletons(Set<Integer> singletons) {
    Set<Integer> sing = newHashSet(singletons);
    assertTrue(sing.remove(1));
    assertTrue(sing.remove(2));
    assertTrue(sing.remove(4));
    assertTrue(sing.remove(5));
    assertTrue(sing.remove(6));
    assertTrue(sing.remove(7));
    assertTrue(sing.isEmpty());
  }
  
  private void testLengthPlusOneCandidates(Collection<SortedSet<Integer>> sets) {
    Iterator<SortedSet<Integer>> it = sets.iterator();
    Set<Integer> set;
    
    set = newHashSet(it.next());
    assertTrue(set.remove(1));
    assertTrue(set.remove(5));
    assertTrue(set.remove(6));
    assertTrue(set.remove(7));
    assertTrue(set.isEmpty());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void read_Itemsets() throws IOException {
    File in = File.createTempFile("read_Itemsets", ".txt");
    
    writeToFile(in, inputData);
    
    List<SortedSet<Integer>> itemsets = readItemsetsFromFile(in.getAbsolutePath());
    
    testTheItemsets(itemsets);
  }
  
  @Test
  public void convert_Level_One() {
    List<List<Integer>> sets = newLinkedList();
    for (String line : inputLines) {
      Set<Integer> set = newHashSet();
      sets.add(convertLineToSet(line, true, set, ItemDelimiter));
    }
    
    testTheSingletons(sets);
  }
  
  @Test
  public void convert_Level_One_Singletons_Correct() {
    List<List<Integer>> sets = newArrayListWithCapacity(inputLines.length);
    HashSet<Integer> singletons = newHashSet();
    singletons.add(1);
    singletons.add(2);
    singletons.add(4);
    singletons.add(5);
    singletons.add(6);
    singletons.add(7);
    
    for (String line : inputLines) {
      sets.add(convertLineToSet(line, true, singletons, ItemDelimiter));
    }
    
    testTheSingletons(sets);
  }
  
  @Test
  public void convert_Level_One_Singletons_Incorrect_No_Influence() {
    List<List<Integer>> sets = newArrayListWithCapacity(inputLines.length);
    HashSet<Integer> singletons = newHashSet();
    singletons.add(1);
    singletons.add(4);
    singletons.add(7);
    
    for (String line : inputLines) {
      sets.add(convertLineToSet(line, true, singletons, ItemDelimiter));
    }
    
    testTheSingletons(sets);
  }
  
  @Test
  public void convert_Not_Level_One_All_Singletons() {
    List<List<Integer>> sets = newArrayListWithCapacity(inputLines.length);
    HashSet<Integer> singletons = newHashSet();
    singletons.add(1);
    singletons.add(2);
    singletons.add(4);
    singletons.add(5);
    singletons.add(6);
    singletons.add(7);
    
    for (String line : inputLines) {
      sets.add(convertLineToSet(line, false, singletons, ItemDelimiter));
    }
    
    testTheSingletons(sets);
  }
  
  @Test
  public void convert_Not_Level_One_Subset_Singletons() {
    List<List<Integer>> sets = newArrayListWithCapacity(inputLines.length);
    HashSet<Integer> singletons = newHashSet();
    singletons.add(4);
    singletons.add(6);
    singletons.add(7);
    
    for (String line : inputLines) {
      sets.add(convertLineToSet(line, false, singletons, ItemDelimiter));
    }
    
    testTheSingletonsSubSet(sets);
  }
  
  @Test
  public void create_Length_Plus_One_Singletons() {
    List<SortedSet<Integer>> itemsets = newArrayList(getItemsets());
    
    Collection<SortedSet<Integer>> candidates = createCandidates(itemsets);
    
    testLengthPlusOneCandidates(candidates);
  }
  
  @Test
  public void obtain_Singletons_From_Collection_Of_Sets() {
    Set<SortedSet<Integer>> itemsets = getItemsets();
    
    Set<Integer> singletons = getSingletonsFromSets(itemsets);
    
    testSingletons(singletons);
  }
}
