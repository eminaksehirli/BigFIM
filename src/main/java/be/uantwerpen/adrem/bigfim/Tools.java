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
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.sort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Set;

import be.uantwerpen.adrem.util.ItemSetTrie;

/**
 * Some extra utility functions used by BigFIM mapper classes.
 */
public class Tools {
  
  // Delimiter for the head of the line
  public static final String HeadDelimiter = "\t";
  // Delimiter for the items
  public static final String ItemDelimiter = " ";
  
  public static int readCountTrieFromItemSetsFile(String fileName, ItemSetTrie itemsetTrie)
      throws NumberFormatException, IOException {
    String line;
    int itemsetSize = 0;
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
    List<int[]> itemsets = newArrayList();
    int[] prevSet = new int[0];
    int candidateCount = 0;
    while ((line = reader.readLine()) != null) {
      String itemset = line.split(HeadDelimiter)[0];
      String[] split = itemset.split(ItemDelimiter);
      itemsetSize = split.length;
      
      int[] intItemset = new int[split.length];
      int i = 0;
      for (String item : split) {
        intItemset[i++] = Integer.parseInt(item);
      }
      
      if (!check(intItemset, prevSet)) {
        candidateCount += addToTrie(itemsets, itemsetTrie);
        itemsets.clear();
      }
      prevSet = intItemset;
      
      itemsets.add(intItemset);
    }
    candidateCount += addToTrie(itemsets, itemsetTrie);
    reader.close();
    System.out.println("Candidates directly from file: " + candidateCount);
    return itemsetSize;
  }
  
  private static int addToTrie(List<int[]> itemsets, ItemSetTrie itemsetTrie) {
    int candidateCount = 0;
    ListIterator<int[]> it1 = itemsets.listIterator(0);
    for (int i = 0; i < itemsets.size() - 1; i++) {
      int[] itemset1 = it1.next();
      ListIterator<int[]> it2 = itemsets.listIterator(i + 1);
      while (it2.hasNext()) {
        int[] itemset2 = it2.next();
        ItemSetTrie trie = itemsetTrie;
        int lastIx = itemset1.length - 1;
        for (int j = 0; j < lastIx; j++) {
          trie = trie.getChild(itemset1[j]);
        }
        if (itemset1[lastIx] < itemset2[lastIx]) {
          trie = trie.getChild(itemset1[lastIx]);
          trie.getChild(itemset2[lastIx]);
        } else {
          trie = trie.getChild(itemset2[lastIx]);
          trie.getChild(itemset1[lastIx]);
        }
        
        candidateCount++;
      }
    }
    return candidateCount;
  }
  
  /**
   * Converts a line containing items into a list of integers. If levelOne is true, all items are added to the list.
   * Otherwise, only items occurring in the set of unique singletons are added to the list. Hence, the parameters
   * levelOne and singletons are excluding each other, i.e., only one of them should be set at a time. If both are set,
   * levelOne is used first.
   * 
   * @param line
   *          the line containing items in integer format
   * @param levelOne
   *          indicates if the first level is being processed here, i.e., singletons should be counted
   * @param singletons
   *          list of unique singletons that should be retrieved from the line
   * @return a list of items in integer format that is either the complete line, if levelOne is true. Otherwise if
   *         singletons is not empty, this list of items is a subset of the singletons occurring in line.
   */
  public static List<Integer> convertLineToSet(String line, boolean levelOne, Set<Integer> singletons,
      String delimiter) {
    String[] itemsSplit = line.split(delimiter);
    
    List<Integer> items = newArrayListWithCapacity(itemsSplit.length);
    for (String itemString : itemsSplit) {
      Integer item = Integer.valueOf(itemString);
      if (!levelOne && !singletons.contains(item)) {
        continue;
      }
      items.add(item);
    }
    sort(items);
    return items;
  }
  
  public static boolean check(int[] set1, int[] set2) {
    if (set1.length != set2.length) {
      return false;
    }
    for (int i = 0, end = set1.length - 1; i < end; i++) {
      if (set1[i] != set2[i]) {
        return false;
      }
    }
    return true;
  }
  
  public static Set<Integer> getSingletonsFromCountTrie(ItemSetTrie trie) {
    Set<Integer> singletons = newHashSet();
    getSingletonsFromCountTrieRec(trie, singletons);
    return singletons;
  }
  
  private static void getSingletonsFromCountTrieRec(ItemSetTrie trie, Set<Integer> singletons) {
    if (trie.id != -1) {
      singletons.add(trie.id);
    }
    for (Entry<Integer,ItemSetTrie> entry : trie.children.entrySet()) {
      getSingletonsFromCountTrieRec(entry.getValue(), singletons);
    }
    if (trie.id == -1) {
      System.out.println("Singletons: " + singletons.size());
    }
  }
}
