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
import static com.google.common.collect.Sets.newTreeSet;
import static java.lang.Integer.valueOf;
import static java.util.Collections.sort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.SortedSet;

/**
 * Some extra utility functions used by BigFIM mapper classes.
 */
public class Tools {
  
  // Delimiter for the head of the line
  public static final String HeadDelimiter = "\t";
  // Delimiter for the items
  public static final String ItemDelimiter = " ";
  
  /**
   * Reads a list of itemsets from a file. Each line is in transactional format and with integer ids as follows:
   * 
   * <pre>
   * {@code<item1><space><item2>...<itemN><tab><support>}
   * </pre>
   * 
   * @param fileName
   *          name of the file from which itemsets are read
   * @return the list of itemsets
   * @throws NumberFormatException
   *           thrown if an item is not in integer format
   * @throws IOException
   *           thrown if the file can not be found or read
   */
  public static List<SortedSet<Integer>> readItemsetsFromFile(String fileName) throws NumberFormatException,
      IOException {
    List<SortedSet<Integer>> itemsets = newArrayList();
    
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
    while ((line = reader.readLine()) != null) {
      String[] splits = line.split(HeadDelimiter)[0].split(ItemDelimiter);
      SortedSet<Integer> set = newTreeSet();
      for (String split : splits) {
        set.add(valueOf(split));
      }
      itemsets.add(set);
    }
    reader.close();
    return itemsets;
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
  public static List<Integer> convertLineToSet(String line, boolean levelOne, Set<Integer> singletons, String delimiter) {
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
  
  /**
   * Creates candidates of length equal to length+1.
   * 
   * @param itemsets
   *          itemsets of length that are merged together to form length+1 candidates
   * @return a set of candidates that are combination of the original itemsets and that are of length+1
   */
  public static Collection<SortedSet<Integer>> createCandidates(List<SortedSet<Integer>> itemsets) {
    List<SortedSet<Integer>> candidates = newArrayListWithCapacity(itemsets.size());
    
    int i = 0;
    ListIterator<SortedSet<Integer>> it1 = itemsets.listIterator(i);
    for (; i < itemsets.size() - 1; i++) {
      Set<Integer> itemset1 = it1.next();
      ListIterator<SortedSet<Integer>> it2 = itemsets.listIterator(i + 1);
      while (it2.hasNext()) {
        Set<Integer> itemset2 = it2.next();
        if (check(itemset1, itemset2)) {
          SortedSet<Integer> set = newTreeSet(itemset1);
          set.addAll(itemset2);
          candidates.add(set);
        } else {
          continue;
        }
      }
    }
    
    return candidates;
  }
  
  private static boolean check(Set<Integer> set1, Set<Integer> set2) {
    Iterator<Integer> it1 = set1.iterator();
    Iterator<Integer> it2 = set2.iterator();
    
    for (int s = set1.size() - 1; s != 0; s--) {
      Integer next = it1.next();
      Integer next2 = it2.next();
      if (!next.equals(next2)) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Gets the unique list of singletons from a collection of words.
   * 
   * @param sets
   *          the complete collection of words
   * @return list of unique singletons appearing in the collection of words
   */
  public static Set<Integer> getSingletonsFromSets(Collection<SortedSet<Integer>> sets) {
    Set<Integer> singletons = newHashSet();
    for (SortedSet<Integer> set : sets) {
      singletons.addAll(set);
    }
    return singletons;
  }
  
}
