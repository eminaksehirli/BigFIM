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
package ua.ac.be.fpm.eclat;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Arrays.copyOf;
import static ua.ac.be.fpm.util.Tools.setDifference;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import ua.ac.be.fpm.eclat.util.Item;
import ua.ac.be.fpm.eclat.util.SetReporter;
import ua.ac.be.fpm.eclat.util.TidList;

import com.google.common.primitives.Ints;

/**
 * Eclat miner implementation for mining closed itemsets. This is the depth-first frequent itemset generation algorithm
 * proposed by Zaki et al. "New Algorithms for Fast Discovery of Association Rules". This implementation starts with tid
 * list, but immediately switches to diffSets when computing candidates for the next level.
 */
public class EclatMiner {
  
  public static class AscendingItemComparator implements Comparator<Item> {
    @Override
    public int compare(Item o1, Item o2) {
      return Ints.compare(o1.support, o2.support);
    }
  }
  
  private SetReporter reporter;
  private long maxSize = Long.MAX_VALUE;
  
  public void setSetReporter(SetReporter setReporter) {
    this.reporter = setReporter;
  }
  
  /**
   * Sets the maximum length of an itemset found by the miner.
   * 
   * @param maxSize
   *          the maximum length of an itemset
   */
  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
  }
  
  /**
   * Mines the sub prefix tree for frequent itemsets. items do not have to be conditioned, instead they should contain
   * full TID's.
   * 
   * @param item
   *          Prefix of the tree to mine.
   * @param items
   *          List of items with their initial TID lists.
   * @param minSup
   *          Minimum support
   */
  public void mineRecByPruning(Item item, List<Item> items, int minSup) {
    
    ArrayList<Item> newItems = newArrayList();
    
    for (Iterator<Item> it = items.iterator(); it.hasNext();) {
      Item item_2 = it.next();
      if (item.id == item_2.id) {
        continue;
      }
      
      TidList condTids = setDifference(item.getTids(), item_2.getTids());
      
      int newSupport = item.support - condTids.size();
      if (newSupport >= minSup) {
        Item newItem = new Item(item_2.id, newSupport, condTids);
        newItems.add(newItem);
      }
    }
    if (newItems.size() > 0) {
      newItems.trimToSize();
      declatRec(new int[] {item.id}, newItems, minSup, false);
    }
  }
  
  /**
   * Mines the sub prefix tree for frequent itemsets.
   * 
   * @param prefix
   *          Prefix of the tree to mine.
   * @param extensions
   *          List of items with their conditional TID lists. All of the items should be frequent extensions of the
   *          prefix, i.e., support of union of prefix and each item should be greater than or equal to minSup.
   * @param minSup
   *          Minimum support
   */
  public void mineRec(int[] prefix, List<Item> extensions, int minSup) {
    declatRec(prefix, extensions, minSup, true);
  }
  
  private void declatRec(int[] prefix, List<Item> items, int minSup, boolean tidLists) {
    Iterator<Item> it1 = items.iterator();
    int[] newPrefix = copyOf(prefix, prefix.length + 1);
    for (int i = 0; i < items.size(); i++) {
      Item item1 = it1.next();
      int support = item1.support;
      newPrefix[newPrefix.length - 1] = item1.id;
      
      // reporter.report(newPrefix, support);
      boolean hasAClosedSuperSet = false;
      final boolean canBeExtended = newPrefix.length < maxSize && i < items.size() - 1;
      if (canBeExtended) {
        List<Item> newItems = newArrayListWithCapacity(items.size() - i);
        TidList tids1 = item1.getTids();
        
        for (ListIterator<Item> it2 = items.listIterator(i + 1); it2.hasNext();) {
          Item item2 = it2.next();
          TidList tids2 = item2.getTids();
          TidList condTids;
          if (tidLists) {
            condTids = setDifference(tids1, tids2);
          } else {
            condTids = setDifference(tids2, tids1);
          }
          
          final int supDiff = condTids.size();
          if (supDiff == 0) {
            hasAClosedSuperSet = true;
          }
          
          int newSupport = support - supDiff;
          if (newSupport >= minSup) {
            Item newItem = new Item(item2.id, newSupport, condTids);
            newItems.add(newItem);
          }
        }
        if (newItems.size() > 0) {
          declatRec(newPrefix, newItems, minSup, false);
        }
      } else {
        // report if the itemset cannot be extended anymore
        reporter.report(newPrefix, support);
      }
      if (canBeExtended && !hasAClosedSuperSet) {
        // do not report closed itemsets
        reporter.report(newPrefix, support);
      }
      
      if (newPrefix.length < maxSize && closureCheck(item1)) {
        break;
      }
    }
  }
  
  private static boolean closureCheck(Item item) {
    return item.getTids().size() == 0;
  }
}