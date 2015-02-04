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
package ua.ac.be.fpm.util;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import ua.ac.be.fpm.eclat.util.TidList;

/**
 * Some extra utility functions for FPM algorithms
 */
public class Tools {
  
  public static TidList intersect(TidList tidList1, TidList tidList2) {
    int[][] intersection = new int[tidList1.tids.length][];
    for (int i = 0; i < tidList1.tids.length; i++) {
      int[] tids_1 = tidList1.tids[i];
      int[] tids_2 = tidList2.tids[i];
      
      if (tids_1 != null && tids_2 != null) {
        int[] inter = intersect(tids_1, tids_2);
        if (inter.length != 0) {
          intersection[i] = inter;
        }
      }
    }
    return new TidList(intersection);
  }
  
  public static TidList setDifference(TidList tidList1, TidList tidList2) {
    int[][] difference = new int[tidList1.tids.length][];
    for (int i = 0; i < tidList1.tids.length; i++) {
      int[] tids_1 = tidList1.tids[i];
      int[] tids_2 = tidList2.tids[i];
      
      if (tids_2 == null) {
        difference[i] = tids_1;
      } else if (tids_1 != null) {
        final int[] diff = setDifference(tids_1, tids_2);
        if (diff.length != 0) {
          difference[i] = diff;
        }
      }
    }
    
    return new TidList(difference);
  }
  
  /**
   * Computes the intersection of two integer arrays and reports it.
   * 
   * @param tids1
   *          the first array of integers
   * @param tids2
   *          the second array of integer
   * @return the intersection of the two integer arrays
   */
  private static int[] intersect(int[] tids1, int[] tids2) {
    List<Integer> intersection = newArrayList();
    
    int ix1 = 0, ix2 = 0;
    while (ix1 != tids1.length && ix2 != tids2.length) {
      int i1 = tids1[ix1];
      int i2 = tids2[ix2];
      if (i1 == i2) {
        intersection.add(i1);
        ix1++;
        ix2++;
      } else if (i1 < i2) {
        ix1++;
      } else {
        ix2++;
      }
    }
    
    return toIntArray(intersection);
  }
  
  /**
   * Computes the set difference of two integer arrays and reports it. Set difference is obtained by removing the
   * integers from the second array of integers from the first array of integers.
   * 
   * @param tids1
   *          the first array of integers
   * @param tids2
   *          the second array of integer
   * @return the intersection of the two integer arrays
   */
  private static int[] setDifference(int[] tids1, int[] tids2) {
    List<Integer> difference = newArrayList();
    
    int ix1 = 0, ix2 = 0;
    while (ix1 != tids1.length && ix2 != tids2.length) {
      int i1 = tids1[ix1];
      int i2 = tids2[ix2];
      if (i1 == i2) {
        ix1++;
        ix2++;
      } else if (i1 < i2) {
        difference.add(tids1[ix1]);
        ix1++;
      } else {
        ix2++;
      }
    }
    for (; ix1 < tids1.length; ix1++) {
      difference.add(tids1[ix1]);
    }
    
    return toIntArray(difference);
  }
  
  private static int[] toIntArray(List<Integer> list) {
    int[] intArray = new int[list.size()];
    int ix = 0;
    for (Integer i : list) {
      intArray[ix++] = i;
    }
    return intArray;
  }
}
