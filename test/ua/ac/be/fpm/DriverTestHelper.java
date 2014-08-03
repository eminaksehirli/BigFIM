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
package ua.ac.be.fpm;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import ua.ac.be.fpm.eclat.util.TrieDumper;

public class DriverTestHelper {
  
  static int[][] Expecteds = new int[][] { {1, 2, 3}, {4, 5, 6, 7}, {1, 3, 7}};
  public static int[][] Length_2_FIs = new int[][] { {1, 3}, {1, 7}, {3, 7}, {2, 6}, {2, 3}, {6, 7}};
  // "2 3" and "6 7" are reported as short FIs because of supersetting.
  
  public static String[] Data = new String[] {"1 2 3", "1 2 3", "1 2 3", "1 2 3", "1 4 5 6 7", "2 4 5 6 7",
      "3 4 5 6 7", "4 5 6 7", "1 3 7", "1 3 7", "1 3 7", "1 3 7", "2 6", "2 6"};
  
  public static final int MinSup = 4;
  private final List<Set<Integer>> expecteds;
  
  public DriverTestHelper() {
    this(Expecteds);
  }
  
  public DriverTestHelper(int[][] expecteds) {
    this(prepareExpecteds(expecteds));
  }
  
  public DriverTestHelper(List<Set<Integer>> expecteds) {
    this.expecteds = expecteds;
  }
  
  public void assertAllFrequentsAreFound(List<Set<Integer>> actuals) {
    List<Set<Integer>> copyOfExpecteds = newArrayList(expecteds);
    nextExpected: for (Iterator<Set<Integer>> expIt = copyOfExpecteds.iterator(); expIt.hasNext();) {
      Set<Integer> expected = expIt.next();
      
      for (Set<Integer> actual : actuals) {
        if (expected.equals(actual)) {
          expIt.remove();
          continue nextExpected;
        }
      }
    }
    
    if (!copyOfExpecteds.isEmpty()) {
      fail("These should be frequent: " + copyOfExpecteds);
    }
  }
  
  public void assertAllOfThemFrequent(List<Set<Integer>> actuals) {
    List<Set<Integer>> copyOfActuals = newArrayList(actuals);
    for (Set<Integer> expected : expecteds) {
      for (Iterator<Set<Integer>> it = copyOfActuals.iterator(); it.hasNext();) {
        Set<Integer> actual = it.next();
        
        if (expected.containsAll(actual)) {
          it.remove();
          continue;
        }
      }
    }
    
    if (!copyOfActuals.isEmpty()) {
      fail("These itemsets should NOT be frequent:" + copyOfActuals);
    }
  }
  
  public static List<Set<Integer>> readResults(final String outputFile) throws IOException, FileNotFoundException {
    File tempFile = File.createTempFile("fis", ".txt");
    tempFile.deleteOnExit();
    TrieDumper.main(new String[] {outputFile, tempFile.getAbsolutePath()});
    
    Scanner sc = new Scanner(tempFile);
    
    List<String> actualStrings = newArrayListWithCapacity(10);
    while (sc.hasNextLine()) {
      String itemsetStr = sc.nextLine().split("\t")[1];
      
      StringBuilder actualStr = new StringBuilder();
      
      int p2 = 0;
      int p1 = itemsetStr.indexOf('(');
      while (p1 >= 0) {
        actualStr.append(itemsetStr.substring(p2, p1)).append(" ");
        p2 = itemsetStr.indexOf(')', p1) + 1;
        p1 = itemsetStr.indexOf('(', p2);
      }
      
      actualStrings.add(actualStr.substring(0, actualStr.length() - 1));
    }
    sc.close();
    
    List<Set<Integer>> actuals = newArrayListWithCapacity(actualStrings.size());
    
    for (String actualString : actualStrings) {
      String[] actualArr = actualString.split(" ");
      Set<Integer> actual = newHashSet();
      for (String i : actualArr) {
        actual.add(Integer.valueOf(i));
      }
      actuals.add(actual);
    }
    return actuals;
  }
  
  public static void delete(File file) {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        delete(f);
      }
    }
    file.delete();
  }
  
  private static List<Set<Integer>> prepareExpecteds(int[][] expecteds) {
    
    List<Set<Integer>> expectedsList = newArrayListWithCapacity(expecteds.length);
    for (int[] expected : expecteds) {
      Set<Integer> e = new HashSet<Integer>();
      for (int i : expected) {
        e.add(i);
      }
      expectedsList.add(e);
    }
    return expectedsList;
  }
}
