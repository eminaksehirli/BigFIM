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
package ua.ac.be.fpm.bigfim;

import static ua.ac.be.fpm.DriverTestHelper.Data;
import static ua.ac.be.fpm.DriverTestHelper.MinSup;
import static ua.ac.be.fpm.DriverTestHelper.readResults;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import ua.ac.be.fpm.DriverTestHelper;
import ua.ac.be.fpm.FIMTestCase;
import ua.ac.be.fpm.bigfim.BigFIMDriver;
import ua.ac.be.fpm.bigfim.ComputeTidListReducer;

public class BigFIMDriverTest_ extends FIMTestCase {
  private static boolean bigFIMHasRun = false;
  private static boolean bigFIMHasRunPGU = false;
  static final String Output_File_Name = "fis/part-r-00000";
  private DriverTestHelper helper;
  private File input;
  private String output;
  private File outputDir;
  private List<Set<Integer>> resultsPGU;
  private static List<Set<Integer>> results;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    helper = new DriverTestHelper();
    input = getTestTempFile("input");
    outputDir = getTestTempDir("output");
    output = outputDir.getAbsoluteFile() + "/" + Output_File_Name;
    
    writeLines(input, Data);
  }
  
  @Test
  public void BigFIM_Finds_Frequent_Itemsets() throws Exception {
    
    runBigFIMOnce();
    helper.assertAllOfThemFrequent(results);
  }
  
  @Test
  public void BigFIM_Finds_Frequent_Itemsets_Prefix_Group_Updated() throws Exception {
    
    runBigFIMOncePrefixGroupUpdated();
    helper.assertAllOfThemFrequent(resultsPGU);
  }
  
  @Test
  public void BigFIM_Finds_All_The_Closed_Frequent_Itemsets() throws Exception {
    
    runBigFIMOnce();
    helper.assertAllFrequentsAreFound(results);
  }
  
  private void runBigFIMOnce() throws Exception {
    if (!bigFIMHasRun) {
      try {
        BigFIMDriver.main(new String[] {"-i", input.getAbsolutePath(), "-o", outputDir.getAbsolutePath(), "-s",
            MinSup + "", "-p", "2", "-m", "4"});
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      results = readResults(output);
    }
    bigFIMHasRun = true;
  }
  
  private void runBigFIMOncePrefixGroupUpdated() throws FileNotFoundException, IOException {
    if (!bigFIMHasRunPGU) {
      try {
        int tmp = ComputeTidListReducer.MAX_NUMBER_OF_TIDS;
        ComputeTidListReducer.MAX_NUMBER_OF_TIDS = 18;
        BigFIMDriver.main(new String[] {"-i", input.getAbsolutePath(), "-o", outputDir.getAbsolutePath(), "-s",
            MinSup + "", "-p", "1", "-m", "4"});
        ComputeTidListReducer.MAX_NUMBER_OF_TIDS = tmp;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      resultsPGU = readResults(output);
    }
    bigFIMHasRunPGU = true;
  }
}
