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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import ua.ac.be.fpm.util.DbTransposer;

public class DbTransposerTest {
  
  private static String[] empty = new String[] {};
  private static String[] nonEmptyNoDuplicates = new String[] {"1 2 3 4", "1 4 5 7", "3 5 6", "4 7"};
  private static String[] nonEmptyWithDuplicates = new String[] {"1 2 3 4 1", "4 1 4 5 7", "3 5 5 6", "4 7 4"};
  
  private static String[] outputEmpty = new String[] {};
  private static String[] outputNonEmpty = new String[] {"1\t0 1", "2\t0", "3\t0 2", "4\t0 1 3", "5\t1 2", "6\t2",
      "7\t1 3"};
  
  private void writeToFile(File in, String[] lines) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(in));
    for (String line : lines) {
      writer.write(line + "\n");
    }
    writer.close();
  }
  
  private String getTidsFileName(String in) {
    int dotIndex = in.lastIndexOf('.');
    if (dotIndex < 1) {
      return in + "-tids";
    }
    return in.substring(0, dotIndex) + "-tids" + in.substring(dotIndex);
  }
  
  private void checkResult(File out, String[] output) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(out));
    String line;
    int i = 0;
    while ((line = reader.readLine()) != null) {
      Assert.assertEquals(output[i++], line);
    }
    reader.close();
  }
  
  @Test
  public void emtpy_Writes_Empty() throws IOException {
    File in = File.createTempFile("in_Empty_Writes_Empty", ".txt");
    File out = new File(getTidsFileName(in.getAbsolutePath()));
    in.deleteOnExit();
    out.deleteOnExit();
    
    writeToFile(in, empty);
    
    DbTransposer transposer = new DbTransposer();
    transposer.transpose(in.getAbsolutePath());
    
    checkResult(out, outputEmpty);
  }
  
  @Test
  public void non_Empty_Writes_Non_Empty() throws IOException {
    File in = File.createTempFile("in_Non_Empty_Writes_Non_Empty", ".txt");
    File out = new File(getTidsFileName(in.getAbsolutePath()));
    in.deleteOnExit();
    out.deleteOnExit();
    
    writeToFile(in, nonEmptyNoDuplicates);
    
    DbTransposer transposer = new DbTransposer();
    transposer.transpose(in.getAbsolutePath());
    
    checkResult(out, outputNonEmpty);
  }
  
  @Test
  public void duplicates_Are_Not_Written() throws IOException {
    File in = File.createTempFile("in_Duplicates_Are_Not_Written", ".txt");
    File out = new File(getTidsFileName(in.getAbsolutePath()));
    in.deleteOnExit();
    out.deleteOnExit();
    
    writeToFile(in, nonEmptyWithDuplicates);
    
    DbTransposer transposer = new DbTransposer();
    transposer.transpose(in.getAbsolutePath());
    
    checkResult(out, outputNonEmpty);
  }
  
  @Test
  public void existing_Output_File_Prints_Error() throws IOException {
    PrintStream ps = EasyMock.createMock(PrintStream.class);
    
    ps.println("File exists, aborting!");
    
    EasyMock.replay(ps);
    
    System.setOut(ps);
    
    File in = File.createTempFile("in_Duplicates_Are_Not_Written", ".txt");
    File out = new File(getTidsFileName(in.getAbsolutePath()));
    out.createNewFile();
    in.deleteOnExit();
    out.deleteOnExit();
    
    writeToFile(in, nonEmptyWithDuplicates);
    
    DbTransposer transposer = new DbTransposer();
    transposer.transpose(in.getAbsolutePath());
    
    EasyMock.verify(ps);
  }
}
