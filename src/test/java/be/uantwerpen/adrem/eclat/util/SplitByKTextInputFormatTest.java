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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static be.uantwerpen.adrem.util.FIMOptions.NUMBER_OF_LINES_KEY;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.easymock.EasyMock;
import org.junit.Test;

import be.uantwerpen.adrem.hadoop.util.SplitByKTextInputFormat;

public class SplitByKTextInputFormatTest {
  
  private static String[] empty = new String[] {};
  
  private static String[] non_Empty = new String[] {"12345", "678910", "1510"};
  
  private static void writeToFile(File in, String[] lines) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(in));
    for (String line : lines) {
      writer.write(line + "\n");
    }
    writer.close();
  }
  
  private static File createTmpFile(String fileName, String[] content) throws IOException {
    File in = File.createTempFile(fileName, ".txt");
    in.deleteOnExit();
    writeToFile(in, content);
    return in;
  }
  
  private Configuration createConfiguration(int... numberOfLines) {
    Configuration conf = new Configuration();
    if (numberOfLines.length > 0) {
      conf.setLong(NUMBER_OF_LINES_KEY, numberOfLines[0]);
    }
    conf.set("fs.default.name", "file:///");
    conf.setBoolean("fs.file.impl.disable.cache", false);
    conf.setClass("fs.file.impl", RawLocalFileSystem.class, FileSystem.class);
    return conf;
  }
  
  private static void checkSplits(List<FileSplit> expected, List<FileSplit> actual) throws IOException {
    assertEquals("Expected <" + expected.size() + "> splits, but got <" + actual.size() + ">.", expected.size(),
        actual.size());
    
    for (int i = 0; i < actual.size(); i++) {
      assertEquals("Split<" + i + ">: Expected pathName <" + actual.get(i).getPath() + ">, but got <"
          + expected.get(i).getPath() + ">.", actual.get(i).getPath(), expected.get(i).getPath());
      assertEquals("Split<" + i + ">: Expected start <" + actual.get(i).getStart() + ">, but got <"
          + expected.get(i).getStart() + ">.", actual.get(i).getStart(), expected.get(i).getStart());
      assertEquals("Split<" + i + ">: Expected length <" + actual.get(i).getLength() + ">, but got <"
          + expected.get(i).getLength() + ">.", actual.get(i).getLength(), expected.get(i).getLength());
      assertArrayEquals("Split<" + i + ">: Expected locations <" + actual.get(i).getLocations() + ">, but got <"
          + expected.get(i).getLocations() + ">.", actual.get(i).getLocations(), expected.get(i).getLocations());
    }
  }
  
  @Test
  public void count_Empty_File_Conf_Not_Set() throws IOException {
    File in = createTmpFile("in_Count_Empty_File_Conf_Not_Set", empty);
    Configuration conf = createConfiguration();
    Path p = new Path(in.getAbsolutePath());
    
    assertEquals(0, SplitByKTextInputFormat.getTotalNumberOfLines(conf, p));
  }
  
  @Test
  public void count_Empty_File_Conf_Set_Correct() throws IOException {
    File in = createTmpFile("in_Count_Empty_File_Conf_Set_Correct", empty);
    Configuration conf = createConfiguration(0);
    Path p = new Path(in.getAbsolutePath());
    
    assertEquals(0, SplitByKTextInputFormat.getTotalNumberOfLines(conf, p));
  }
  
  @Test
  public void count_Empty_File_Conf_Set_Incorrect() throws IOException {
    File in = createTmpFile("in_Count_Empty_File_Conf_Set_Incorrect", empty);
    Configuration conf = createConfiguration(10);
    Path p = new Path(in.getAbsolutePath());
    
    assertEquals(10, SplitByKTextInputFormat.getTotalNumberOfLines(conf, p));
  }
  
  @Test
  public void count_Non_Empty_File_Conf_Not_Set() throws IOException {
    File in = createTmpFile("in_Count_Non_Empty_File_Conf_Not_Set", non_Empty);
    Configuration conf = createConfiguration();
    Path p = new Path(in.getAbsolutePath());
    
    assertEquals(3, SplitByKTextInputFormat.getTotalNumberOfLines(conf, p));
  }
  
  @Test
  public void count_Non_Empty_File_Conf_Set_Correct() throws IOException {
    File in = createTmpFile("in_Count_Non_Empty_File_Conf_Set_Correct", non_Empty);
    Configuration conf = createConfiguration(3);
    Path p = new Path(in.getAbsolutePath());
    
    assertEquals(3, SplitByKTextInputFormat.getTotalNumberOfLines(conf, p));
  }
  
  @Test
  public void count_Non_Empty_File_Conf_Set_Incorrect() throws IOException {
    File in = createTmpFile("in_Count_Non_Empty_File_Conf_Set_Incorrect", non_Empty);
    Configuration conf = createConfiguration(10);
    Path p = new Path(in.getAbsolutePath());
    
    assertEquals(10, SplitByKTextInputFormat.getTotalNumberOfLines(conf, p));
  }
  
  @Test
  public void splits_Empty_File() throws IOException {
    File in = createTmpFile("in_Splits_Empty_File", empty);
    Configuration conf = createConfiguration();
    
    FileStatus status = EasyMock.createMock(FileStatus.class);
    EasyMock.expect(status.getPath()).andReturn(new Path(in.getAbsolutePath()));
    EasyMock.expect(status.isDir()).andReturn(false);
    EasyMock.replay(status);
    
    List<FileSplit> splits = SplitByKTextInputFormat.getSplitsForFile(status, conf, 2);
    
    ArrayList<FileSplit> expected = newArrayList();
    assertEquals(expected, splits);
  }
  
  @Test
  public void splits_Non_Empty_File_One_Split() throws IOException {
    File in = createTmpFile("in_Splits_Non_Empty_File_One_Split", non_Empty);
    Configuration conf = createConfiguration();
    
    FileStatus status = EasyMock.createMock(FileStatus.class);
    EasyMock.expect(status.getPath()).andReturn(new Path(in.getAbsolutePath()));
    EasyMock.expect(status.isDir()).andReturn(false);
    EasyMock.replay(status);
    
    List<FileSplit> splits = SplitByKTextInputFormat.getSplitsForFile(status, conf, 1);
    
    List<FileSplit> expected = newArrayListWithCapacity(1);
    expected.add(new FileSplit(new Path(in.getAbsolutePath()), 0, 17, new String[] {}));
    
    checkSplits(expected, splits);
  }
  
  @Test
  public void splits_Non_Empty_File_Ok_Splits() throws IOException {
    File in = createTmpFile("in_Splits_Non_Empty_File_Ok_Splits", non_Empty);
    Configuration conf = createConfiguration();
    
    FileStatus status = EasyMock.createMock(FileStatus.class);
    EasyMock.expect(status.getPath()).andReturn(new Path(in.getAbsolutePath()));
    EasyMock.expect(status.isDir()).andReturn(false);
    EasyMock.replay(status);
    
    List<FileSplit> splits = SplitByKTextInputFormat.getSplitsForFile(status, conf, 2);
    
    List<FileSplit> expected = newArrayListWithCapacity(2);
    expected.add(new FileSplit(new Path(in.getAbsolutePath()), 0, 12, new String[] {}));
    expected.add(new FileSplit(new Path(in.getAbsolutePath()), 12, 5, new String[] {}));
    
    checkSplits(expected, splits);
  }
  
  @Test
  public void splits_Non_Empty_File_More_Splits_Than_Lines() throws IOException {
    File in = createTmpFile("in_Splits_Non_Empty_File_More_Splits_Than_Lines", non_Empty);
    Configuration conf = createConfiguration();
    
    FileStatus status = EasyMock.createMock(FileStatus.class);
    EasyMock.expect(status.getPath()).andReturn(new Path(in.getAbsolutePath()));
    EasyMock.expect(status.isDir()).andReturn(false);
    EasyMock.replay(status);
    
    List<FileSplit> splits = SplitByKTextInputFormat.getSplitsForFile(status, conf, 10);
    
    List<FileSplit> expected = newArrayListWithCapacity(3);
    expected.add(new FileSplit(new Path(in.getAbsolutePath()), 0, 5, new String[] {}));
    expected.add(new FileSplit(new Path(in.getAbsolutePath()), 5, 7, new String[] {}));
    expected.add(new FileSplit(new Path(in.getAbsolutePath()), 12, 5, new String[] {}));
    
    checkSplits(expected, splits);
  }
}
