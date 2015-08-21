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
package be.uantwerpen.adrem.hadoop.util;

import static be.uantwerpen.adrem.util.FIMOptions.OUTPUT_DIR_KEY;
import static com.google.common.collect.Sets.newHashSet;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import be.uantwerpen.adrem.util.FIMOptions;

/**
 * Some extra utility functions for Hadoop.
 */
public class Tools {
  
  /**
   * Cleans the Hadoop file system by deleting the specified files if they exist.
   * 
   * @param files
   *          the files to delete
   */
  public static void cleanDirs(String... files) {
    System.out.println("[Cleaning]: Cleaning HDFS");
    Configuration conf = new Configuration();
    for (String filename : files) {
      System.out.println("[Cleaning]: Trying to delete " + filename);
      Path path = new Path(filename);
      try {
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
          if (fs.delete(path, true)) {
            System.out.println("[Cleaning]: Deleted " + filename);
          } else {
            System.out.println("[Cleaning]: Error while deleting " + filename);
          }
        } else {
          System.out.println("[Cleaning]: " + filename + " does not exist on HDFS");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  @SuppressWarnings("rawtypes")
  public static Job prepareJob(Path inputPath, Path outputPath, Class<? extends InputFormat> inputFormat,
      Class<? extends Mapper> mapper, Class<? extends Writable> mapperKey, Class<? extends Writable> mapperValue,
      Class<? extends Reducer> reducer, Class<? extends Writable> reducerKey, Class<? extends Writable> reducerValue,
      Class<? extends OutputFormat> outputFormat) throws IOException {
    Job job = new Job(new Configuration());
    
    Configuration jobConf = job.getConfiguration();
    
    if (reducer.equals(Reducer.class)) {
      if (mapper.equals(Mapper.class)) {
        throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
      }
      job.setJarByClass(mapper);
    } else {
      job.setJarByClass(reducer);
    }
    
    job.setInputFormatClass(inputFormat);
    jobConf.set("mapred.input.dir", inputPath.toString());
    
    job.setMapperClass(mapper);
    if (mapperKey != null) {
      job.setMapOutputKeyClass(mapperKey);
    }
    if (mapperValue != null) {
      job.setMapOutputValueClass(mapperValue);
    }
    
    jobConf.setBoolean("mapred.compress.map.output", true);
    
    job.setReducerClass(reducer);
    job.setOutputKeyClass(reducerKey);
    job.setOutputValueClass(reducerValue);
    
    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());
    
    return job;
  }
  
  public static String getJobAbsoluteOutputDir(@SuppressWarnings("rawtypes") Context context) {
    try {
      Path path = new Path(context.getConfiguration().get(OUTPUT_DIR_KEY));
      FileSystem fs = path.getFileSystem(context.getConfiguration());
      return fs.getFileStatus(path).getPath().toString();
    } catch (IOException e) {}
    return "";
  }
  
  public static String createPath(String... parts) {
    StringBuilder path = new StringBuilder();
    for (String part : parts) {
      path.append(part);
      path.append(Path.SEPARATOR);
    }
    return path.substring(0, path.length() - 1);
  }
  
  public static void cleanupAfterJob(FIMOptions opt) {
    if (!opt.debug) {
      cleanupSubdirsExcept(opt.outputDir, newHashSet("fis", "shortfis"));
    }
  }
  
  public static void cleanupSubdirsExcept(String dir, Collection<String> toKeep) {
    Path path = new Path(dir);
    try {
      for (FileStatus fs : path.getFileSystem(new Configuration()).listStatus(path)) {
        String[] sp = fs.getPath().toString().split(Path.SEPARATOR);
        String filename = sp[sp.length - 1];
        if (toKeep.contains(filename)) {
          cleanDirs(fs.getPath().toString() + Path.SEPARATOR + "_SUCCESS");
          continue;
        }
        cleanDirs(fs.getPath().toString());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
