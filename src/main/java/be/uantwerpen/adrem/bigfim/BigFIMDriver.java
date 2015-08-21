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

import static be.uantwerpen.adrem.bigfim.AprioriPhaseReducer.COUNTER_GROUPNAME;
import static be.uantwerpen.adrem.bigfim.AprioriPhaseReducer.COUNTER_NRLARGEPREFIXGROUPS;
import static be.uantwerpen.adrem.hadoop.util.SplitByKTextInputFormat.NUMBER_OF_CHUNKS;
import static be.uantwerpen.adrem.hadoop.util.Tools.cleanDirs;
import static be.uantwerpen.adrem.hadoop.util.Tools.cleanupAfterJob;
import static be.uantwerpen.adrem.hadoop.util.Tools.prepareJob;
import static be.uantwerpen.adrem.util.FIMOptions.DELIMITER_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.MIN_SUP_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.NUMBER_OF_LINES_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.NUMBER_OF_MAPPERS_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.OUTPUT_DIR_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.PREFIX_LENGTH_KEY;
import static java.io.File.separator;
import static org.apache.hadoop.filecache.DistributedCache.addCacheFile;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import be.uantwerpen.adrem.eclat.EclatMinerMapper;
import be.uantwerpen.adrem.eclat.EclatMinerReducer;
import be.uantwerpen.adrem.hadoop.util.IntArrayWritable;
import be.uantwerpen.adrem.hadoop.util.IntMatrixWritable;
import be.uantwerpen.adrem.hadoop.util.NoSplitSequenceFileInputFormat;
import be.uantwerpen.adrem.hadoop.util.SplitByKTextInputFormat;
import be.uantwerpen.adrem.util.FIMOptions;

/**
 * Driver class for the BigFIM algorithm. This class calls all necessary map and reduce cycles and eventually writes the
 * output to a subdirectory called 'fis'.
 * 
 * The first phase consists of mining the X-prefixes using a distributed version of apriori. Each mapper is given a part
 * of the database and computes words of length+1 that are frequent. In the second phase the tid lists of the words are
 * computed. The third and last phase uses Eclat on a conditional dataset to compute closed sets.
 */
public class BigFIMDriver implements Tool {
  
  private static final String OFis = "fis";
  public static final String rExt = "-r-00000";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BigFIMDriver(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    FIMOptions opt = new FIMOptions();
    if (!opt.parseOptions(args)) {
      opt.printHelp();
      return -1;
    }
    
    cleanDirs(new String[] {opt.outputDir});
    long start = System.currentTimeMillis();
    
    int phase = startAprioriPhase(opt);
    if (canStartPrefixGeneration(opt, phase)) {
      startCreatePrefixGroups(opt, phase);
      if (canStartMining(opt)) {
        startMining(opt);
      } else {
        System.out.println("[BigFIM]: No prefix groups found");
      }
    } else {
      System.out.println("[BigFIM]: No prefixes to extend further");
    }
    cleanupAfterJob(opt);
    long end = System.currentTimeMillis();
    
    System.out.println("[BigFIM]: Total time: " + (end - start) / 1000 + "s");
    return 1;
  }
  
  private boolean canStartPrefixGeneration(FIMOptions opt, int phase) throws IOException {
    Path path = new Path(opt.outputDir + separator + "tg" + phase);
    return phase >= opt.prefixLength && path.getFileSystem(new Configuration()).exists(path);
  }
  
  private boolean canStartMining(FIMOptions opt) throws IOException {
    Path path = new Path(opt.outputDir + separator + "pg");
    return path.getFileSystem(new Configuration()).exists(path);
  }
  
  private static void setConfigurationValues(Configuration conf, FIMOptions opt) {
    conf.set(DELIMITER_KEY, opt.delimiter);
    conf.setInt(MIN_SUP_KEY, opt.minSup);
    conf.setInt(NUMBER_OF_MAPPERS_KEY, opt.nrMappers);
    conf.setInt(NUMBER_OF_CHUNKS, opt.nrMappers);
    conf.setInt(PREFIX_LENGTH_KEY, opt.prefixLength);
    conf.setStrings(OUTPUT_DIR_KEY, opt.outputDir);
  }
  
  private static void runJob(Job job, String jobName) throws ClassNotFoundException, IOException, InterruptedException {
    long start = System.currentTimeMillis();
    job.waitForCompletion(true);
    long end = System.currentTimeMillis();
    System.out.println("Job " + jobName + " took " + (end - start) / 1000 + "s");
  }
  
  protected int startAprioriPhase(FIMOptions opt)
      throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
    long nrLines = -1;
    int i = 1;
    boolean run = true;
    while (run) {
      if (i == 1) {
        String outputDir = opt.outputDir + separator + "ap" + i;
        String info = "Apriori Phase " + i;
        run = runAprioriOncPhaseOnce(opt, nrLines, i, info, outputDir, null);
      } else {
        Path path = new Path(opt.outputDir + separator + "tg" + (i - 1));
        for (FileStatus status : path.getFileSystem(new Configuration()).listStatus(path)) {
          String cacheFile = status.getPath().toString();
          String trieGroupCount = cacheFile.substring(cacheFile.lastIndexOf('/'), cacheFile.length());
          trieGroupCount = trieGroupCount.split("-")[1];
          String outputDir = opt.outputDir + separator + "ap" + i + "-trieGroup" + trieGroupCount;
          System.out.println("CacheFile " + cacheFile);
          String info = "Apriori Phase " + i + ", Trie Group " + trieGroupCount;
          run = runAprioriOncPhaseOnce(opt, nrLines, i, info, outputDir, cacheFile);
        }
      }
      
      i++;
    }
    return i - 1;
  }
  
  private boolean runAprioriOncPhaseOnce(FIMOptions opt, long nrLines, int i, String info, String outputDir,
      String cacheFile) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
    int prefixSize = opt.prefixLength;
    
    System.out.println("[AprioriPhase]: Phase: " + i + " input: " + opt.inputFile + ", output: " + opt.outputDir);
    
    Job job = prepareJob(new Path(opt.inputFile), new Path(outputDir), SplitByKTextInputFormat.class,
        AprioriPhaseMapper.class, Text.class, Text.class, AprioriPhaseReducer.class, Text.class, IntWritable.class,
        TextOutputFormat.class);
        
    job.setJobName(info);
    job.setJarByClass(BigFIMDriver.class);
    
    job.setNumReduceTasks(1);
    
    Configuration conf = job.getConfiguration();
    setConfigurationValues(conf, opt);
    if (nrLines != -1) {
      conf.setLong(NUMBER_OF_LINES_KEY, nrLines);
    }
    
    if (cacheFile != null) {
      addCacheFile(new URI(cacheFile.replace(" ", "%20")), conf);
    }
    
    runJob(job, info);
    
    if (prefixSize <= i
        && job.getCounters().findCounter(COUNTER_GROUPNAME, COUNTER_NRLARGEPREFIXGROUPS).getValue() == 0) {
      return false;
    }
    if (prefixSize < i) {
      System.out.println("[AprioriPhase]: Prefix group length updated! Now " + (i) + " instead of " + prefixSize);
    }
    return true;
  }
  
  private void startCreatePrefixGroups(FIMOptions opt, int phase)
      throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    Path path = new Path(opt.outputDir + separator + "tg" + phase);
    for (FileStatus status : path.getFileSystem(new Configuration()).listStatus(path)) {
      String cacheFile = status.getPath().toString();
      String trieGroupCount = cacheFile.substring(cacheFile.lastIndexOf('/'), cacheFile.length());
      trieGroupCount = trieGroupCount.split("-")[1];
      String outputFile = opt.outputDir + separator + "pg-trieGroup" + trieGroupCount;
      System.out.println(
          "[CreatePrefixGroups]: input: " + opt.inputFile + ", output: " + opt.outputDir + ", cache: " + cacheFile);
          
      Job job = prepareJob(new Path(opt.inputFile), new Path(outputFile), SplitByKTextInputFormat.class,
          ComputeTidListMapper.class, Text.class, IntArrayWritable.class, ComputeTidListReducer.class,
          IntArrayWritable.class, IntMatrixWritable.class, SequenceFileOutputFormat.class);
          
      job.setJobName("Create Prefix Groups");
      job.setJarByClass(BigFIMDriver.class);
      job.setNumReduceTasks(1);
      
      Configuration conf = job.getConfiguration();
      setConfigurationValues(conf, opt);
      conf.setInt(PREFIX_LENGTH_KEY, phase);
      
      addCacheFile(new URI(cacheFile.replace(" ", "%20")), job.getConfiguration());
      
      runJob(job, "Prefix Creation");
    }
  }
  
  private void startMining(FIMOptions opt) throws IOException, ClassNotFoundException, InterruptedException {
    String inputFilesDir = opt.outputDir + separator + "pg" + separator;
    String outputFile = opt.outputDir + separator + OFis;
    System.out.println("[StartMining]: input: " + inputFilesDir + ", output: " + outputFile);
    
    Job job = prepareJob(new Path(inputFilesDir), new Path(outputFile), NoSplitSequenceFileInputFormat.class,
        EclatMinerMapper.class, Text.class, Text.class, EclatMinerReducer.class, Text.class, Text.class,
        TextOutputFormat.class);
        
    job.setJobName("Start Mining");
    job.setJarByClass(BigFIMDriver.class);
    job.setNumReduceTasks(1);
    
    Configuration conf = job.getConfiguration();
    setConfigurationValues(conf, opt);
    
    List<Path> inputPaths = new ArrayList<Path>();
    
    FileStatus[] listStatus = FileSystem.get(conf).globStatus(new Path(inputFilesDir + "bucket*"));
    for (FileStatus fstat : listStatus) {
      inputPaths.add(fstat.getPath());
    }
    
    setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
    
    runJob(job, "Mining");
  }
  
  @Override
  public Configuration getConf() {
    return null;
  }
  
  @Override
  public void setConf(Configuration arg0) {}
}