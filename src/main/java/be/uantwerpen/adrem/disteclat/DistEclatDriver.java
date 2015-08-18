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
package be.uantwerpen.adrem.disteclat;

import static be.uantwerpen.adrem.hadoop.util.SplitByKTextInputFormat.NUMBER_OF_CHUNKS;
import static be.uantwerpen.adrem.hadoop.util.Tools.cleanDirs;
import static be.uantwerpen.adrem.hadoop.util.Tools.prepareJob;
import static be.uantwerpen.adrem.util.FIMOptions.DELIMITER_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.MIN_SUP_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.NUMBER_OF_MAPPERS_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.OUTPUT_DIR_KEY;
import static be.uantwerpen.adrem.util.FIMOptions.PREFIX_LENGTH_KEY;
import static java.io.File.separator;
import static java.lang.System.currentTimeMillis;
import static org.apache.hadoop.filecache.DistributedCache.addCacheFile;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;
import static org.apache.hadoop.mapreduce.lib.output.MultipleOutputs.addNamedOutput;

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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import be.uantwerpen.adrem.bigfim.ComputeTidListMapper;
import be.uantwerpen.adrem.eclat.EclatMinerMapper;
import be.uantwerpen.adrem.eclat.EclatMinerReducer;
import be.uantwerpen.adrem.hadoop.util.IntArrayWritable;
import be.uantwerpen.adrem.hadoop.util.IntMatrixWritable;
import be.uantwerpen.adrem.hadoop.util.NoSplitSequenceFileInputFormat;
import be.uantwerpen.adrem.hadoop.util.SplitByKTextInputFormat;
import be.uantwerpen.adrem.util.FIMOptions;

/**
 * Driver class for DistEclat (distributed Eclat) implementation on the Hadoop framework. DistEclat operates in three
 * steps and starts from databases in vertical format. It first mines X-FIs seed elements which it further distributes
 * among available mappers.
 * 
 * The first step consists of reading the vertical database file and reporting the frequent singletons. The latter are
 * distributed by the reducer into distinct groups. The distinct groups are used in the next cycle to compute X-FIs
 * seeds.The seeds are again distributed among a new batch of mappers. The mappers compute closed sets on their local
 * subtrees, indicated by the received prefixes.
 */
public class DistEclatDriver implements Tool {
  
  // output files first MapReduce cycle
  public static final String OSingletonsDistribution = "singletonsDistribution";
  public static final String OSingletonsOrder = "singletonsOrder";
  public static final String OSingletonsTids = "singletonsTids";
  
  // output files second MapReduce cycle
  public static final String OShortFIs = "shortFIs";
  
  // output files third MapReduce cycle
  private static final String OFis = "fis";
  
  // default extension for output file of first reducer
  public static final String rExt = "-r-00000";
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DistEclatDriver(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    FIMOptions opt = new FIMOptions();
    if (!opt.parseOptions(args)) {
      opt.printHelp();
      return -1;
    }
    
    String tmpDir1 = opt.outputDir + separator + "tmp1" + separator;
    String tmpDir2 = opt.outputDir + separator + "prefixes" + separator;
    
    long start = currentTimeMillis();
    cleanDirs(new String[] {opt.outputDir, tmpDir1, tmpDir2});
    readHorizontalDb(tmpDir1, opt);
    startPrefixComputation(tmpDir1, tmpDir2, opt);
    startMining(tmpDir2, opt);
    long end = currentTimeMillis();
    
    System.out.println("[DistEclat]: Total time: " + (end - start) / 1000 + "s");
    return 1;
  }
  
  /**
   * Passes all configuration flags to the Hadoop Configuration framework
   * 
   * @param conf
   *          the Hadoop configuration
   * @param config
   *          the configuration that has user-defined flags
   */
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
  
  /**
   * Starts the first MapReduce cycle. First, the transaction file is partitioned into a number of chunks that is given
   * to different mappers. Each mapper reads a chunk and return the items together with their partial tid-lists. The
   * reducer attaches the partial tid-lists to each other, then discards the infrequent ones and sorts the frequent one
   * based on ascending frequency and divides the singletons among available mappers.
   * 
   * This method generates three files, the frequent singletons (OSingletonsTids), the order file for singletons based
   * on ascending frequency (OSingletonsOrder) and the singletons distribution file (OSingletonsDistribution).
   * 
   * @param outputFile
   * @param opt
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private void readHorizontalDb(String outputFile, FIMOptions opt)
      throws IOException, ClassNotFoundException, InterruptedException {
    System.out.println("[ItemReading]: input: " + opt.inputFile + ", output: " + outputFile);
    
    Job job = prepareJob(new Path(opt.inputFile), new Path(outputFile), SplitByKTextInputFormat.class,
        ComputeTidListMapper.class, Text.class, IntArrayWritable.class, ItemReaderReducer.class, IntWritable.class,
        Writable.class, TextOutputFormat.class);
        
    job.setJobName("Read Singletons");
    job.setJarByClass(DistEclatDriver.class);
    job.setNumReduceTasks(1);
    
    Configuration conf = job.getConfiguration();
    setConfigurationValues(conf, opt);
    
    addNamedOutput(job, OSingletonsDistribution, TextOutputFormat.class, Text.class, Text.class);
    addNamedOutput(job, OSingletonsOrder, TextOutputFormat.class, Text.class, Text.class);
    addNamedOutput(job, OSingletonsTids, SequenceFileOutputFormat.class, IntWritable.class, IntMatrixWritable.class);
    
    runJob(job, "Item Reading");
  }
  
  /**
   * Starts the second MapReduce cycle. Each mapper gets a list of singletons from which it should start building X-FIs.
   * Each mapper uses Eclat to quickly compute the list of X-FIs. The total set of X-FIs is again obtained by the
   * reducer, which then gets divided into independent sets. All sets that have been computed from level 1 to X are
   * already reported. The distribution of seeds is obtained by some allocation scheme, e.g., Round-Robin,
   * Lowest-Frequency, ...
   * 
   * This method generates three files, the frequent itemsets from level 1 to X (OFises), the prefix groups
   * (OPrefixGroups) and the prefix distribution file (OPrefixDistribution).
   * 
   * @param inputDir
   * @param outputDir
   * @param opt
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws URISyntaxException
   */
  private void startPrefixComputation(String inputDir, String outputDir, FIMOptions opt)
      throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
      
    String inputFile = inputDir + separator + OSingletonsDistribution + rExt;
    String singletonsOrderFile = inputDir + separator + OSingletonsOrder + rExt;
    String singletonsTidsFile = inputDir + separator + OSingletonsTids + rExt;
    
    System.out.println("[PrefixComputation]: input: " + inputFile);
    
    Job job = prepareJob(new Path(inputFile), new Path(outputDir), NLineInputFormat.class, PrefixComputerMapper.class,
        Text.class, IntMatrixWritable.class, PrefixComputerReducer.class, IntArrayWritable.class,
        IntMatrixWritable.class, SequenceFileOutputFormat.class);
        
    job.setJobName("Compute Prefixes");
    job.setJarByClass(DistEclatDriver.class);
    job.setNumReduceTasks(1);
    
    Configuration conf = job.getConfiguration();
    setConfigurationValues(conf, opt);
    
    addCacheFile(new URI(singletonsOrderFile.replace(" ", "%20")), job.getConfiguration());
    addCacheFile(new URI(singletonsTidsFile.replace(" ", "%20")), job.getConfiguration());
    
    runJob(job, "Partition Prefixes");
  }
  
  /**
   * Starts the third MapReduce cycle. Each mapper reads the prefix groups assigned to it and computes the collection of
   * closed sets. All information is reported to the reducer which finally writes the output to disk.
   * 
   * 
   * @param inputDir
   * @param config
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws URISyntaxException
   */
  private void startMining(String inputDir, FIMOptions opt)
      throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
      
    String inputFilesDir = inputDir;
    String outputFile = opt.outputDir + separator + OFis;
    System.out.println("[StartMining]: input: " + inputFilesDir + ", output: " + outputFile);
    
    Job job = prepareJob(new Path(inputFilesDir), new Path(outputFile), NoSplitSequenceFileInputFormat.class,
        EclatMinerMapper.class, Text.class, Text.class, EclatMinerReducer.class, Text.class, Text.class,
        TextOutputFormat.class);
        
    job.setJobName("Start Mining");
    job.setJarByClass(DistEclatDriver.class);
    job.setNumReduceTasks(1);
    
    Configuration conf = job.getConfiguration();
    setConfigurationValues(conf, opt);
    
    List<Path> inputPaths = new ArrayList<Path>();
    
    FileStatus[] listStatus = FileSystem.get(conf).globStatus(new Path(inputFilesDir + "bucket*"));
    for (FileStatus fstat : listStatus) {
      inputPaths.add(fstat.getPath());
    }
    
    if (inputPaths.isEmpty()) {
      System.out.println("[StartMining]: No prefixes to extend further");
      return;
    }
    
    setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
    
    runJob(job, "Mining");
  }
  
  @Override
  public Configuration getConf() {
    return null;
  }
  
  @Override
  public void setConf(Configuration arg0) {
  
  }
}