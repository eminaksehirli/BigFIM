package ua.ac.be.fpm.disteclat;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.junit.Test;

import ua.ac.be.fpm.DriverTestHelper;

public abstract class RetailTest_ {
  
  private String inputFile;
  private String inputDir;
  
  public RetailTest_() {
    super();
  }
  
  protected abstract String runMiner(String inputFile) throws Exception;
  
  @Test
  public void retailTest() throws Exception {
    inputDir = "src/test/resources/";
    inputFile = inputDir + "retail.dat";
    
    final String outputFile = runMiner(inputFile);
    
    List<Set<Integer>> results = DriverTestHelper.readResults(outputFile);
    
    List<Set<Integer>> expecteds = prepareExpecteds();
    
    DriverTestHelper helper = new DriverTestHelper(expecteds);
    
    helper.assertAllFrequentsAreFound(results);
    helper.assertAllOfThemFrequent(results);
  }
  
  protected List<Set<Integer>> prepareExpecteds() throws FileNotFoundException {
    List<Set<Integer>> expecteds = newArrayList();
    String expectedsFile = inputDir + "retail_maximal_sup100_min2.dat";
    Scanner sc = new Scanner(new File(expectedsFile));
    
    while (sc.hasNextLine()) {
      String[] lineArr = sc.nextLine().split(" ");
      Set<Integer> itemset = newHashSet();
      for (int i = 0; i < lineArr.length - 1; i++) {
        itemset.add(Integer.valueOf(lineArr[i]));
      }
      expecteds.add(itemset);
    }
    return expecteds;
  }
}