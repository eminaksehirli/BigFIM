package ua.ac.be.fpm.disteclat;

import static java.io.File.separator;
import static ua.ac.be.fpm.DriverTestHelper.Data;
import static ua.ac.be.fpm.DriverTestHelper.MinSup;
import static ua.ac.be.fpm.DriverTestHelper.readResults;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import ua.ac.be.fpm.DriverTestHelper;
import ua.ac.be.fpm.FIMTestCase;
import ua.ac.be.fpm.disteclat.DistEclatDriver;

public class DistEclatDriverTest_ extends FIMTestCase {
  static final String Output_File_Name = "fis/part-r-00000";
  private static boolean distEclatHasRun = false;
  private DriverTestHelper helper;
  private File input;
  private String output;
  private File outputDir;
  private static List<Set<Integer>> results;
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper = new DriverTestHelper();
    outputDir = getTestTempDir("output");
    output = outputDir.getAbsoluteFile() + separator + Output_File_Name;
    input = getTestTempFile("input");
    writeLines(input, Data);
  }
  
  @Test
  public void Dist_Eclat_Finds_Frequent_Itemsets() throws Exception {
    runDistEclatOnce();
    helper.assertAllOfThemFrequent(results);
  }
  
  @Test
  public void Dist_Eclat_Finds_All_The_Closed_Frequent_Itemsets() throws Exception {
    runDistEclatOnce();
    helper.assertAllFrequentsAreFound(results);
  }
  
  @Test
  public void Shorter_Than_Prefix_Frequent_Itemsets_Reported_In_A_Separate_File() throws Exception {
    try {
      DistEclatDriver.main(new String[] {"-i", input.getAbsolutePath(), "-o", outputDir.getAbsolutePath(), "-s",
          (MinSup - 1) + "", "-p", "3", "-m", "4"});
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    distEclatHasRun = false;
    
    final String shortFIsFileName = outputDir.getAbsoluteFile() + separator + DistEclatDriver.OShortFIs;
    helper = new DriverTestHelper(DriverTestHelper.Length_2_FIs);
    
    results = readResults(shortFIsFileName);
    helper.assertAllOfThemFrequent(results);
    helper.assertAllFrequentsAreFound(results);
  }
  
  private void runDistEclatOnce() throws Exception {
    if (!distEclatHasRun) {
      try {
        DistEclatDriver.main(new String[] {"-i", input.getAbsolutePath(), "-o", outputDir.getAbsolutePath(), "-s",
            MinSup + "", "-p", "2", "-m", "4"});
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      results = readResults(output);
    }
    distEclatHasRun = true;
  }
}
