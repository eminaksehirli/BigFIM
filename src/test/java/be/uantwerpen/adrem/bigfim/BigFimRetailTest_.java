package be.uantwerpen.adrem.bigfim;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import be.uantwerpen.adrem.bigfim.BigFIMDriver;
import be.uantwerpen.adrem.disteclat.RetailTest_;

public class BigFimRetailTest_ extends RetailTest_ {
  
  @Override
  protected String runMiner(String inputFile) throws Exception {
    BigFIMDriver.main(new String[] {"-i", inputFile, "-o", "output", "-s", "100", "-p", "2", "-m", "2"});
    
    final String outputFile = "output/" + BigFIMDriverTest_.Output_File_Name;
    return outputFile;
  }
  
  @Override
  protected List<Set<Integer>> prepareExpecteds() throws FileNotFoundException {
    final List<Set<Integer>> expecteds = super.prepareExpecteds();
    
    for (Iterator<Set<Integer>> it = expecteds.iterator(); it.hasNext();) {
      Set<Integer> set = (Set<Integer>) it.next();
      if (set.size() < 3) {
        it.remove();
      }
    }
    
    return expecteds;
  }
}
