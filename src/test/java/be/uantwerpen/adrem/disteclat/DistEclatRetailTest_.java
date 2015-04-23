package be.uantwerpen.adrem.disteclat;

import be.uantwerpen.adrem.disteclat.DistEclatDriver;


public class DistEclatRetailTest_ extends RetailTest_ {
  
  @Override
  protected String runMiner(String inputFile) throws Exception {
    DistEclatDriver.main(new String[] {"-i", inputFile, "-o", "output", "-s", "100", "-p", "2", "-m", "2"});
    
    final String outputFile = "output/" + DistEclatDriverTest_.Output_File_Name;
    return outputFile;
  }
}
