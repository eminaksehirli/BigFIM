package ua.ac.be.fpm.util;

public class FIMOptions {
  
  public static final String INPUT_FILE_KEY = "input_file";
  public static final String MIN_SUP_KEY = "minsup";
  public static final String NUMBER_OF_LINES_KEY = "number_of_lines_read";
  public static final String NUMBER_OF_MAPPERS_KEY = "number_of_mappers";
  public static final String OUTPUT_DIR_KEY = "output_dir";
  public static final String PREFIX_LENGTH_KEY = "prefix_length";
  public static final String SUBDB_SIZE = "sub_db_size";
  public static final String DELIMITER_KEY = "delimiter";
  
  public String inputFile = "";
  public String outputDir = "";
  public int minSup = -1;
  public int prefixLength = 1;
  public int nrMappers = -1;
  public String delimiter = " ";
  
  public boolean parseOptions(String args[]) {
    if (args.length % 2 != 0) {
      printHelp();
    }
    for (int i = 0; i < args.length; i += 2) {
      if (args[i].equals("-i") || args[i].equals("--input")) {
        inputFile = args[i + 1];
      } else if (args[i].equals("-o") || args[i].equals("--output")) {
        outputDir = args[i + 1];
      } else if (args[i].equals("-s") || args[i].equals("--support")) {
        minSup = Integer.parseInt(args[i + 1]);
      } else if (args[i].equals("-p") || args[i].equals("--prefixlength")) {
        prefixLength = Integer.parseInt(args[i + 1]);
      } else if (args[i].equals("-m") || args[i].equals("--mappercount")) {
        nrMappers = Integer.parseInt(args[i + 1]);
      } else if (args[i].equals("-d") || args[i].equals("--delimiter")) {
        delimiter = args[i + 1];
      }
    }
    
    return checkRequiredOptions();
  }
  
  private boolean checkRequiredOptions() {
    return !inputFile.equals("") && !outputDir.equals("") && minSup != -1 && prefixLength != -1;
  }
  
  @Override
  public String toString() {
    return "FIMOptions [inputFile=" + inputFile + ", outputPath=" + outputDir + ", minSup=" + minSup
        + ", prefixLength=" + prefixLength + ", nrMappers=" + nrMappers + ", delimiter=" + delimiter + "]";
  }
  
  public void printHelp() {
    System.out.println("Usage:");
    System.out.println("[--input <input> --output <output> --support <Minimum Support>");
    System.out.println("--prefixlength <Prefix Length> --mappercount <Number of Mappers>");
    System.out.println("--delimiter <Delimiter> --countonly <Count Only> --help]");
    System.out.println("Job-Specific Options:");
    System.out.println("\t--input (-i) input");
    System.out.println("\t\tPath to job input directory.");
    System.out.println("\t--output (-o) output");
    System.out.println("\t\tThe directory pathname for output.");
    System.out.println("\t--support (-s) Minimum Support");
    System.out.println("\t\tMinimum support of frequent itemsets found");
    System.out.println("\t--Prefix Length (-p) Prefix Length");
    System.out.println("\t\tLength of prefixes to mine before distributing search space");
    System.out.println("\t--Number of Mappers (-m) Number of Mappers");
    System.out.println("\t\tNumber of mappers to use");
    System.out.println("\t--Delimiter (-d) Delimiter");
    System.out.println("\t\tItem delimiter in the data file ");
    System.out.println("\t--help (-h)");
    System.out.println("\t\tPrint out help");
  }
}