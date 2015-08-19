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
import static com.google.common.collect.Sets.newTreeSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * Prints the itemsets represented by a Trie String. For extra information on Trie Printer see tests.
 */
public class TrieDumper {
  public static final char SYMBOL = '$';
  public static final char SEPARATOR = '|';
  public static final char OPENSUP = '(';
  public static final char CLOSESUP = ')';
  
  static PrintStream out = System.out;
  
  public static void printAsSets(String trieString) {
    List<String> items = newArrayList();
    StringBuilder builder = new StringBuilder();
    trieString = trieString.split("\t")[1];
    for (int i = 0; i < trieString.length(); i++) {
      char c = trieString.charAt(i);
      if (c == SYMBOL) {
        if (items.size() == 0) {
          // out.println("already 0");
        } else {
          items.remove(items.size() - 1);
        }
      } else if (c == SEPARATOR) {
        items.add(builder.toString());
        builder.setLength(0);
      } else if (c == OPENSUP) {
        if (builder.length() != 0) {
          items.add(builder.toString());
          builder.setLength(0);
        }
      } else if (c == CLOSESUP) {
        for (String item : newTreeSet(items)) {
          out.print(item + " ");
        }
        out.println("(" + builder.toString() + ")");
        builder.setLength(0);
      } else {
        builder.append(c);
      }
    }
  }
  
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Usage: TriePrinter encoded-input-file [output-file]");
      System.out.println("\nIf the output file is not given, standart output will be used.");
      return;
    }
    
    if (args.length > 1) {
      TrieDumper.out = new PrintStream(new File(args[1]));
    }
    
    BufferedReader reader = new BufferedReader(new FileReader(args[0]));
    String line;
    while ((line = reader.readLine()) != null) {
      TrieDumper.printAsSets(line);
    }
    reader.close();
  }
}
