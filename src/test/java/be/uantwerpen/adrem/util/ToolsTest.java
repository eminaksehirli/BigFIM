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
package ua.ac.be.fpm.util;

import static org.junit.Assert.assertEquals;
import static ua.ac.be.fpm.util.Tools.intersect;
import static ua.ac.be.fpm.util.Tools.setDifference;

import org.junit.Test;

import ua.ac.be.fpm.eclat.util.TidList;

public class ToolsTest {
  
  @Test
  public void intersection_Empty_VS_Non_Empty() {
    TidList a = new TidList(new int[3][]);
    TidList b = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    
    assertEquals(a, intersect(a, b));
  }
  
  @Test
  public void intersection_Non_Empty_VS_Empty() {
    TidList a = new TidList(new int[3][]);
    TidList b = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    
    assertEquals(a, intersect(b, a));
  }
  
  @Test
  public void intersection_Non_Empty_VS_Non_Empty_No_Overlap1() {
    TidList a = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    TidList b = new TidList(new int[][] { {6, 7}, {8}, null});
    
    assertEquals(new TidList(new int[3][]), intersect(a, b));
  }
  
  @Test
  public void intersection_Non_Empty_VS_Non_Empty_No_Overlap2() {
    TidList a = new TidList(new int[][] { {6, 7}, {8}});
    TidList b = new TidList(new int[][] { {1, 2}, {3}});
    
    assertEquals(new TidList(new int[2][]), intersect(a, b));
  }
  
  @Test
  public void intersection_Non_Empty_VS_Non_Empty_Overlap1() {
    TidList a = new TidList(new int[][] {{1, 2, 3, 4, 5}});
    TidList b = new TidList(new int[][] {{1, 3, 5, 7, 9}});
    
    assertEquals(new TidList(new int[][] {{1, 3, 5}}), intersect(a, b));
  }
  
  @Test
  public void intersection_Non_Empty_VS_Non_Empty_Overlap2() {
    TidList a = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    TidList b = new TidList(new int[][] { {1, 3}, {3}, {7, 9}});
    
    assertEquals(new TidList(new int[][] { {1}, {3}, null}), intersect(a, b));
  }
  
  @Test
  public void intersection_Non_Empty_VS_Non_Empty_Overlap3() {
    TidList a = new TidList(new int[][] {null, {3}, {4, 5}});
    TidList b = new TidList(new int[][] { {1, 3}, {3}, null});
    
    assertEquals(new TidList(new int[][] {null, {3}, null}), intersect(a, b));
  }
  
  @Test
  public void setDifference_Empty_VS_Non_Empty() {
    TidList a = new TidList(new int[3][]);
    TidList b = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    
    assertEquals(a, setDifference(a, b));
  }
  
  @Test
  public void setDifference_Non_Empty_VS_Empty() {
    TidList a = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    TidList b = new TidList(new int[3][]);
    
    assertEquals(a, setDifference(a, b));
  }
  
  @Test
  public void setDifference_Non_Empty_VS_Non_Empty_No_Overlap1() {
    TidList a = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    TidList b = new TidList(new int[][] { {6, 7}, {8}, {}});
    
    assertEquals(a, setDifference(a, b));
  }
  
  @Test
  public void setDifference_Non_Empty_VS_Non_Empty_No_Overlap2() {
    TidList a = new TidList(new int[][] { {6, 7}, {8}});
    TidList b = new TidList(new int[][] { {1, 2}, {3}});
    
    assertEquals(a, setDifference(a, b));
  }
  
  @Test
  public void setDifference_Non_Empty_VS_Non_Empty_Overlap() {
    TidList a = new TidList(new int[][] {{1, 2, 3, 4, 5}});
    TidList b = new TidList(new int[][] {{1, 3, 5, 7, 9}});
    
    assertEquals(new TidList(new int[][] {{2, 4}}), setDifference(a, b));
  }
  
  @Test
  public void setDifference_Non_Empty_VS_Non_Empty_Overlap_2() {
    TidList a = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    TidList b = new TidList(new int[][] { {1, 3}, {5}, {4, 5}});
    
    assertEquals(new TidList(new int[][] { {2}, {3}, null}), setDifference(a, b));
  }
  
  @Test
  public void setDifference_Non_Empty_VS_Non_Empty_Overlap_3() {
    TidList a = new TidList(new int[][] { {1, 2}, {3}, {4, 5}});
    TidList b = new TidList(new int[][] { {1, 2}, {3}, {4, 6}});
    
    assertEquals(new TidList(new int[][] {null, null, {5}}), setDifference(a, b));
  }
}
