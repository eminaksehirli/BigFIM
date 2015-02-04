package be.uantwerpen.adrem.eclat.util;

import java.util.Arrays;

public class TidList implements Cloneable {
  public int[][] tids;
  
  public TidList(int[][] tids) {
    this.tids = tids;
    for (int i = 0; i < tids.length; i++) {
      if(tids[i] != null && tids[i].length == 0) {
        this.tids[i] = null;
      }
    }
    
    this.tids = tids;
  }
  
  public int size() {
    int sum = 0;
    for (int[] arr : tids) {
      if (arr != null) {
        sum += arr.length;
      }
    }
    return sum;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(tids);
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    TidList other = (TidList) obj;
    if (!Arrays.deepEquals(tids, other.tids)) return false;
    return true;
  }
  
  @Override
  public String toString() {
    return Arrays.deepToString(tids);
  }
  
  @Override
  public Object clone() {
    int[][] newTids = new int[tids.length][];
    for (int i = 0; i < tids.length; i++) {
      if (tids[i] != null) {
        newTids[i] = Arrays.copyOf(tids[i], tids.length);
      }
    }
    
    return new TidList(newTids);
  }
}
