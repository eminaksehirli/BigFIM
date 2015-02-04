package ua.ac.be.fpm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;

import junit.framework.TestCase;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;

/**
 * This class is based on MahoutTestCase!
 */
public class FIMTestCase extends TestCase {
  
  private File testTempDir;
  
  protected final File getTestTempFile(String name) throws IOException {
    return getTestTempFileOrDir(name, false);
  }
  
  protected final File getTestTempDir(String name) throws IOException {
    return getTestTempFileOrDir(name, true);
  }
  
  private File getTestTempFileOrDir(String name, boolean dir) throws IOException {
    File f = new File(getTestTempDir(), name);
    f.deleteOnExit();
    if (dir && !f.mkdirs()) {
      throw new IOException("Could not make directory " + f);
    }
    return f;
  }
  
  protected final File getTestTempDir() throws IOException {
    if (testTempDir == null) {
      String systemTmpDir = System.getProperty("mahout.test.directory");
      if (systemTmpDir == null) {
        systemTmpDir = "target/";
        systemTmpDir += "test-data";
      }
      long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
      testTempDir = new File(systemTmpDir, "mahout-" + getClass().getSimpleName() + '-' + simpleRandomLong);
      if (!testTempDir.mkdirs()) {
        throw new IOException("Could not create " + testTempDir);
      }
      testTempDir.deleteOnExit();
    }
    return testTempDir;
  }
  
  protected static void setField(Object target, String fieldname, Object value) throws NoSuchFieldException,
      IllegalAccessException {
    Field field = findDeclaredField(target.getClass(), fieldname);
    field.setAccessible(true);
    field.set(target, value);
  }
  
  private static Field findDeclaredField(Class<?> inClass, String fieldname) throws NoSuchFieldException {
    while (!Object.class.equals(inClass)) {
      for (Field field : inClass.getDeclaredFields()) {
        if (field.getName().equalsIgnoreCase(fieldname)) {
          return field;
        }
      }
      inClass = inClass.getSuperclass();
    }
    throw new NoSuchFieldException();
  }
  
  protected static void writeLines(File file, String... lines) throws IOException {
    Writer writer = new OutputStreamWriter(new FileOutputStream(file), Charsets.UTF_8);
    try {
      for (String line : lines) {
        writer.write(line);
        writer.write('\n');
      }
    } finally {
      Closeables.close(writer, false);
    }
  }
}
