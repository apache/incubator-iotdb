package org.apache.iotdb.tsfile.file.header;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.utils.TestHelper;
import org.apache.iotdb.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PageHeaderTest {
  public static final int UNCOMPRESSED_SIZE = 123456;
  public static final int COMPRESSED_SIZE = 100000;
  public static final int NUM_OF_VALUES = 10000;
  public static final long MAX_TIMESTAMO = 523372036854775806L;
  public static final long MIN_TIMESTAMO = 423372036854775806L;
  public static final TSDataType DATA_TYPE = TSDataType.TEXT;
  public static final int OFFSET = 123456;
  final String PATH = "target/outputPageHeader.tsfile";

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    File file = new File(PATH);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
  }

  @Test
  public void testWriteIntoFile() throws IOException {
    PageHeader header = TestHelper.createSimplePageHeader();
    serialized(header);
    PageHeader readHeader = deSerialized();
    Utils.isPageHeaderEqual(header, readHeader);
    serialized(readHeader);
  }

  private PageHeader deSerialized() {
    FileInputStream fis = null;
    PageHeader header = null;
    try {
      fis = new FileInputStream(new File(PATH));
      header = PageHeader.deserializeFrom(fis, DATA_TYPE);
      return header;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return header;
  }

  private void serialized(PageHeader header) {
    File file = new File(PATH);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      header.serializeTo(fos);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testReadWithOffset() throws IOException {
    PageHeader header = TestHelper.createSimplePageHeader();
    serialized(header, OFFSET);
    PageHeader readHeader = deSerialized(OFFSET);
    Utils.isPageHeaderEqual(header, readHeader);
    serialized(readHeader);
  }

  private PageHeader deSerialized(int offset) {
    FileInputStream fis = null;
    PageHeader header = null;
    try {
      fis = new FileInputStream(new File(PATH));
      header = PageHeader.deserializeFrom(DATA_TYPE, fis.getChannel(), offset, true);
      return header;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return header;
  }

  private void serialized(PageHeader header, int offset) {
    File file = new File(PATH);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
    FileOutputStream fos = null;
    FileChannel fc = null;
    try {
      fos = new FileOutputStream(file);
      fos.write(new byte[offset]);
      header.serializeTo(fos);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}