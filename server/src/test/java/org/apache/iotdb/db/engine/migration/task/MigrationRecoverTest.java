package org.apache.iotdb.db.engine.migration.task;

import org.apache.iotdb.db.engine.migration.utils.MigrationLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

public class MigrationRecoverTest extends MigrationTest {
  @Test
  public void testRecoverNotStart() throws IOException {
    // generate log
    MigrationLogger logger = new MigrationLogger(testSgSysDir, 0);
    logger.logSourceFiles(srcFiles, true);
    logger.logTargetDir(targetDir);
    logger.close();

    // recover
    MigrationRecoverTask recoverTask =
        new MigrationRecoverTask(
            logger.getLogFile(),
            this::migrationCallback,
            this::deleteTsFile,
            this::getTsFileListByTimePartition,
            testSgName,
            testSgSysDir);
    recoverTask.recover();

    for (File src : srcFiles) {
      File srcResource = fsFactory.getFile(src.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertFalse(src.exists());
      Assert.assertFalse(srcResource.exists());

      File target = fsFactory.getFile(testSgTier2Dir, src.getName());
      File targetResource = fsFactory.getFile(target.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertTrue(target.exists());
      Assert.assertTrue(targetResource.exists());
    }
  }

  @Test
  public void testRecoverCopyEnd() throws IOException {
    // generate log
    MigrationLogger logger = new MigrationLogger(testSgSysDir, 0);
    logger.logSourceFiles(srcFiles, true);
    logger.logTargetDir(targetDir);
    logger.startMigration();
    logger.startMigrateTsFile(srcFiles.get(0));
    File resource = fsFactory.getFile(srcFiles.get(0).getPath() + TsFileResource.RESOURCE_SUFFIX);
    fsFactory.copyFile(resource, fsFactory.getFile(targetDir, resource.getName()));
    fsFactory.copyFile(srcFiles.get(0), fsFactory.getFile(targetDir, srcFiles.get(0).getName()));
    logger.endCopyTsFile();
    logger.close();

    // recover
    MigrationRecoverTask recoverTask =
        new MigrationRecoverTask(
            logger.getLogFile(),
            this::migrationCallback,
            this::deleteTsFile,
            this::getTsFileListByTimePartition,
            testSgName,
            testSgSysDir);
    recoverTask.recover();

    for (File src : srcFiles) {
      File srcResource = fsFactory.getFile(src.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertFalse(src.exists());
      Assert.assertFalse(srcResource.exists());

      File target = fsFactory.getFile(testSgTier2Dir, src.getName());
      File targetResource = fsFactory.getFile(target.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertTrue(target.exists());
      Assert.assertTrue(targetResource.exists());
    }
  }

  private void migrationCallback(
      File tsFileToDelete,
      File tsFileToLoad,
      boolean sequence,
      BiConsumer<File, File> opsToBothFiles) {
    Assert.assertTrue(tsFileToDelete.exists());
    Assert.assertFalse(tsFileToLoad.exists());

    if (opsToBothFiles != null) {
      opsToBothFiles.accept(tsFileToDelete, tsFileToLoad);
    }

    Assert.assertFalse(tsFileToDelete.exists());
    Assert.assertTrue(tsFileToLoad.exists());
  }

  private void deleteTsFile(File tsfileToBeDeleted) {
    tsfileToBeDeleted.delete();
    fsFactory.getFile(tsfileToBeDeleted.getPath() + TsFileResource.RESOURCE_SUFFIX).delete();
  }

  private List<TsFileResource> getTsFileListByTimePartition(boolean sequence, long timePartition) {
    return srcResources;
  }
}
