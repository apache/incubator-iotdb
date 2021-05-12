/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.udf.service;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.UDFRegistrationException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.udf.api.UDF;
import org.apache.iotdb.db.query.udf.builtin.BuiltinFunction;
import org.apache.iotdb.db.query.udf.core.context.UDFContext;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UDFRegistrationService implements IService {

  private static final Logger logger = LoggerFactory.getLogger(UDFRegistrationService.class);

  private static final String ULOG_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator
          + "udf"
          + File.separator;
  private static final String LOG_FILE_NAME = ULOG_FILE_DIR + "ulog.txt";
  private static final String TEMPORARY_LOG_FILE_NAME = LOG_FILE_NAME + ".tmp";

  private final ReentrantLock registrationLock;
  private final ConcurrentHashMap<String, UDFRegistrationInformation> registrationInformation;

  private final ReentrantReadWriteLock logWriterLock;
  private UDFLogWriter logWriter;

  private UDFRegistrationService() {
    registrationLock = new ReentrantLock();
    registrationInformation = new ConcurrentHashMap<>();
    logWriterLock = new ReentrantReadWriteLock();
  }

  public void acquireRegistrationLock() {
    registrationLock.lock();
  }

  public void releaseRegistrationLock() {
    registrationLock.unlock();
  }

  public void register(
      String functionName, String className, boolean isTemporary, boolean writeToTemporaryLogFile)
      throws UDFRegistrationException {
    functionName = functionName.toUpperCase();
    validateFunctionName(functionName, className);
    checkIfRegistered(functionName, className, isTemporary);
    doRegister(functionName, className, isTemporary);
    tryAppendRegistrationLog(functionName, className, isTemporary, writeToTemporaryLogFile);
  }

  private static void validateFunctionName(String functionName, String className)
      throws UDFRegistrationException {
    if (!SQLConstant.getNativeFunctionNames().contains(functionName.toLowerCase())) {
      return;
    }

    String errorMessage =
        String.format(
            "Failed to register UDF %s(%s), because the given function name conflicts with the built-in function name",
            functionName, className);

    logger.warn(errorMessage);
    throw new UDFRegistrationException(errorMessage);
  }

  private void checkIfRegistered(String functionName, String className, boolean isTemporary)
      throws UDFRegistrationException {
    UDFRegistrationInformation information = registrationInformation.get(functionName);
    if (information == null) {
      return;
    }

    String errorMessage;
    if (information.isBuiltin()) {
      errorMessage =
          String.format(
              "Failed to register UDF %s(%s), because the given function name is the same as a built-in UDF function name.",
              functionName, className);
    } else {
      if (information.getClassName().equals(className)) {
        errorMessage =
            String.format(
                "Failed to register %sTEMPORARY UDF %s(%s), because a %sTEMPORARY UDF %s(%s) with the same function name and the class name has already been registered.",
                isTemporary ? "" : "non-",
                functionName,
                className,
                information.isTemporary() ? "" : "non-",
                information.getFunctionName(),
                information.getClassName());
      } else {
        errorMessage =
            String.format(
                "Failed to register UDF %s(%s), because a UDF %s(%s) with the same function name but a different class name has already been registered.",
                functionName, className, information.getFunctionName(), information.getClassName());
      }
    }

    logger.warn(errorMessage);
    throw new UDFRegistrationException(errorMessage);
  }

  private void doRegister(String functionName, String className, boolean isTemporary)
      throws UDFRegistrationException {
    acquireRegistrationLock();
    try {
      UDFClassLoader currentActiveClassLoader =
          UDFClassLoaderManager.getInstance().updateAndGetActiveClassLoader();
      updateAllRegisteredClasses(currentActiveClassLoader);

      Class<?> functionClass = Class.forName(className, true, currentActiveClassLoader);
      functionClass.getDeclaredConstructor().newInstance();
      registrationInformation.put(
          functionName,
          new UDFRegistrationInformation(
              functionName, className, isTemporary, false, functionClass));
    } catch (IOException
        | InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException e) {
      String errorMessage =
          String.format(
              "Failed to register UDF %s(%s), because its instance can not be constructed successfully. Exception: %s",
              functionName, className, e);
      logger.warn(errorMessage);
      throw new UDFRegistrationException(errorMessage);
    } finally {
      releaseRegistrationLock();
    }
  }

  private void tryAppendRegistrationLog(
      String functionName, String className, boolean isTemporary, boolean writeToTemporaryLogFile)
      throws UDFRegistrationException {
    if (!writeToTemporaryLogFile || isTemporary) {
      return;
    }

    try {
      appendRegistrationLog(functionName, className);
    } catch (IOException e) {
      registrationInformation.remove(functionName);
      String errorMessage =
          String.format(
              "Failed to append UDF log when registering UDF %s(%s), because %s",
              functionName, className, e);
      logger.error(errorMessage);
      throw new UDFRegistrationException(errorMessage, e);
    }
  }

  private void updateAllRegisteredClasses(UDFClassLoader activeClassLoader)
      throws ClassNotFoundException {
    for (UDFRegistrationInformation information : getRegistrationInformation()) {
      if (!information.isBuiltin()) {
        information.updateFunctionClass(activeClassLoader);
      }
    }
  }

  public void deregister(String functionName) throws UDFRegistrationException {
    functionName = functionName.toUpperCase();
    UDFRegistrationInformation information = registrationInformation.remove(functionName);
    if (information == null) {
      String errorMessage = String.format("UDF %s does not exist.", functionName);
      logger.warn(errorMessage);
      throw new UDFRegistrationException(errorMessage);
    }

    if (information.isBuiltin()) {
      String errorMessage =
          String.format("Built-in function %s can not be deregistered.", functionName);
      logger.error(errorMessage);
      throw new UDFRegistrationException(errorMessage);
    }

    if (!information.isTemporary()) {
      try {
        appendDeregistrationLog(functionName);
      } catch (IOException e) {
        registrationInformation.put(functionName, information);
        String errorMessage =
            String.format(
                "Failed to append UDF log when deregistering UDF %s, because %s", functionName, e);
        logger.error(errorMessage);
        throw new UDFRegistrationException(errorMessage, e);
      }
    }
  }

  private void appendRegistrationLog(String functionName, String className) throws IOException {
    logWriterLock.writeLock().lock();
    try {
      logWriter.register(functionName, className);
    } finally {
      logWriterLock.writeLock().unlock();
    }
  }

  private void appendDeregistrationLog(String functionName) throws IOException {
    logWriterLock.writeLock().lock();
    try {
      logWriter.deregister(functionName);
    } finally {
      logWriterLock.writeLock().unlock();
    }
  }

  public UDF reflect(UDFContext context) throws QueryProcessException {
    String functionName = context.getName().toUpperCase();
    UDFRegistrationInformation information = registrationInformation.get(functionName);
    if (information == null) {
      String errorMessage =
          String.format(
              "Failed to reflect UDF instance, because UDF %s has not been registered.",
              functionName);
      logger.warn(errorMessage);
      throw new QueryProcessException(errorMessage);
    }

    if (!information.isBuiltin()) {
      Thread.currentThread()
          .setContextClassLoader(UDFClassLoaderManager.getInstance().getActiveClassLoader());
    }

    try {
      return (UDF) information.getFunctionClass().getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException e) {
      String errorMessage =
          String.format(
              "Failed to reflect UDF %s(%s) instance, because %s",
              functionName, information.getClassName(), e);
      logger.warn(errorMessage);
      throw new QueryProcessException(errorMessage);
    }
  }

  public UDFRegistrationInformation[] getRegistrationInformation() {
    return registrationInformation.values().toArray(new UDFRegistrationInformation[0]);
  }

  @Override
  public void start() throws StartupException {
    try {
      registerBuiltinFunctions();
      makeDirIfNecessary();
      doRecovery();
      logWriter = new UDFLogWriter(LOG_FILE_NAME);
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  private void registerBuiltinFunctions() {
    for (BuiltinFunction builtinFunction : BuiltinFunction.values()) {
      String functionName = builtinFunction.getFunctionName();
      registrationInformation.put(
          functionName,
          new UDFRegistrationInformation(
              functionName,
              builtinFunction.getClassName(),
              false,
              true,
              builtinFunction.getFunctionClass()));
    }
  }

  private void makeDirIfNecessary() throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(ULOG_FILE_DIR);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private void doRecovery() throws IOException {
    File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(TEMPORARY_LOG_FILE_NAME);
    File logFile = SystemFileFactory.INSTANCE.getFile(LOG_FILE_NAME);

    if (temporaryLogFile.exists()) {
      if (logFile.exists()) {
        recoveryFromLogFile(logFile);
        FileUtils.deleteQuietly(temporaryLogFile);
      } else {
        recoveryFromLogFile(temporaryLogFile);
        SystemFileFactory.INSTANCE.moveFile(temporaryLogFile, logFile);
      }
    } else if (logFile.exists()) {
      recoveryFromLogFile(logFile);
    }
  }

  private void recoveryFromLogFile(File logFile) throws IOException {
    HashMap<String, String> recoveredUDFs = new HashMap<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] data = line.split(",");
        byte type = Byte.parseByte(data[0]);
        if (type == UDFLogWriter.REGISTER_TYPE) {
          recoveredUDFs.put(data[1], data[2]);
        } else if (type == UDFLogWriter.DEREGISTER_TYPE) {
          recoveredUDFs.remove(data[1]);
        }
      }
    }

    for (Entry<String, String> udf : recoveredUDFs.entrySet()) {
      try {
        register(udf.getKey(), udf.getValue(), false, false);
      } catch (UDFRegistrationException ignored) {
        // ignored
      }
    }
  }

  @Override
  public void stop() {
    try {
      writeTemporaryLogFile();

      logWriter.close();
      logWriter.deleteLogFile();

      File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(TEMPORARY_LOG_FILE_NAME);
      File logFile = SystemFileFactory.INSTANCE.getFile(LOG_FILE_NAME);
      SystemFileFactory.INSTANCE.moveFile(temporaryLogFile, logFile);
    } catch (IOException ignored) {
      // ignored
    }
  }

  private void writeTemporaryLogFile() throws IOException {
    UDFLogWriter temporaryLogFile = new UDFLogWriter(TEMPORARY_LOG_FILE_NAME);
    for (UDFRegistrationInformation information : registrationInformation.values()) {
      if (information.isBuiltin() || information.isTemporary()) {
        continue;
      }
      temporaryLogFile.register(information.getFunctionName(), information.getClassName());
    }
    temporaryLogFile.close();
  }

  @TestOnly
  public void deregisterAll() throws UDFRegistrationException {
    for (UDFRegistrationInformation information : getRegistrationInformation()) {
      if (!information.isBuiltin()) {
        deregister(information.getFunctionName());
      }
    }
  }

  @TestOnly
  public void registerBuiltinFunction(String functionName, String className)
      throws ClassNotFoundException {
    ClassLoader classLoader = getClass().getClassLoader();
    Class<?> functionClass = Class.forName(className, true, classLoader);
    functionName = functionName.toUpperCase();
    registrationInformation.put(
        functionName,
        new UDFRegistrationInformation(functionName, className, false, true, functionClass));
  }

  @TestOnly
  public void deregisterBuiltinFunction(String functionName) {
    registrationInformation.remove(functionName.toUpperCase());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UDF_REGISTRATION_SERVICE;
  }

  public static UDFRegistrationService getInstance() {
    return UDFRegistrationService.UDFRegistrationServiceHelper.INSTANCE;
  }

  private static class UDFRegistrationServiceHelper {

    private static final UDFRegistrationService INSTANCE = new UDFRegistrationService();

    private UDFRegistrationServiceHelper() {}
  }
}
