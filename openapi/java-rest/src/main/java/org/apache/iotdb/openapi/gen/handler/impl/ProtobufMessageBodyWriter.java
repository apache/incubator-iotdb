/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.openapi.gen.handler.impl;

import com.google.protobuf.Message;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.WeakHashMap;

@Provider
@Produces("application/x-protobuf")
public class ProtobufMessageBodyWriter implements MessageBodyWriter<Message> {

  @Override
  public boolean isWriteable(
      Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
    return Message.class.isAssignableFrom(aClass);
    // return false;
  }

  private Map<Object, byte[]> buffer = new WeakHashMap<Object, byte[]>();

  @Override
  public long getSize(
      Message message, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      message.writeTo(baos);
    } catch (IOException e) {
      return -1;
    }
    byte[] bytes = baos.toByteArray();
    buffer.put(message, bytes);
    return bytes.length;
    // return 0;
  }

  @Override
  public void writeTo(
      Message message,
      Class<?> aClass,
      Type type,
      Annotation[] annotations,
      MediaType mediaType,
      MultivaluedMap<String, Object> multivaluedMap,
      OutputStream outputStream)
      throws IOException, WebApplicationException {
    outputStream.write(buffer.remove(message));
  }
}
