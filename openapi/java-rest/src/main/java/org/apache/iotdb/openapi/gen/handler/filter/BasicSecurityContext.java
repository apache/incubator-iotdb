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
package org.apache.iotdb.openapi.gen.handler.filter;

import org.apache.iotdb.openapi.gen.handler.model.User;

import javax.ws.rs.core.SecurityContext;

import java.security.Principal;

public class BasicSecurityContext implements SecurityContext {

  private final User user;
  private final boolean secure;

  public BasicSecurityContext(User user, boolean secure) {
    this.user = user;
    this.secure = secure;
  }

  @Override
  public Principal getUserPrincipal() {
    return new Principal() {
      @Override
      public String getName() {
        return user.getUsername();
      }
    };
  }

  public User getUser() {
    return user;
  }

  @Override
  public boolean isUserInRole(String s) {
    return false;
  }

  @Override
  public boolean isSecure() {
    return false;
  }

  @Override
  public String getAuthenticationScheme() {
    return null;
  }
}
