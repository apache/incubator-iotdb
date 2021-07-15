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

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.openapi.gen.handler.ApiResponseMessage;
import org.apache.iotdb.openapi.gen.handler.model.User;

import org.glassfish.jersey.internal.util.Base64;

import javax.servlet.annotation.WebFilter;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import java.io.IOException;

@WebFilter("/*")
@Provider
public class AuthorizationFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    IAuthorizer authorizer;
    if ("OPTIONS".equals(containerRequestContext.getMethod())) {

    } else {
      boolean b = false;
      String authzHeader =
          containerRequestContext.getHeaderString(HttpHeaders.AUTHORIZATION); // (1)
      authzHeader = containerRequestContext.getHeaderString("authorization");
      String decoded = Base64.decodeAsString(authzHeader.replace("Basic ", ""));
      String[] split = decoded.split(":");
      User user = new User();
      user.setUsername(split[0]);
      if (split.length != 2) {

        Response resp =
            Response.status(Status.FORBIDDEN)
                .type(MediaType.APPLICATION_JSON)
                .entity(
                    new ApiResponseMessage(
                        ApiResponseMessage.INFO, "username or passowrd is incorrect!"))
                .build();
        containerRequestContext.abortWith(resp);
      } else {
        user.setPassword(split[1]);
        BasicSecurityContext basicSecurityContext =
            new BasicSecurityContext(user, containerRequestContext.getSecurityContext().isSecure());
        containerRequestContext.setSecurityContext(basicSecurityContext);
        try {
          authorizer = BasicAuthorizer.getInstance();
          b = authorizer.login(split[0], split[1]);
          if (!b) {
            Response resp =
                Response.status(Status.FORBIDDEN)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(
                        new ApiResponseMessage(
                            ApiResponseMessage.INFO, "username or passowrd is incorrect!"))
                    .build();
            containerRequestContext.abortWith(resp);
          }
        } catch (AuthException e) {
          e.printStackTrace();
          Response resp =
              Response.status(Status.INTERNAL_SERVER_ERROR)
                  .type(MediaType.APPLICATION_JSON)
                  .entity(new ApiResponseMessage(ApiResponseMessage.ERROR, e.getMessage()))
                  .build();
          containerRequestContext.abortWith(resp);
        }
      }
    }
  }
}
