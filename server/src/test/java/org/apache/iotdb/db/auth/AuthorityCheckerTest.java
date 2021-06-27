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
package org.apache.iotdb.db.auth;

import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

public class AuthorityCheckerTest {

  IAuthorizer authorizer;
  User user;
  String nodeName = "root.laptop.d1";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    authorizer = BasicAuthorizer.getInstance();
    user = new User("user", "password");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAuthorityChecker() throws AuthException, IllegalPathException {
    authorizer.createUser(user.getName(), user.getPassword());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.INSERT_TIMESERIES.ordinal());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, PrivilegeType.CREATE_ROLE.ordinal());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, PrivilegeType.CREATE_USER.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.READ_TIMESERIES.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.DELETE_TIMESERIES.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.REVOKE_USER_ROLE.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.GRANT_USER_ROLE.ordinal());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, PrivilegeType.LIST_USER.ordinal());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, PrivilegeType.LIST_ROLE.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.REVOKE_USER_PRIVILEGE.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.GRANT_ROLE_PRIVILEGE.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.GRANT_USER_PRIVILEGE.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.MODIFY_PASSWORD.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.REVOKE_ROLE_PRIVILEGE.ordinal());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, PrivilegeType.DELETE_ROLE.ordinal());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, PrivilegeType.DELETE_USER.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.SET_STORAGE_GROUP.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.CREATE_TIMESERIES.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.DELETE_STORAGE_GROUP.ordinal());
    authorizer.grantPrivilegeToUser(
        user.getName(), nodeName, PrivilegeType.ALTER_TIMESERIES.ordinal());
    authorizer.grantPrivilegeToUser(user.getName(), nodeName, PrivilegeType.TTL.ordinal());
    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.INSERT,
            user.getName()));

    Assert.assertTrue(AuthorityChecker.check("root", null, null, null));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.CREATE_ROLE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.QUERY,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.DROP_INDEX,
            user.getName()));

    // check empty list
    Assert.assertFalse(
        AuthorityChecker.check(
            user.getName(), new ArrayList<>(), OperatorType.INSERT, user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.MODIFY_PASSWORD,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.GRANT_USER_PRIVILEGE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.GRANT_ROLE_PRIVILEGE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.REVOKE_USER_PRIVILEGE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.REVOKE_ROLE_PRIVILEGE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.REVOKE_ROLE_PRIVILEGE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.GRANT_USER_ROLE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.DELETE_USER,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.DELETE_ROLE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.LIST_ROLE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.LIST_USER,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.SET_STORAGE_GROUP,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.CREATE_TIMESERIES,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.DELETE_TIMESERIES,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.FILL,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.GROUP_BY_FILL,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.DELETE_STORAGE_GROUP,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.ALTER_TIMESERIES,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.TTL,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.BATCH_INSERT,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.BATCH_INSERT_ONE_DEVICE,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.MULTI_BATCH_INSERT,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.BATCH_INSERT_ROWS,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.CREATE_ALIGNED_TIMESERIES,
            user.getName()));

    Assert.assertTrue(
        AuthorityChecker.check(
            user.getName(),
            Collections.singletonList(new PartialPath(nodeName)),
            OperatorType.CREATE_MULTI_TIMESERIES,
            user.getName()));
  }
}
