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
package org.apache.iotdb.db.auth.authorizer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.auth.role.IRoleManager;
import org.apache.iotdb.db.auth.user.IUserManager;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.AuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicAuthorizer implements IAuthorizer, IService {

  private static final Logger logger = LoggerFactory.getLogger(BasicAuthorizer.class);
  private static final Set<Integer> ADMIN_PRIVILEGES;
  private static final String NO_SUCH_ROLE_EXCEPTION = "No such role : %s";
  private static final String NO_SUCH_USER_EXCEPTION = "No such user : %s";

  static {
    ADMIN_PRIVILEGES = new HashSet<>();
    for (int i = 0; i < PrivilegeType.values().length; i++) {
      ADMIN_PRIVILEGES.add(i);
    }
  }

  IUserManager userManager;
  IRoleManager roleManager;

  BasicAuthorizer(IUserManager userManager, IRoleManager roleManager) throws AuthException {
    this.userManager = userManager;
    this.roleManager = roleManager;
    init();
  }

  protected void init() throws AuthException {
    userManager.reset();
    roleManager.reset();
    logger.info("Initialization of Authorizer completes");
  }

  /**
   * function for getting the instance of the local file authorizer.
   */
  public static IAuthorizer getInstance() throws AuthException {
    if (InstanceHolder.instance == null) {
      throw new AuthException("Authorizer uninitialized");
    }
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {
    private static IAuthorizer instance;

    static {
        Class<BasicAuthorizer> c;
        try {
          c = (Class<BasicAuthorizer>) Class.forName(IoTDBDescriptor.getInstance().getConfig().getAuthorizerProvider());
          logger.info("Authorizer provider class: {}", IoTDBDescriptor.getInstance().getConfig().getAuthorizerProvider());
          instance = c.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          instance = null;
          //startup failed.
          throw new IllegalStateException("Authorizer could not be initialized!", e);
        }
    }
  }



  /** Checks if a user has admin privileges */
  abstract boolean isAdmin(String username);

  @Override
  public boolean login(String username, String password) throws AuthException {
    User user = userManager.getUser(username);
    return user != null && password != null && user.getPassword().equals(AuthUtils.encryptPassword(password));
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    if (!userManager.createUser(username, password)) {
      throw new AuthException(String.format("User %s already exists", username));
    }
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    if (isAdmin(username)) {
      throw new AuthException("Default administrator cannot be deleted");
    }
    if (!userManager.deleteUser(username)) {
      throw new AuthException(String.format("User %s does not exist", username));
    }
  }

  @Override
  public void grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    String newPath = path;
    if (isAdmin(username)) {
      throw new AuthException("Invalid operation, administrator already has all privileges");
    }
    if (!PrivilegeType.isPathRelevant(privilegeId)) {
      newPath = IoTDBConstant.PATH_ROOT;
    }
    if (!userManager.grantPrivilegeToUser(username, newPath, privilegeId)) {
      throw new AuthException(String.format(
          "User %s already has %s on %s", username, PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    if (isAdmin(username)) {
      throw new AuthException("Invalid operation, administrator must have all privileges");
    }
    String p = path;
    if (!PrivilegeType.isPathRelevant(privilegeId)) {
      p = IoTDBConstant.PATH_ROOT;
    }
    if (!userManager.revokePrivilegeFromUser(username, p, privilegeId)) {
      throw new AuthException(String.format("User %s does not have %s on %s", username,
          PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    if (!roleManager.createRole(roleName)) {
      throw new AuthException(String.format("Role %s already exists", roleName));
    }
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    boolean success = roleManager.deleteRole(roleName);
    if (!success) {
      throw new AuthException(String.format("Role %s does not exist", roleName));
    } else {
      // proceed to revoke the role in all users
      List<String> users = userManager.listAllUsers();
      for (String user : users) {
        try {
          userManager.revokeRoleFromUser(roleName, user);
        } catch (AuthException e) {
          logger.warn(
              "Error encountered when revoking a role {} from user {} after deletion, because {}",
              roleName, user, e);
        }
      }
    }
  }

  @Override
  public void grantPrivilegeToRole(String roleName, String path, int privilegeId)
      throws AuthException {
    String p = path;
    if (!PrivilegeType.isPathRelevant(privilegeId)) {
      p = IoTDBConstant.PATH_ROOT;
    }
    if (!roleManager.grantPrivilegeToRole(roleName, p, privilegeId)) {
      throw new AuthException(String.format("Role %s already has %s on %s", roleName,
          PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, String path, int privilegeId)
      throws AuthException {
    String p = path;
    if (!PrivilegeType.isPathRelevant(privilegeId)) {
      p = IoTDBConstant.PATH_ROOT;
    }
    if (!roleManager.revokePrivilegeFromRole(roleName, p, privilegeId)) {
      throw new AuthException(String.format("Role %s does not have %s on %s", roleName,
          PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    Role role = roleManager.getRole(roleName);
    if (role == null) {
      throw new AuthException(String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
    // the role may be deleted before it ts granted to the user, so a double check is necessary.
    boolean success = userManager.grantRoleToUser(roleName, username);
    if (success) {
      role = roleManager.getRole(roleName);
      if (role == null) {
        throw new AuthException(String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
      }
    } else {
      throw new AuthException(String.format("User %s already has role %s",
          username, roleName));
    }
  }

  @Override
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    Role role = roleManager.getRole(roleName);
    if (role == null) {
      throw new AuthException(String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
    if (!userManager.revokeRoleFromUser(roleName, username)) {
      throw new AuthException(String.format("User %s does not have role %s", username,
          roleName));
    }
  }

  @Override
  public Set<Integer> getPrivileges(String username, String path) throws AuthException {
    if (isAdmin(username)) {
      return ADMIN_PRIVILEGES;
    }
    User user = userManager.getUser(username);
    if (user == null) {
      throw new AuthException(String.format(NO_SUCH_USER_EXCEPTION, username));
    }
    // get privileges of the user
    Set<Integer> privileges = user.getPrivileges(path);
    // mergeUnseq the privileges of the roles of the user
    for (String roleName : user.getRoleList()) {
      Role role = roleManager.getRole(roleName);
      if (role != null) {
        privileges.addAll(role.getPrivileges(path));
      }
    }
    return privileges;
  }

  @Override
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    if (!userManager.updateUserPassword(username, newPassword)) {
      throw new AuthException("password " + newPassword + " is illegal");
    }
  }

  @Override
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    if (isAdmin(username)) {
      return true;
    }
    User user = userManager.getUser(username);
    if (user == null) {
      throw new AuthException(String.format(NO_SUCH_USER_EXCEPTION, username));
    }
    // get privileges of the user
    if (user.checkPrivilege(path, privilegeId)) {
      return true;
    }
    // mergeUnseq the privileges of the roles of the user
    for (String roleName : user.getRoleList()) {
      Role role = roleManager.getRole(roleName);
      if (role.checkPrivilege(path, privilegeId)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Map<String, Boolean> getAllUserWaterMarkStatus() {
    Map<String, Boolean> userWaterMarkStatus = new HashMap<>();
    List<String> allUsers = listAllUsers();
    for (String user : allUsers) {
      try {
        userWaterMarkStatus.put(user, isUserUseWaterMark(user));
      } catch (AuthException e) {
        logger.error(String.format(NO_SUCH_USER_EXCEPTION, user));
      }
    }
    return userWaterMarkStatus;
  }

  @Override
  public Map<String, User> getAllUsers() {
    Map<String, User> allUsers = new HashMap<>();
    List<String> userNames = listAllUsers();
    for (String userName : userNames) {
      try {
        allUsers.put(userName, getUser(userName));
      } catch (AuthException e) {
        logger.error(String.format("get all users failed, No such user : %s", userName));
      }
    }
    return allUsers;
  }


  @Override
  public Map<String, Role> getAllRoles() {
    Map<String, Role> allRoles = new HashMap<>();
    List<String> roleNames = listAllRoles();
    for (String roleName : roleNames) {
      try {
        allRoles.put(roleName, getRole(roleName));
      } catch (AuthException e) {
        logger.error(String.format("get all roles failed, No such role : %s", roleName));
      }
    }
    return allRoles;
  }


  @Override
  public void reset() throws AuthException {
    init();
  }

  @Override
  public void start() throws StartupException {
    try {
      init();
    } catch (AuthException e) {
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {

  }

  @Override
  public ServiceType getID() {
    return ServiceType.AUTHORIZATION_SERVICE;
  }

  @Override
  public List<String> listAllUsers() {
    return userManager.listAllUsers();
  }

  @Override
  public List<String> listAllRoles() {
    return roleManager.listAllRoles();
  }

  @Override
  public Role getRole(String roleName) throws AuthException {
    return roleManager.getRole(roleName);
  }

  @Override
  public User getUser(String username) throws AuthException {
    return userManager.getUser(username);
  }

  @Override
  public boolean isUserUseWaterMark(String userName) throws AuthException {
    return userManager.isUserUseWaterMark(userName);
  }

  @Override
  public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
    userManager.setUserUseWaterMark(userName, useWaterMark);
  }

  @Override
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    userManager.replaceAllUsers(users);
  }

  @Override
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    roleManager.replaceAllRoles(roles);
  }
}
