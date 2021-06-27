<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Administration Management

IoTDB provides users with account privilege management operations, so as to ensure data security.

We will show you basic user privilege management operations through the following specific examples. Detailed SQL syntax and usage details can be found in [SQL Documentation](../Appendix/SQL-Reference.md).
At the same time, in the JAVA programming environment, you can use the [Java JDBC](../API/Programming-JDBC.md) to execute privilege management statements in a single or batch mode.

## Basic Concepts

### User

The user is the legal user of the database. A user corresponds to a unique username and has a password as a means of authentication. Before using a database, a person must first provide a legitimate username and password to make himself/herself a user.

### Privilege

The database provides a variety of operations, and not all users can perform all operations. If a user can perform an operation, the user is said to have the privilege to perform the operation. privileges are divided into data management privilege (such as adding, deleting and modifying data) and authority management privilege (such as creation and deletion of users and roles, granting and revoking of privileges, etc.). Data management privilege often needs a path to limit its effective range, which is a subtree rooted at the path's corresponding node.

### Role

A role is a set of privileges and has a unique role name as an identifier. A user usually corresponds to a real identity (such as a traffic dispatcher), while a real identity may correspond to multiple users. These users with the same real identity tend to have the same privileges. Roles are abstractions that can unify the management of such privileges.

### Default User

There is a default user in IoTDB after the initial installation: root, and the default password is root. This user is an administrator user, who cannot be deleted and has all the privileges. Neither can new privileges be granted to the root user nor can privileges owned by the root user be deleted.

## Privilege Management Operation Examples

According to the [sample data](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), the sample data of IoTDB might belong to different power generation groups such as ln, sgcc, etc. Different power generation groups do not want others to obtain their own database data, so we need to have data privilege isolated at the group layer.

### Create User

We can create two users for ln and sgcc groups, named ln\_write\_user and sgcc\_write\_user, with both passwords being write\_pwd. The SQL statement is:

```
CREATE USER ln_write_user 'write_pwd'
CREATE USER sgcc_write_user 'write_pwd'
```
Then use the following SQL statement to show the user:

```
LIST USER
```
As can be seen from the result shown below, the two users have been created:

```
+---------------+
|           user|
+---------------+
|  ln_write_user|
|           root|
|sgcc_write_user|
+---------------+
Total line number = 3
It costs 0.004s
```

### Grant User Privilege

At this point, although two users have been created, they do not have any privileges, so they can not operate on the database. For example, we use ln_write_user to write data in the database, the SQL statement is:

```
INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:

```
Msg: 602: No permissions for this operation INSERT
```

Now, we grant the two users write privileges to the corresponding storage groups, and try to write data again. The SQL statement is:

```
GRANT USER ln_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.ln
GRANT USER sgcc_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.sgcc
INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
```
The execution result is as follows:

```
IoTDB> GRANT USER ln_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.ln
Msg: The statement is executed successfully.
IoTDB> GRANT USER sgcc_write_user PRIVILEGES 'INSERT_TIMESERIES' on root.sgcc
Msg: The statement is executed successfully.
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp, status) values(1509465600000, true)
Msg: The statement is executed successfully.
```

## Other Instructions

### The Relationship among Users, Privileges and Roles

A Role is a set of privileges, and privileges and roles are both attributes of users. That is, a role can have several privileges and a user can have several roles and privileges (called the user's own privileges).

At present, there is no conflicting privilege in IoTDB, so the real privileges of a user is the union of the user's own privileges and the privileges of the user's roles. That is to say, to determine whether a user can perform an operation, it depends on whether one of the user's own privileges or the privileges of the user's roles permits the operation. The user's own privileges and privileges of the user's roles may overlap, but it does not matter.

It should be noted that if users have a privilege (corresponding to operation A) themselves and their roles contain the same privilege, then revoking the privilege from the users themselves alone can not prohibit the users from performing operation A, since it is necessary to revoke the privilege from the role, or revoke the role from the user. Similarly, revoking the privilege from the users's roles alone can not prohibit the users from performing operation A.

At the same time, changes to roles are immediately reflected on all users who own the roles. For example, adding certain privileges to roles will immediately give all users who own the roles corresponding privileges, and deleting certain privileges will also deprive the corresponding users of the privileges (unless the users themselves have the privileges).

### List of Privileges Included in the System

**List of privileges Included in the System**

|privilege Name|Interpretation|
|:---|:---|
|SET\_STORAGE\_GROUP|set storage groups; path dependent|
|DELETE\_STORAGE\_GROUP|delete storage groups; path dependent|
|CREATE\_TIMESERIES|create timeseries; path dependent|
|INSERT\_TIMESERIES|insert data; path dependent|
|ALTER\_TIMESERIES|alter data; path dependent|
|READ\_TIMESERIES|query data; path dependent|
|DELETE\_TIMESERIES|delete data or timeseries; path dependent|
|CREATE\_USER|create users; path independent|
|DELETE\_USER|delete users; path independent|
|MODIFY\_PASSWORD|modify passwords for all users; path independent; (Those who do not have this privilege can still change their own passwords. )|
|LIST\_USER|list all users; list a user's privileges; list a user's roles with three kinds of operation privileges; path independent|
|GRANT\_USER\_PRIVILEGE|grant user privileges; path independent|
|REVOKE\_USER\_PRIVILEGE|revoke user privileges; path independent|
|GRANT\_USER\_ROLE|grant user roles; path independent|
|REVOKE\_USER\_ROLE|revoke user roles; path independent|
|CREATE\_ROLE|create roles; path independent|
|DELETE\_ROLE|delete roles; path independent|
|LIST\_ROLE|list all roles; list the privileges of a role; list the three kinds of operation privileges of all users owning a role; path independent|
|GRANT\_ROLE\_PRIVILEGE|grant role privileges; path independent|
|REVOKE\_ROLE\_PRIVILEGE|revoke role privileges; path independent|
|CREATE_FUNCTION|register UDFs; path independent|
|DROP_FUNCTION|deregister UDFs; path independent|
|CREATE_TRIGGER|create triggers; path independent|
|DROP_TRIGGER|drop triggers; path independent|
|START_TRIGGER|start triggers; path independent|
|STOP_TRIGGER|stop triggers; path independent|
|TTL|use TTL; path independent|

### Username Restrictions

IoTDB specifies that the character length of a username should not be less than 4, and the username cannot contain spaces.
### Password Restrictions

IoTDB specifies that the character length of a password should have no less than 4 character length, and no spaces. The password is encrypted with MD5.
### Role Name Restrictions

IoTDB specifies that the character length of a role name should have no less than 4 character length, and no spaces.
