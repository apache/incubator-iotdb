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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.util.JSONObjectUtils;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.auth.role.LocalFileRoleManager;
import org.apache.iotdb.db.auth.user.LocalFileUserManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses an OpenID Connect provider for Authorization / Authentication.
 */
public class OpenIdAuthorizer extends BasicAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(OpenIdAuthorizer.class);
    public static final String IOTDB_ADMIN_ROLE_NAME = "iotdb_admin";
    public static final String OPENID_USER_PREFIX = "openid-";

    private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

    private final RSAPublicKey providerKey;

    /** Stores all claims to the respective user */
    private final Map<String, Claims> loggedClaims = new HashMap<>();

    public OpenIdAuthorizer() throws AuthException, ParseException, IOException, URISyntaxException {
        this(config.getOpenIdProviderUrl());
    }

    OpenIdAuthorizer(JSONObject jwk) throws AuthException {
        super(new LocalFileUserManager(config.getSystemDir() + File.separator + "users"),
                new LocalFileRoleManager(config.getSystemDir() + File.separator + "roles"));
        try {
            providerKey = RSAKey.parse(jwk).toRSAPublicKey();
        } catch (java.text.ParseException | JOSEException e) {
            throw new AuthException("Unable to get OIDC Provider Key from JWK " +  jwk.toString(), e);
        }
        logger.info("Initialized with providerKey: {}", providerKey);
    }

    OpenIdAuthorizer(String providerUrl) throws AuthException, URISyntaxException, ParseException, IOException {
        this(getJWKFromProvider(providerUrl));
    }

    private static JSONObject getJWKFromProvider(String providerUrl) throws URISyntaxException, IOException, ParseException, AuthException {
        if (providerUrl == null) {
            throw new IllegalArgumentException("OpenID Connect Provider URI must be given!");
        }

        // Fetch Metadata
        OIDCProviderMetadata providerMetadata = fetchMetadata(providerUrl);

        logger.debug("Using Provider Metadata: {}", providerMetadata);

        try {
            URL url = new URI(providerMetadata.getJWKSetURI().toString().replace("http", "https")).toURL();
            logger.debug("Using url {}", url);
            return getProviderRSAJWK(url.openStream());
        } catch (IOException e) {
            throw new AuthException("Unable to start the Auth", e);
        }
    }

    private static JSONObject getProviderRSAJWK(InputStream is) throws ParseException {
        // Read all data from stream
        StringBuilder sb = new StringBuilder();
        try (Scanner scanner = new Scanner(is)) {
            while (scanner.hasNext()) {
                sb.append(scanner.next());
            }
        }

        // Parse the data as json
        String jsonString = sb.toString();
        JSONObject json = JSONObjectUtils.parse(jsonString);

        // Find the RSA signing key
        JSONArray keyList = (JSONArray) json.get("keys");
        for (Object key : keyList) {
            JSONObject k = (JSONObject) key;
            if (k.get("use").equals("sig") && k.get("kty").equals("RSA")) {
                return k;
            }
        }
        return null;
    }

    static OIDCProviderMetadata fetchMetadata(String providerUrl) throws URISyntaxException, IOException, ParseException {
        URI issuerURI = new URI(providerUrl);
        URL providerConfigurationURL = issuerURI.resolve(".well-known/openid-configuration").toURL();
        InputStream stream = providerConfigurationURL.openStream();
        // Read all data from URL
        String providerInfo = null;
        try (java.util.Scanner s = new java.util.Scanner(stream)) {
            providerInfo = s.useDelimiter("\\A").hasNext() ? s.next() : "";
        }
        return OIDCProviderMetadata.parse(providerInfo);
    }

    @Override
    public boolean login(String token, String password) throws AuthException {
        if (password != null && !password.isEmpty()) {
            logger.error("JWT Login failed as a non-empty Password was given username (token): {}, password: {}", token, password);
            return false;
        }
        if (token == null || token.isEmpty()) {
            logger.error("JWT Login failed as a Username (token) was empty!");
            return false;
        }
        //This line will throw an exception if it is not a signed JWS (as expected)
        Claims claims;
        try {
            claims = validateToken(token);
        } catch (JwtException e) {
            logger.error("Unable to login the user wit jwt {}", password, e);
            return false;
        }
        logger.debug("JWT was validated successfully!");
        logger.debug("ID: {}", claims.getId());
        logger.debug("Subject: {}", claims.getSubject());
        logger.debug("Issuer: {}", claims.getIssuer());
        logger.debug("Expiration: {}", claims.getExpiration());
        // Create User if not exists
        String iotdbUsername = getUsername(claims);
        if (!super.listAllUsers().contains(iotdbUsername)) {
            logger.info("User {} logs in for first time, storing it locally!", iotdbUsername);
            // We give the user a random password so that no one could hijack them via local login
            super.createUser(iotdbUsername, UUID.randomUUID().toString());
        }
        // Always store claims and user
        this.loggedClaims.put(getUsername(claims), claims);
        return true;
    }

    private Claims validateToken(String token) {
        return Jwts
                .parser()
                // Basically ignore the Expiration Date, if there is any???
                .setAllowedClockSkewSeconds(Long.MAX_VALUE / 1000)
                // .setSigningKey(DatatypeConverter.parseBase64Binary(secret))
                .setSigningKey(providerKey)
                .parseClaimsJws(token)
                .getBody();
    }

    private String getUsername(Claims claims) {
        return OPENID_USER_PREFIX + claims.getSubject();
    }

    private String getUsername(String token) {
        return getUsername(validateToken(token));
    }

    @Override
    public void createUser(String username, String password) throws AuthException {
        throwUnsupportedOperationException();
    }

    private void throwUnsupportedOperationException() {
        throw new UnsupportedOperationException("This operation is not supported for JWT Auth Provider!");
    }

    @Override
    public void deleteUser(String username) throws AuthException {
        throwUnsupportedOperationException();
    }

    /**
     * So not with the token!
     * @param token Usually the JWT but could also be just the name of the user ({@link #getUsername(String)}.
     * @return true if the user is an admin
     */
    @Override
    boolean isAdmin(String token) {
        Claims claims;
        if (this.loggedClaims.containsKey(token)) {
            // This is a username!
            claims = this.loggedClaims.get(token);
        } else {
            // Its a token
            try {
                claims = validateToken(token);
            } catch (JwtException e) {
                logger.warn("Unable to validate token {}!", token, e);
                return false;
            }
        }
        // Get available roles (from keycloack)
        List<String> availableRoles = ((Map<String, List<String>>) claims.get("realm_access")).get("roles");
        if (!availableRoles.contains(IOTDB_ADMIN_ROLE_NAME)) {
            logger.warn("Given Token has no admin rights, is there a ROLE with name {} in 'realm_access' role set?", IOTDB_ADMIN_ROLE_NAME);
            return false;
        }
        return true;
    }

    @Override
    public boolean checkUserPrivileges(String username, String path, int privilegeId)
            throws AuthException {
        if (isAdmin(username)) {
            return true;
        }

        User user = userManager.getUser(getUsername(username));
        if (user == null) {
            throw new AuthException(String.format("No such user : %s", getUsername(username)));
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
    public void updateUserPassword(String username, String newPassword) throws AuthException {
        throwUnsupportedOperationException();
    }

}
