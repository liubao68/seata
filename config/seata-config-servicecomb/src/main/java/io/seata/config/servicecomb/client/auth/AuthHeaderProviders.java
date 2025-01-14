/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.seata.config.servicecomb.client.auth;

import io.seata.config.Configuration;
import io.seata.config.servicecomb.SeataServicecombKeys;
import org.apache.commons.lang3.StringUtils;
import org.apache.servicecomb.foundation.auth.AuthHeaderProvider;
import org.apache.servicecomb.foundation.ssl.SSLCustom;
import org.apache.servicecomb.foundation.ssl.SSLOption;
import org.apache.servicecomb.http.client.auth.RequestAuthHeaderProvider;
import org.apache.servicecomb.http.client.common.HttpConfiguration;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhaozhongwei22@163.com
 */
public class AuthHeaderProviders {

    public static RequestAuthHeaderProvider getRequestAuthHeaderProvider(Configuration properties) {
        List<AuthHeaderProvider> authHeaderProviders = new ArrayList<>();
        return getRequestAuthHeaderProvider(authHeaderProviders);
    }

    public static HttpConfiguration.SSLProperties createSslProperties(Configuration properties) {

        HttpConfiguration.SSLProperties sslProperties = new HttpConfiguration.SSLProperties();
        sslProperties.setEnabled(Boolean
            .parseBoolean(properties.getConfig(SeataServicecombKeys.KEY_SSL_ENABLED, SeataServicecombKeys.FALSE)));
        if (sslProperties.isEnabled()) {
            SSLOption option = new SSLOption();
            option.setEngine(properties.getConfig(SeataServicecombKeys.KEY_SSL_ENGINE, SeataServicecombKeys.JDK));
            option.setProtocols(properties.getConfig(SeataServicecombKeys.KEY_SSL_PROTOCOLS, SeataServicecombKeys.TLS));
            option.setCiphers(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_CIPHERS, SeataServicecombKeys.DEFAULT_CIPHERS));
            option.setAuthPeer(Boolean.parseBoolean(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_AUTH_PEER, SeataServicecombKeys.FALSE)));
            option.setCheckCNHost(Boolean.parseBoolean(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_CHECKCN_HOST, SeataServicecombKeys.FALSE)));
            option.setCheckCNWhite(Boolean.parseBoolean(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_CHECKCN_WHITE, SeataServicecombKeys.FALSE)));
            option.setCheckCNWhiteFile(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_CHECKCN_WHITE_FILE, SeataServicecombKeys.EMPTY));
            option.setAllowRenegociate(Boolean.parseBoolean(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_ALLOW_RENEGOTIATE, SeataServicecombKeys.FALSE)));
            option.setStorePath(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_STORE_PATH, SeataServicecombKeys.INTERNAL));
            option.setKeyStore(properties.getConfig(SeataServicecombKeys.KEY_SSL_KEYSTORE, SeataServicecombKeys.EMPTY));
            option.setKeyStoreType(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_KEYSTORE_TYPE, SeataServicecombKeys.PKCS12));
            option.setKeyStoreValue(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_KEYSTORE_VALUE, SeataServicecombKeys.EMPTY));
            option.setTrustStore(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_TRUST_STORE, SeataServicecombKeys.EMPTY));
            option.setTrustStoreType(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_TRUST_STORE_TYPE, SeataServicecombKeys.EMPTY));
            option.setTrustStoreValue(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_TRUST_STORE_VALUE, SeataServicecombKeys.EMPTY));
            option.setCrl(properties.getConfig(SeataServicecombKeys.KEY_SSL_CRL, SeataServicecombKeys.EMPTY));

            SSLCustom sslCustom = SSLCustom.createSSLCustom(
                properties.getConfig(SeataServicecombKeys.KEY_SSL_SSL_CUSTOM_CLASS, SeataServicecombKeys.EMPTY));
            sslProperties.setSslOption(option);
            sslProperties.setSslCustom(sslCustom);
        }
        return sslProperties;
    }

    public static RequestAuthHeaderProvider getRequestAuthHeaderProvider(List<AuthHeaderProvider> authHeaderProviders) {
        return signRequest -> {
            Map<String, String> headers = new HashMap<>(0);
            authHeaderProviders.forEach(authHeaderProvider -> headers.putAll(authHeaderProvider.authHeaders()));
            return headers;
        };
    }

    private static String safeGetProject(String project) {
        if (StringUtils.isEmpty(project)) {
            return project;
        }
        try {
            return URLEncoder.encode(project, SeataServicecombKeys.UTF_8);
        } catch (UnsupportedEncodingException e) {
            return project;
        }
    }
}
