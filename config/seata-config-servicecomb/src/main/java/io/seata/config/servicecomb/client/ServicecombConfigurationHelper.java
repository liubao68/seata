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

package io.seata.config.servicecomb.client;

import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.config.Configuration;
import io.seata.config.servicecomb.SeataServicecombKeys;
import io.seata.config.servicecomb.client.auth.AuthHeaderProviders;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.servicecomb.config.center.client.AddressManager;
import org.apache.servicecomb.config.center.client.ConfigCenterClient;
import org.apache.servicecomb.config.center.client.ConfigCenterManager;
import org.apache.servicecomb.config.center.client.model.QueryConfigurationsRequest;
import org.apache.servicecomb.config.center.client.model.QueryConfigurationsResponse;
import org.apache.servicecomb.config.common.ConfigConverter;
import org.apache.servicecomb.config.kie.client.KieClient;
import org.apache.servicecomb.config.kie.client.KieConfigManager;
import org.apache.servicecomb.config.kie.client.model.KieAddressManager;
import org.apache.servicecomb.config.kie.client.model.KieConfiguration;
import org.apache.servicecomb.foundation.auth.AuthHeaderProvider;
import org.apache.servicecomb.http.client.auth.RequestAuthHeaderProvider;
import org.apache.servicecomb.http.client.common.HttpTransport;
import org.apache.servicecomb.http.client.common.HttpTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhaozhongwei22@163.com
 */
public class ServicecombConfigurationHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServicecombConfigurationHelper.class);

    volatile ConfigCenterManager configCenterManager;

    volatile KieConfigManager kieConfigManager;

    HttpTransport httpTransport;

    private boolean isKie = false;

    private ConfigCenterConfiguration configCenterConfiguration;

    private KieConfigConfiguration kieConfigConfiguration;

    private KieConfiguration kieConfiguration;

    private ConfigConverter configConverter;

    private Configuration properties;

    public ServicecombConfigurationHelper(Configuration properties) {

        this.properties = properties;
        configConverter = initConfigConverter();

        configCenterConfiguration = new ConfigCenterConfiguration(properties);
        kieConfigConfiguration = new KieConfigConfiguration(properties);

        addConfigCenterProperties(properties);

    }

    /***/
    private ConfigConverter initConfigConverter() {
        String fileSources =
            properties.getConfig(SeataServicecombKeys.KEY_CONFIG_FILESOURCE, SeataServicecombKeys.EMPTY);
        if (StringUtils.isEmpty(fileSources)) {
            configConverter = new ConfigConverter(null);
        } else {
            configConverter = new ConfigConverter(Arrays.asList(fileSources.split(SeataServicecombKeys.COMMA)));
        }
        return configConverter;
    }

    private void addConfigCenterProperties(Configuration properties) {
        isKie = SeataServicecombKeys.KIE
            .equals(properties.getConfig(SeataServicecombKeys.KEY_CONFIG_ADDRESSTYPE, SeataServicecombKeys.KIE));
        RequestConfig.Builder config = HttpTransportFactory.defaultRequestConfig();

        this.setTimeOut(config);

        httpTransport = HttpTransportFactory.createHttpTransport(AuthHeaderProviders.createSslProperties(properties),
            getRequestAuthHeaderProvider(), config.build());

        if (isKie) {
            configKieClient(properties);
        } else {
            configCenterClient(properties);
        }
    }

    private static RequestAuthHeaderProvider getRequestAuthHeaderProvider() {
        List<AuthHeaderProvider> authHeaderProviders = EnhancedServiceLoader.loadAll(AuthHeaderProvider.class);
        return signRequest -> {
            Map<String, String> headers = new HashMap<>();
            authHeaderProviders.forEach(provider -> headers.putAll(provider.authHeaders()));
            return headers;
        };
    }

    private void configCenterClient(Configuration properties) {
        QueryConfigurationsRequest queryConfigurationsRequest =
            configCenterConfiguration.createQueryConfigurationsRequest();
        AddressManager addressManager = configCenterConfiguration.createAddressManager();
        if (addressManager == null) {
            LOGGER.warn("Config center address is not configured and will not enable dynamic config.");
            return;
        }
        ConfigCenterClient configCenterClient = new ConfigCenterClient(addressManager, httpTransport);
        try {
            QueryConfigurationsResponse response = configCenterClient.queryConfigurations(queryConfigurationsRequest);
            if (response.isChanged()) {
                configConverter.updateData(response.getConfigurations());
            }
            queryConfigurationsRequest.setRevision(response.getRevision());

        } catch (Exception e) {
            LOGGER.warn("set up Servicesomb configuration failed at startup.", e);
        }
        configCenterManager = new ConfigCenterManager(configCenterClient, EventManager.getEventBus(), configConverter);
        configCenterManager.setQueryConfigurationsRequest(queryConfigurationsRequest);
        configCenterManager.startConfigCenterManager();
    }

    private void configKieClient(Configuration properties) {

        kieConfiguration = kieConfigConfiguration.createKieConfiguration();

        KieAddressManager kieAddressManager = kieConfigConfiguration.createKieAddressManager();
        if (kieAddressManager == null) {
            LOGGER.warn("Kie address is not configured and will not enable dynamic config.");
            return;
        }

        KieClient kieClient = new KieClient(kieAddressManager, httpTransport, kieConfiguration);

        kieConfigManager =
            new KieConfigManager(kieClient, EventManager.getEventBus(), kieConfiguration, configConverter);
        kieConfigManager.firstPull();
        kieConfigManager.startConfigKieManager();
    }

    private void setTimeOut(RequestConfig.Builder config) {
        if (!isKie) {
            return;
        }
        String test =
            properties.getConfig(SeataServicecombKeys.KEY_SERVICE_ENABLELONGPOLLING, SeataServicecombKeys.TRUE);
        if (Boolean.parseBoolean(test)) {
            int pollingWaitInSeconds =
                Integer.valueOf(properties.getConfig(SeataServicecombKeys.KEY_SERVICE_POLLINGWAITSEC,
                    SeataServicecombKeys.DEFAULT_SERVICE_POLLINGWAITSEC));
            config.setSocketTimeout(pollingWaitInSeconds * 1000 + 5000);
        }
    }

    public Map<String, Object> getCurrentData() {
        return configConverter.getCurrentData();
    }

}
