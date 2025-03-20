/*
 * Copyright (c) 2022, WSO2 LLC. (http://www.wso2.com) All Rights Reserved.
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
 */
 
package io.ballerina.lib.snowflake.nativeimpl;

import io.ballerina.lib.snowflake.Constants;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;

import java.util.Locale;
import java.util.Properties;

import static io.ballerina.lib.snowflake.Constants.ClientConfiguration.BASIC_AUTH_TYPE;
import static io.ballerina.lib.snowflake.Constants.ClientConfiguration.KEY_BASED_AUTH_TYPE;

/**
 * This class will include the native method implementation for the JDBC client.
 *
 * @since 1.0.0
 */
public class ClientProcessor {

    public static Object createClient(BObject client, BMap<BString, Object> clientConfig,
                                      BMap<BString, Object> globalPool) {
        String url = clientConfig.getStringValue(Constants.ClientConfiguration.URL).getValue();
        if (!isJdbcUrlValid(url)) {
            return ErrorGenerator.getSQLApplicationError("Invalid JDBC URL: " + url);
        }

        BMap options = clientConfig.getMapValue(Constants.ClientConfiguration.OPTIONS);
        BMap<BString, Object> properties = ValueCreator.createMapValue();
        Properties poolProperties = null;

        String datasourceName = null;
        if (options != null) {
            properties = options.getMapValue(Constants.ClientConfiguration.PROPERTIES);
            BString dataSourceNamVal = options.getStringValue(Constants.ClientConfiguration.DATASOURCE_NAME);
            datasourceName = dataSourceNamVal == null ? null : dataSourceNamVal.getValue();
            if (properties != null) {
                for (BString propKey : properties.getKeys()) {
                    if (propKey.getValue().toLowerCase(Locale.ENGLISH).matches(Constants.CONNECT_TIMEOUT)) {
                        poolProperties = new Properties();
                        poolProperties.setProperty(Constants.POOL_CONNECTION_TIMEOUT,
                                properties.getStringValue(propKey).getValue());
                    }
                }
            }
        }

        BMap connectionPool = clientConfig.getMapValue(Constants.ClientConfiguration.CONNECTION_POOL_OPTIONS);

        BMap authConfigs = clientConfig.getMapValue(Constants.ClientConfiguration.AUTH_CONFIG);
        String authType = TypeUtils.getType(authConfigs).getName();

        SQLDatasource.SQLDatasourceParams sqlDatasourceParams;
        BString userVal = authConfigs.getStringValue(Constants.ClientConfiguration.USER);
        String user = userVal == null ? null : userVal.getValue();

        if (BASIC_AUTH_TYPE.equals(authType)) {
            BString passwordVal = authConfigs.getStringValue(Constants.ClientConfiguration.PASSWORD);
            String password = passwordVal == null ? null : passwordVal.getValue();

            sqlDatasourceParams = new SQLDatasource.SQLDatasourceParams()
                    .setUrl(url)
                    .setUser(user)
                    .setPassword(password)
                    .setDatasourceName(datasourceName)
                    .setOptions(properties)
                    .setPoolProperties(poolProperties)
                    .setConnectionPool(connectionPool, globalPool);
        } else if (KEY_BASED_AUTH_TYPE.equals((authType))) {
            BString privateKeyPathValue = authConfigs.getStringValue(StringUtils.fromString("privateKeyPath"));
            BString keyPassphraseValue = authConfigs.getStringValue(StringUtils.fromString("privateKeyPassphrase"));
            if (properties != null) {
                properties.put(StringUtils.fromString("private_key_file"), privateKeyPathValue);
                properties.put(StringUtils.fromString("private_key_file_pwd"), keyPassphraseValue);
            } else {
                properties = ValueCreator.createMapValue();
                properties.put(StringUtils.fromString("private_key_file"), privateKeyPathValue);
                properties.put(StringUtils.fromString("private_key_file_pwd"), keyPassphraseValue);
            }
            sqlDatasourceParams = new SQLDatasource.SQLDatasourceParams()
                    .setUrl(url)
                    .setUser(user)
                    .setDatasourceName(datasourceName)
                    .setOptions(properties)
                    .setPoolProperties(poolProperties)
                    .setConnectionPool(connectionPool, globalPool);
        } else {
            return ErrorGenerator.getSQLApplicationError("Invalid Auth Type: " + authType);
        }

        boolean executeGKFlag = false;
        boolean batchExecuteGKFlag = false;

        return io.ballerina.stdlib.sql.nativeimpl.ClientProcessor.createClient(client, sqlDatasourceParams,
                                                                               executeGKFlag, batchExecuteGKFlag);
    }

    // Unable to perform a complete validation since URL differs based on the database.
    private static boolean isJdbcUrlValid(String jdbcUrl) {
        return !jdbcUrl.isEmpty() && jdbcUrl.trim().startsWith("jdbc:");
    }

    public static Object close(BObject client) {
        return io.ballerina.stdlib.sql.nativeimpl.ClientProcessor.close(client);
    }
}
