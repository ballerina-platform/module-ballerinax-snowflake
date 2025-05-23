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

package io.ballerina.lib.snowflake;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BString;

/**
 * Constants for JDBC client.
 *
 * @since 1.0.0
 */
public final class Constants {
    /**
     * Constants for Endpoint Configs.
     */
    public static final class ClientConfiguration {
        public static final BString URL = StringUtils.fromString("url");
        public static final BString USER = StringUtils.fromString("user");
        public static final BString PASSWORD = StringUtils.fromString("password");
        public static final BString DATASOURCE_NAME = StringUtils.fromString("datasourceName");
        public static final BString REQUEST_GENERATED_KEYS = StringUtils.fromString("requestGeneratedKeys");
        public static final BString CONNECTION_POOL_OPTIONS = StringUtils.fromString("connectionPool");
        public static final BString OPTIONS = StringUtils.fromString("options");
        public static final BString AUTH_CONFIG = StringUtils.fromString("authConfig");
        public static final String BASIC_AUTH_TYPE = "BasicAuth";
        public static final String KEY_BASED_AUTH_TYPE = "KeyBasedAuth";
        public static final BString PROPERTIES = StringUtils.fromString("properties");

        public static final String CONFIG_PRIVATE_KEY_PATH = "privateKeyPath";
        public static final String CONFIG_PRIVATE_KEY_PASSPHRASE = "privateKeyPassphrase";
        public static final String PROPERTY_PRIVATE_KEY_FILE = "private_key_file";
        public static final String PROPERTY_PRIVATE_KEY_FILE_PWD = "private_key_file_pwd";
    }

    public static final String CONNECT_TIMEOUT = ".*(connect).*(timeout).*";
    public static final String POOL_CONNECTION_TIMEOUT = "ConnectionTimeout";
}
