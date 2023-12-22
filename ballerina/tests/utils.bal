// Copyright (c) 2023, WSO2 LLC. (http://www.wso2.com) All Rights Reserved.
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

import ballerina/io;
import ballerinax/mysql.driver as _;

isolated function getByteColumnChannel() returns io:ReadableByteChannel|error {
    io:ReadableByteChannel byteChannel = check io:openReadableFile("./tests/resources/files/byteValue.txt");
    return byteChannel;
}

isolated function getBlobColumnChannel() returns io:ReadableByteChannel|error {
    io:ReadableByteChannel byteChannel = check io:openReadableFile("./tests/resources/files/blobValue.txt");
    return byteChannel;
}

isolated function getClobColumnChannel() returns io:ReadableCharacterChannel|error {
    io:ReadableByteChannel byteChannel = check io:openReadableFile("./tests/resources/files/clobValue.txt");
    io:ReadableCharacterChannel sourceChannel = new (byteChannel, "UTF-8");
    return sourceChannel;
}

isolated function getTextColumnChannel() returns io:ReadableCharacterChannel|error {
    io:ReadableByteChannel byteChannel = check io:openReadableFile("./tests/resources/files/clobValue.txt");
    io:ReadableCharacterChannel sourceChannel = new (byteChannel, "UTF-8");
    return sourceChannel;
}
