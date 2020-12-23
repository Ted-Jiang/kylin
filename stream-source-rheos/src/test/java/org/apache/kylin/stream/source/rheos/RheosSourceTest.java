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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.stream.source.rheos;

import org.junit.Assert;
import org.junit.Test;

public class RheosSourceTest {

    @Test
    public void testGetTopicSubject() throws Exception {
        String topicSubjectInfo = "{\n" +
                "   \"data\":[\n" +
                "      {\n" +
                "         \"createdBy\":\"tatian$$loyalty-dynamiccoupons\",\n" +
                "         \"createdDate\":\"2020-09-21T02:21:04Z\",\n" +
                "         \"lastModifiedBy\":\"tatian$$loyalty-dynamiccoupons\",\n" +
                "         \"lastModifiedDate\":\"2020-09-21T02:21:04Z\",\n" +
                "         \"subject\":\"TransPageKylin\",\n" +
                "         \"compatibility\":\"backward\",\n" +
                "         \"admin\":true\n" +
                "      }\n" +
                "   ]\n" +
                "}";

        String subject = RheosSource.parseAndGetRheosTopicSubject(topicSubjectInfo);
        Assert.assertEquals("TransPageKylin", subject);
    }
}
