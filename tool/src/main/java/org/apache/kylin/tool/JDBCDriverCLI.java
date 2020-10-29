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
package org.apache.kylin.tool;

import org.apache.kylin.jdbc.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 */
public class JDBCDriverCLI {

    private static final Logger logger = LoggerFactory.getLogger(JDBCDriverCLI.class);

    public ResultSet getSQLResult(String sql, String url, String projectName, String user, String password) {

        Driver driver = null;
        ResultSet resultSet = null;
        Connection conn = null;

        try {
            driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
            Properties info = new Properties();
            info.put("user", user);
            info.put("password", password);
            conn = driver.connect("jdbc:kylin://" + url + "/" + projectName, info);
            Statement state = conn.createStatement();
            resultSet = state.executeQuery(sql);
            if (resultSet.next()) {
                return resultSet;
            }
            logger.warn("resultSet is null");
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


        return resultSet;

    }
}