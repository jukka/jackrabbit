/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.fs.db;

import org.apache.jackrabbit.core.fs.FileSystemException;
import org.apache.log4j.Logger;

/**
 * <code>OracleFileSystem</code> is a JDBC-based <code>FileSystem</code>
 * implementation for Jackrabbit that persists file system entries in an
 * Oracle database.
 * <p/>
 * It is configured through the following properties:
 * <ul>
 * <li><code>driver</code>: the FQN name of the JDBC driver class
 * (default: <code>"oracle.jdbc.OracleDriver"</code>)</li>
 * <li><code>schema</code>: type of schema to be used
 * (default: <code>"oracle"</code>)</li>
 * <li><code>url</code>: the database url (e.g.
 * <code>"jdbc:oracle:thin:@[host]:[port]:[sid]"</code>)</li>
 * <li><code>user</code>: the database user</li>
 * <li><code>password</code>: the user's password</li>
 * <li><code>schemaObjectPrefix</code>: prefix to be prepended to schema objects</li>
 * </ul>
 * See also {@link DbFileSystem}.
 * <p/>
 * The following is a fragment from a sample configuration:
 * <pre>
 *   &lt;FileSystem class="org.apache.jackrabbit.core.fs.db.OracleFileSystem"&gt;
 *       &lt;param name="url" value="jdbc:oracle:thin:@127.0.0.1:1521:orcl"/&gt;
 *       &lt;param name="user" value="scott"/&gt;
 *       &lt;param name="password" value="tiger"/&gt;
 *       &lt;param name="schemaObjectPrefix" value="rep_"/&gt;
 *  &lt;/FileSystem&gt;
 * </pre>
 */
public class OracleFileSystem extends DbFileSystem {

    /**
     * Logger instance
     */
    private static Logger log = Logger.getLogger(OracleFileSystem.class);

    /**
     * Creates a new <code>OracleFileSystem</code> instance.
     */
    public OracleFileSystem() {
        // preset some attributes to reasonable defaults
        schema = "oracle";
        driver = "oracle.jdbc.OracleDriver";
        schemaObjectPrefix = "";
        user = "";
        password = "";
        initialized = false;
    }

    //-----------------------------------------------< DbFileSystem overrides >
    /**
     * {@inheritDoc}
     * <p/>
     * Since Oracle treats emtpy strings and BLOBs as null values the SQL
     * statements had to adapated accordingly. The following changes were
     * necessary:
     * <ul>
     * <li>The distinction between file and folder entries is based on
     * FSENTRY_LENGTH being null/not null rather than FSENTRY_DATA being
     * null/not null because FSENTRY_DATA of a 0-length (i.e. empty) file is
     * null in Oracle.</li>
     * <li>Folder entries: Since the root folder has an empty name (which would
     * be null in Oracle), an empty name is automatically converted and treated
     * as " ".</li>
     * </ul>
     */
    public void init() throws FileSystemException {
        if (initialized) {
            throw new IllegalStateException("already initialized");
        }

        try {
            // setup jdbc connection
            initConnection();

            // check if schema objects exist and create them if necessary
            checkSchema();

            // prepare statements
            insertFileStmt = con.prepareStatement("insert into "
                    + schemaObjectPrefix + "FSENTRY "
                    + "(FSENTRY_PATH, FSENTRY_NAME, FSENTRY_DATA, "
                    + "FSENTRY_LASTMOD, FSENTRY_LENGTH) "
                    + "values (?, ?, ?, ?, ?)");

            insertFolderStmt = con.prepareStatement("insert into "
                    + schemaObjectPrefix + "FSENTRY "
                    + "(FSENTRY_PATH, FSENTRY_NAME, FSENTRY_LASTMOD, FSENTRY_LENGTH) "
                    + "values (?, nvl(?, ' '), ?, null)");

            updateDataStmt = con.prepareStatement("update "
                    + schemaObjectPrefix + "FSENTRY "
                    + "set FSENTRY_DATA = ?, FSENTRY_LASTMOD = ?, FSENTRY_LENGTH = ? "
                    + "where FSENTRY_PATH = ? and FSENTRY_NAME = ? "
                    + "and FSENTRY_LENGTH is not null");

            updateLastModifiedStmt = con.prepareStatement("update "
                    + schemaObjectPrefix + "FSENTRY set FSENTRY_LASTMOD = ? "
                    + "where FSENTRY_PATH = ? and FSENTRY_NAME = ? "
                    + "and FSENTRY_LENGTH is not null");

            selectExistStmt = con.prepareStatement("select 1 from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME = nvl(?, ' ')");

            selectFileExistStmt = con.prepareStatement("select 1 from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME = ? and FSENTRY_LENGTH is not null");

            selectFolderExistStmt = con.prepareStatement("select 1 from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME = nvl(?, ' ') and FSENTRY_LENGTH is null");

            selectFileNamesStmt = con.prepareStatement("select FSENTRY_NAME from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_LENGTH is not null");

            selectFolderNamesStmt = con.prepareStatement("select FSENTRY_NAME from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME != ' ' "
                    + "and FSENTRY_LENGTH is null");

            selectFileAndFolderNamesStmt = con.prepareStatement("select FSENTRY_NAME from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME != ' '");

            selectChildCountStmt = con.prepareStatement("select count(FSENTRY_NAME) from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ?  "
                    + "and FSENTRY_NAME != ' '");

            selectDataStmt = con.prepareStatement("select nvl(FSENTRY_DATA, empty_blob()) from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME = ? and FSENTRY_LENGTH is not null");

            selectLastModifiedStmt = con.prepareStatement("select FSENTRY_LASTMOD from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME = nvl(?, ' ')");

            selectLengthStmt = con.prepareStatement("select nvl(FSENTRY_LENGTH, 0) from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME = ? and FSENTRY_LENGTH is not null");

            deleteFileStmt = con.prepareStatement("delete from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME = ? and FSENTRY_LENGTH is not null");

            deleteFolderStmt = con.prepareStatement("delete from "
                    + schemaObjectPrefix + "FSENTRY where "
                    + "(FSENTRY_PATH = ? and FSENTRY_NAME = nvl(?, ' ') and FSENTRY_LENGTH is null) "
                    + "or (FSENTRY_PATH = ?) "
                    + "or (FSENTRY_PATH like ?) ");

            copyFileStmt = con.prepareStatement("insert into "
                    + schemaObjectPrefix + "FSENTRY "
                    + "(FSENTRY_PATH, FSENTRY_NAME, FSENTRY_DATA, "
                    + "FSENTRY_LASTMOD, FSENTRY_LENGTH) "
                    + "select ?, ?, FSENTRY_DATA, "
                    + "FSENTRY_LASTMOD, FSENTRY_LENGTH from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_NAME = ? and FSENTRY_LENGTH is not null");

            copyFilesStmt = con.prepareStatement("insert into "
                    + schemaObjectPrefix + "FSENTRY "
                    + "(FSENTRY_PATH, FSENTRY_NAME, FSENTRY_DATA, "
                    + "FSENTRY_LASTMOD, FSENTRY_LENGTH) "
                    + "select ?, FSENTRY_NAME, FSENTRY_DATA, "
                    + "FSENTRY_LASTMOD, FSENTRY_LENGTH from "
                    + schemaObjectPrefix + "FSENTRY where FSENTRY_PATH = ? "
                    + "and FSENTRY_LENGTH is not null");

            // finally verify that there's a file system root entry
            verifyRootExists();

            initialized = true;
        } catch (Exception e) {
            String msg = "failed to initialize file system";
            log.error(msg, e);
            throw new FileSystemException(msg, e);
        }
    }
}
