/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hibench.sparkbench.sql.datagen;
import java.io.*;
import java.nio.charset.StandardCharsets;

/** Weighted vocabularies frozen from the original generator on the reference JDK 11. */
public final class SqlVocabulary {
    public static final String[] searchKeys = load("search_keys");
    public static final String[] userAgents = load("user_agents");
    public static final String[] countryCodes = load("country_codes");
    private static String[] load(String name) {
        InputStream stream = SqlVocabulary.class.getResourceAsStream("/org/hibench/sql/" + name);
        if (stream == null) throw new IllegalStateException("Missing SQL vocabulary: " + name);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines().map(String::trim).toArray(String[]::new);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
}
