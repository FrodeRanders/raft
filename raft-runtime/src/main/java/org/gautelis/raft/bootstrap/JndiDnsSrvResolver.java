/*
 * Copyright (C) 2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.raft.bootstrap;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.InitialDirContext;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * DNS SRV resolver based on the JDK JNDI DNS provider.
 */
public final class JndiDnsSrvResolver implements DnsSrvResolver {
    @Override
    public List<SrvRecord> resolve(String serviceName) {
        try {
            Hashtable<String, String> environment = new Hashtable<>();
            environment.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
            var context = new InitialDirContext(environment);
            Attribute srv = context.getAttributes(serviceName, new String[]{"SRV"}).get("SRV");
            if (srv == null) {
                return List.of();
            }
            List<SrvRecord> records = new ArrayList<>();
            NamingEnumeration<?> values = srv.getAll();
            while (values.hasMore()) {
                records.add(parseSrvRecord(values.next().toString()));
            }
            return records;
        } catch (Exception e) {
            throw new IllegalStateException("Failed resolving DNS SRV records for " + serviceName, e);
        }
    }

    static SrvRecord parseSrvRecord(String raw) {
        String[] parts = raw == null ? new String[0] : raw.trim().split("\\s+");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Invalid DNS SRV record: " + raw);
        }
        return new SrvRecord(
                Integer.parseInt(parts[0]),
                Integer.parseInt(parts[1]),
                Integer.parseInt(parts[2]),
                stripTrailingDot(parts[3])
        );
    }

    private static String stripTrailingDot(String target) {
        String value = target.trim();
        while (value.endsWith(".")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }
}
