/*
 * Copyright (C) 2025-2026 Frode Randers
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
package org.gautelis.raft.protocol;

/**
 * Summarizes transport latency statistics for one remote peer and one RPC type.
 *
 * @param peerId peer identifier
 * @param rpcType RPC type name
 * @param samples number of latency samples included
 * @param meanMillis mean latency in milliseconds
 * @param minMillis minimum observed latency in milliseconds
 * @param maxMillis maximum observed latency in milliseconds
 * @param cvPercent coefficient of variation expressed as a percentage
 */
public record TelemetryPeerStats(
        String peerId,
        String rpcType,
        long samples,
        double meanMillis,
        double minMillis,
        double maxMillis,
        double cvPercent
) {}
