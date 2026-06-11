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
package org.gautelis.raft;

/**
 * Callback invoked when the committed cluster configuration changes.
 *
 * <p>This fires after the configuration has been updated in the Raft node,
 * so callers can safely read the current membership via
 * {@link RaftNode#telemetrySnapshot()} or other configuration accessors.</p>
 *
 * <p>Unlike {@code decommissionListener}, this fires for any membership
 * change: nodes joining, leaving, promotions, demotions, and joint-consensus
 * transitions.</p>
 */
@FunctionalInterface
public interface MembershipChangeListener {
    /**
     * Called when the committed cluster configuration has changed.
     *
     * @param configuration the new active cluster configuration
     */
    void onMembershipChanged(ClusterConfiguration configuration);
}
