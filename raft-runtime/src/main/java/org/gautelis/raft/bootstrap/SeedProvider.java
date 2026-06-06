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

import java.util.List;

/**
 * Supplies the initial peer endpoints used to contact or form a Raft cluster.
 *
 * <p>Seed providers are startup helpers only. Their output is not treated as
 * continuing authority after the node has learned committed Raft membership.</p>
 */
public interface SeedProvider {
    List<SeedEndpoint> seeds();
}
