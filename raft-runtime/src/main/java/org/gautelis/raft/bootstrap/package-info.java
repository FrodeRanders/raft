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
/**
 * Runtime bootstrap APIs for applications that embed the Raft library.
 *
 * <p>Application modules use this package to declare CLI commands, create runtime
 * adapters, and plug in admission, authentication, and authorization policy around
 * client writes. Raft still owns consensus, membership, durable log handling, and
 * read safety; the application owns how domain commands and queries are encoded and
 * interpreted.</p>
 */
package org.gautelis.raft.bootstrap;
