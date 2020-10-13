/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.roles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.atomix.raft.impl.RaftContext;
import io.atomix.raft.metrics.RaftReplicationMetrics;
import io.atomix.raft.protocol.AppendRequest;
import io.atomix.raft.protocol.AppendResponse;
import io.atomix.raft.storage.RaftStorage;
import io.atomix.raft.storage.log.RaftLog;
import io.atomix.raft.storage.log.RaftLogWriter;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.serializer.NamespaceImpl;
import io.atomix.utils.serializer.NamespaceImpl.Builder;
import io.atomix.utils.serializer.Namespaces;
import io.zeebe.snapshots.raft.PersistedSnapshot;
import io.zeebe.snapshots.raft.ReceivableSnapshotStore;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class PassiveRoleTest {

  private static final NamespaceImpl NAMESPACE =
      new Builder().register(Namespaces.BASIC).register(ZeebeEntry.class).build();
  @Rule public Timeout timeout = new Timeout(30, TimeUnit.SECONDS);
  private RaftContext ctx;
  private ZeebeEntry entry = new ZeebeEntry(1, 1, 0, 1, ByteBuffer.allocate(0));

  @Before
  public void setup() {
    final RaftStorage storage = mock(RaftStorage.class);
    when(storage.getMaxEntrySize()).thenReturn(1024);
    when(storage.namespace()).thenReturn(NAMESPACE);

    final RaftLogWriter writer = mock(RaftLogWriter.class);
    when(writer.getLastIndex()).thenReturn(1L);
    when(writer.append((ZeebeEntry) any())).thenReturn(new Indexed<>(1, entry, 1, 1));

    final PersistedSnapshot snapshot = mock(PersistedSnapshot.class);
    when(snapshot.getIndex()).thenReturn(1L);
    when(snapshot.getTerm()).thenReturn(1L);

    final ReceivableSnapshotStore store = mock(ReceivableSnapshotStore.class);
    when(store.getLatestSnapshot()).thenReturn(Optional.of(snapshot));

    ctx = mock(RaftContext.class);
    when(ctx.getStorage()).thenReturn(storage);
    when(ctx.getLogWriter()).thenReturn(writer);
    when(ctx.getPersistedSnapshotStore()).thenReturn(store);
    when(ctx.getTerm()).thenReturn(1L);
    when(ctx.getReplicationMetrics()).thenReturn(mock(RaftReplicationMetrics.class));
    when(ctx.getLog()).thenReturn(mock(RaftLog.class));
  }

  @Test
  public void shouldRejectRequestIfDifferentNumberEntriesAndChecksums() {
    // given
    final PassiveRole role = new PassiveRole(ctx);
    final List<RaftLogEntry> entries = generateEntries(1);
    final AppendRequest request = new AppendRequest(2, "", 1, 1, entries, List.of(1L, 2L), 1);

    // when
    final AppendResponse response = role.handleAppend(request).join();

    // then
    assertThat(response.succeeded()).isFalse();
  }

  @Test
  public void shouldRejectIfChecksumIsIncorrect() {
    // given
    final PassiveRole role = new PassiveRole(ctx);
    final List<RaftLogEntry> entries = generateEntries(3);
    final List<Long> checksums = getChecksums(entries);
    checksums.set(1, 0L);
    final AppendRequest request = new AppendRequest(2, "", 1, 1, entries, checksums, 1);

    // when
    final AppendResponse response = role.handleAppend(request).join();

    // then
    assertThat(response.succeeded()).isFalse();
    assertThat(response.lastLogIndex()).isEqualTo(((ZeebeEntry) entries.get(1)).highestPosition());
  }

  @Test
  public void shouldSucceed() {
    // given
    final PassiveRole role = new PassiveRole(ctx);
    final List<RaftLogEntry> entries = generateEntries(2);
    final List<Long> checksums = getChecksums(entries);
    final AppendRequest request = new AppendRequest(2, "", 1, 1, entries, checksums, 1);

    // when
    final AppendResponse response = role.handleAppend(request).join();

    // then
    assertThat(response.succeeded()).isTrue();
  }

  private List<RaftLogEntry> generateEntries(final int numEntries) {
    final List<RaftLogEntry> entries = new ArrayList<>();
    for (int i = 0; i < numEntries; i++) {
      entries.add(new ZeebeEntry(1, 1, i + 1, i + 1, ByteBuffer.allocate(0)));
    }

    return entries;
  }

  private List<Long> getChecksums(final List<RaftLogEntry> entries) {
    final List<Long> checksums = new ArrayList<>();

    for (final RaftLogEntry entry : entries) {
      final byte[] serialized = NAMESPACE.serialize(entry);
      final Checksum crc32 = new CRC32();
      crc32.update(serialized, 0, serialized.length);

      checksums.add(crc32.getValue());
    }

    return checksums;
  }
}
