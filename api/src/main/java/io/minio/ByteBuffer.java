/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2025 MinIO, Inc.
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

package io.minio;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ByteBuffer extends OutputStream {
  private static class Buffer extends ByteArrayOutputStream {
    public InputStream inputStream() {
      return (count > 0) ? new ByteArrayInputStream(buf, 0, count) : null;
    }
  }

  private static final long MAX_SIZE = 5L * 1024 * 1024 * 1024;
  private static final int CHUNK_SIZE = Integer.MAX_VALUE;
  private final long totalSize;
  private final List<Buffer> buffers = new ArrayList<>();
  private int index = 0;
  private long writtenBytes = 0;
  private boolean isClosed = false;

  public ByteBuffer(long totalSize) {
    if (totalSize > MAX_SIZE) {
      throw new IllegalArgumentException("Total size cannot exceed 5GiB");
    }
    this.totalSize = totalSize;
  }

  private void updateIndex() {
    if (buffers.isEmpty()) {
      index = 0;
      buffers.add(new Buffer());
    } else if (writtenBytes >= (long) (index + 1) * CHUNK_SIZE) {
      index++;
      if (index > buffers.size() - 1) buffers.add(new Buffer());
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (isClosed) throw new IOException("Stream is closed");
    if (writtenBytes >= totalSize) throw new IOException("Exceeded total size limit");
    updateIndex();
    buffers.get(index).write(b);
    writtenBytes++;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (isClosed) throw new IOException("Stream is closed");
    if (len > (totalSize - writtenBytes)) throw new IOException("Exceeded total size limit");
    int remaining = len;
    while (remaining > 0) {
      updateIndex();
      Buffer currentBuffer = buffers.get(index);
      int bytesToWrite = Math.min(remaining, CHUNK_SIZE - currentBuffer.size());
      currentBuffer.write(b, off + (len - remaining), bytesToWrite);
      writtenBytes += bytesToWrite;
      remaining -= bytesToWrite;
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  public long length() {
    return writtenBytes;
  }

  public void reset() throws IOException {
    if (isClosed) throw new IOException("Cannot reset a closed stream");
    writtenBytes = 0;
    index = 0;
    for (Buffer buffer : buffers) buffer.reset();
  }

  public void close() throws IOException {
    if (!isClosed) {
      isClosed = true;
      buffers.clear();
    }
  }

  public InputStream inputStream() throws IOException {
    List<InputStream> streams = new ArrayList<>();
    for (Buffer buffer : buffers) {
      InputStream stream = buffer.inputStream();
      if (stream != null) streams.add(stream);
    }
    switch (streams.size()) {
      case 0:
        return new ByteArrayInputStream(Utils.EMPTY_BODY);
      case 1:
        return streams.get(0);
      default:
        return new SequenceInputStream(Collections.enumeration(streams));
    }
  }
}
