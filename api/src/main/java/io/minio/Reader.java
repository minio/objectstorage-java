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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.Nonnull;

/** Reader reads part data from file or input stream sequentially and returns PartSource. */
class Reader {
  private byte[] buf16k = new byte[16384]; // 16KiB buffer for optimization.

  private InputStream stream;
  private Long objectSize;
  private long partSize;

  private long bytesRead = 0;
  private Byte oneByte = null;
  boolean endOfStream = false;

  public Reader(@Nonnull InputStream stream, Long objectSize, long partSize) {
    this.stream = Utils.validateNotNull(stream, "stream");
    if (partSize < 0) throw new IllegalArgumentException("part size must be provided");
    this.objectSize = objectSize;
    this.partSize = partSize;
  }

  private long copyStream(OutputStream os, long length) throws IOException {
    long bytesCopied = 0;
    int bytesRead = 0;
    while (bytesCopied < length
        && (bytesRead = stream.read(buf16k, 0, (int) Math.min(buf16k.length, length - bytesCopied)))
            != -1) {
      os.write(buf16k, 0, bytesRead);
      bytesCopied += bytesRead;
    }
    return bytesCopied;
  }

  private void readUnknownSizedStream(OutputStream os) throws IOException {
    long partSize = this.partSize;

    if (oneByte != null) {
      os.write(oneByte);
      oneByte = null;
      partSize--;
    }

    if (copyStream(os, partSize) == partSize) {
      int b = stream.read();
      if (b != -1) {
        oneByte = (byte) b;
      } else {
        endOfStream = true;
      }
    } else {
      endOfStream = true;
    }
  }

  private void readSizedStream(OutputStream os) throws IOException {
    long partSize = (long) Math.min(this.partSize, objectSize - bytesRead);
    long bytesCopied = copyStream(os, partSize);
    bytesRead += bytesCopied;
    if (bytesCopied != partSize) {
      endOfStream = true;
      throw new IOException(
          "Unexpected end of stream; expected=" + partSize + ", got=" + bytesCopied);
    }
    endOfStream = bytesRead == objectSize;
  }

  public void read(OutputStream os) throws IOException {
    if (objectSize == null) {
      readUnknownSizedStream(os);
    } else {
      readSizedStream(os);
    }
  }

  public boolean endOfStream() {
    return endOfStream;
  }

  // public void check(OutputStream os, InputStream is, Long objectSize, long partSize) {
  //   Integer partCount = null;
  //   if (objectSize != null) partCount = (int) Math.max(1, (objectSize + partSize - 1) /
  // partSize);
  //   for (int partNumber = 1; partCount == null || partNumber <= partCount; partNumber++) {
  //     read(os);
  //     if (partCount == null && endOfStream()) partCount = partNumber;
  //
  //     if (partCount != null && partNumber == partCount) {
  //       doSinglePutObbject();
  //     } else {
  //       uploadPart();
  //     }
  //   }
  // }
}
