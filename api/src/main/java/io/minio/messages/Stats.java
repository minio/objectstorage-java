/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2019 MinIO, Inc.
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

package io.minio.messages;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/** Helper class to denote Stats information of S3 select response message. */
@Root(name = "Stats", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class Stats {
  @Element(name = "BytesScanned", required = false)
  private Long bytesScanned;

  @Element(name = "BytesProcessed", required = false)
  private Long bytesProcessed;

  @Element(name = "BytesReturned", required = false)
  private Long bytesReturned;

  /** Constructs a new Stats object. */
  public Stats(
      @Element(name = "BytesScanned", required = false) Long bytesScanned,
      @Element(name = "BytesProcessed", required = false) Long bytesProcessed,
      @Element(name = "BytesReturned", required = false) Long bytesReturned) {
    this.bytesScanned = bytesScanned;
    this.bytesProcessed = bytesProcessed;
    this.bytesReturned = bytesReturned;
  }

  /** Returns bytes scanned. */
  public Long bytesScanned() {
    return bytesScanned;
  }

  /** Returns bytes processed. */
  public Long bytesProcessed() {
    return bytesProcessed;
  }

  /** Returns bytes returned. */
  public Long bytesReturned() {
    return bytesReturned;
  }

  protected String stringify() {
    return String.format(
        "bytesScanned=%s, bytesProcessed=%s, bytesReturned=%s",
        bytesScanned, bytesProcessed, bytesReturned);
  }

  @Override
  public String toString() {
    return String.format("Stats{%s}", stringify());
  }
}
