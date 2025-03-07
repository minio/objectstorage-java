/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2015 MinIO, Inc.
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

import io.minio.Time;
import io.minio.Utils;
import java.time.ZonedDateTime;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/**
 * Helper class to denote Part information of a multipart upload and used in {@link
 * CompleteMultipartUpload} and {@link ListPartsResult}.
 */
@Root(name = "Part", strict = false)
public class Part extends Checksum {
  @Element(name = "PartNumber", required = false)
  private int partNumber;

  @Element(name = "ETag", required = false)
  private String etag;

  @Element(name = "LastModified", required = false)
  private Time.S3Time lastModified;

  @Element(name = "Size", required = false)
  private Long size;

  public Part() {}

  public Part(int partNumber, String etag) {
    this.partNumber = partNumber;
    this.etag = etag;
  }

  public Part(
      int partNumber,
      String etag,
      String checksumCRC32,
      String checksumCRC32C,
      String checksumCRC64NVME,
      String checksumSHA1,
      String checksumSHA256) {
    super(checksumCRC32, checksumCRC32C, checksumCRC64NVME, checksumSHA1, checksumSHA256, null);
    this.partNumber = partNumber;
    this.etag = etag;
  }

  public int partNumber() {
    return partNumber;
  }

  public String etag() {
    return etag.replaceAll("\"", "");
  }

  public ZonedDateTime lastModified() {
    return lastModified == null ? null : lastModified.toZonedDateTime();
  }

  public long partSize() {
    return size;
  }

  @Override
  public String toString() {
    return String.format(
        "Part{partNumber=%s, etag=%s, lastModified=%s, size=%s, %s}",
        Utils.stringify(partNumber),
        Utils.stringify(etag),
        Utils.stringify(lastModified),
        Utils.stringify(size),
        super.stringify());
  }
}
