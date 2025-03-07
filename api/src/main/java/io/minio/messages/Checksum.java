/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2025 MinIO, Inc.
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.minio.Utils;
import java.util.Locale;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/** Helper class for. */
@Root(name = "Checksum", strict = false)
public class Checksum {
  @Element(name = "ChecksumCRC32", required = false)
  private String checksumCRC32;

  @Element(name = "ChecksumCRC32C", required = false)
  private String checksumCRC32C;

  @Element(name = "ChecksumCRC64NVME", required = false)
  private String checksumCRC64NVME;

  @Element(name = "ChecksumSHA1", required = false)
  private String checksumSHA1;

  @Element(name = "ChecksumSHA256", required = false)
  private String checksumSHA256;

  @Element(name = "ChecksumType", required = false)
  private String checksumType;

  public Checksum() {}

  public Checksum(
      String checksumCRC32,
      String checksumCRC32C,
      String checksumCRC64NVME,
      String checksumSHA1,
      String checksumSHA256,
      String checksumType) {
    this.checksumCRC32 = checksumCRC32;
    this.checksumCRC32C = checksumCRC32C;
    this.checksumCRC64NVME = checksumCRC64NVME;
    this.checksumSHA1 = checksumSHA1;
    this.checksumSHA256 = checksumSHA256;
    this.checksumType = checksumType;
  }

  public String checksumCRC32() {
    return checksumCRC32;
  }

  public String checksumCRC32C() {
    return checksumCRC32C;
  }

  public String checksumCRC64NVME() {
    return checksumCRC64NVME;
  }

  public String checksumSHA1() {
    return checksumSHA1;
  }

  public String checksumSHA256() {
    return checksumSHA256;
  }

  public String checksumType() {
    return checksumType;
  }

  private void addHeader(Multimap<String, String> map, String algorithm, String value) {
    if (value != null || !value.isEmpty()) {
      map.put("x-amz-checksum-algorithm", algorithm);
      map.put("x-amz-checksum-algorithm-" + algorithm.toLowerCase(Locale.US), value);
    }
  }

  public Multimap<String, String> headers() {
    Multimap<String, String> map = HashMultimap.create();
    addHeader(map, "CRC32", checksumCRC32);
    addHeader(map, "CRC32C", checksumCRC32C);
    addHeader(map, "CRC64NVME", checksumCRC64NVME);
    addHeader(map, "SHA1", checksumSHA1);
    addHeader(map, "SHA256", checksumSHA256);
    return map;
  }

  protected String stringify() {
    return String.format(
        "checksumCRC32=%s, checksumCRC32C=%s, checksumCRC64NVME=%s, checksumSHA1=%s,"
            + " checksumSHA256=%s, checksumType=%s",
        Utils.stringify(checksumCRC32),
        Utils.stringify(checksumCRC32C),
        Utils.stringify(checksumCRC64NVME),
        Utils.stringify(checksumSHA1),
        Utils.stringify(checksumSHA256),
        Utils.stringify(checksumType));
  }

  @Override
  public String toString() {
    return String.format("Checksum{%s}", stringify());
  }
}
