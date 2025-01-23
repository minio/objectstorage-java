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

package io.minio;

import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#listObjectsV1} and {@link MinioClient#listObjectsV1}.
 */
public class ListObjectsV1Args extends BucketArgs {
  private String delimiter;
  private String encodingType;
  private Integer maxKeys;
  private String prefix;
  private String marker;

  public String delimiter() {
    return delimiter;
  }

  public String encodingType() {
    return encodingType;
  }

  public int maxKeys() {
    return maxKeys;
  }

  public String prefix() {
    return prefix;
  }

  public String marker() {
    return marker;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link ListObjectsV1Args}. */
  public static final class Builder extends BucketArgs.Builder<Builder, ListObjectsV1Args> {
    public Builder delimiter(String delimiter) {
      operations.add(args -> args.delimiter = delimiter);
      return this;
    }

    public Builder encodingType(String encodingType) {
      operations.add(args -> args.encodingType = encodingType);
      return this;
    }

    public Builder maxKeys(Integer maxKeys) {
      if (maxKeys != null && maxKeys < 1) {
        throw new IllegalArgumentException("valid max keys must be provided");
      }

      operations.add(args -> args.maxKeys = maxKeys);
      return this;
    }

    public Builder prefix(String prefix) {
      operations.add(args -> args.prefix = prefix);
      return this;
    }

    public Builder marker(String marker) {
      operations.add(args -> args.marker = marker);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ListObjectsV1Args)) return false;
    if (!super.equals(o)) return false;
    ListObjectsV1Args that = (ListObjectsV1Args) o;
    return Objects.equals(delimiter, that.delimiter)
        && Objects.equals(encodingType, that.encodingType)
        && Objects.equals(maxKeys, that.maxKeys)
        && Objects.equals(prefix, that.prefix)
        && Objects.equals(marker, that.marker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), delimiter, encodingType, maxKeys, prefix, marker);
  }
}
