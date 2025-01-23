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
 * Argument class of {@link MinioAsyncClient#listObjectsV2} and {@link MinioClient#listObjectsV2}.
 */
public class ListObjectsV2Args extends BucketArgs {
  private String delimiter;
  private String encodingType;
  private Integer maxKeys;
  private String prefix;
  private String startAfter;
  private String continuationToken;
  private boolean fetchOwner;
  private boolean includeUserMetadata;

  public ListObjectsV2Args(ListObjectsArgs args) {
    this.delimiter = args.delimiter();
    this.encodingType = args.useUrlEncodingType() ? "url" : null;
    this.maxKeys = args.maxKeys();
    this.prefix = args.prefix();
    this.startAfter = args.startAfter();
    this.continuationToken = args.continuationToken();
    this.fetchOwner = args.fetchOwner();
    this.includeUserMetadata = args.includeUserMetadata();
  }

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

  public String startAfter() {
    return startAfter;
  }

  public String continuationToken() {
    return continuationToken;
  }

  public boolean fetchOwner() {
    return fetchOwner;
  }

  public boolean includeUserMetadata() {
    return includeUserMetadata;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link ListObjectsV2Args}. */
  public static final class Builder extends BucketArgs.Builder<Builder, ListObjectsV2Args> {
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

    public Builder startAfter(String startAfter) {
      operations.add(args -> args.startAfter = startAfter);
      return this;
    }

    public Builder continuationToken(String continuationToken) {
      operations.add(args -> args.continuationToken = continuationToken);
      return this;
    }

    public Builder fetchOwner(boolean fetchOwner) {
      operations.add(args -> args.fetchOwner = fetchOwner);
      return this;
    }

    public Builder includeUserMetadata(boolean includeUserMetadata) {
      operations.add(args -> args.includeUserMetadata = includeUserMetadata);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ListObjectsV2Args)) return false;
    if (!super.equals(o)) return false;
    ListObjectsV2Args that = (ListObjectsV2Args) o;
    return Objects.equals(delimiter, that.delimiter)
        && Objects.equals(encodingType, that.encodingType)
        && Objects.equals(maxKeys, that.maxKeys)
        && Objects.equals(prefix, that.prefix)
        && Objects.equals(startAfter, that.startAfter)
        && Objects.equals(continuationToken, that.continuationToken)
        && fetchOwner == that.fetchOwner
        && includeUserMetadata == that.includeUserMetadata;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        delimiter,
        encodingType,
        maxKeys,
        prefix,
        startAfter,
        continuationToken,
        fetchOwner,
        includeUserMetadata);
  }
}
