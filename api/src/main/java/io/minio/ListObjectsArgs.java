/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2020 MinIO, Inc.
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

/** Argument class of {@link MinioAsyncClient#listObjects} and {@link MinioClient#listObjects}. */
public class ListObjectsArgs extends BucketArgs {
  private String delimiter = "";
  private boolean useUrlEncodingType = true;
  private String keyMarker; // 'marker' for ListObjectsV1 and 'startAfter' for ListObjectsV2.
  private int maxKeys = 1000;
  private String prefix = "";
  private String continuationToken; // only for ListObjectsV2.
  private boolean fetchOwner; // only for ListObjectsV2.
  private String versionIdMarker; // only for GetObjectVersions.
  private boolean includeUserMetadata; // MinIO extension applicable to ListObjectsV2.
  private boolean recursive;
  private boolean useApiVersion1;
  private boolean includeVersions;

  public String delimiter() {
    if (recursive) {
      return "";
    }

    return (delimiter.isEmpty() ? "/" : delimiter);
  }

  public boolean useUrlEncodingType() {
    return useUrlEncodingType;
  }

  public String keyMarker() {
    return keyMarker;
  }

  public String marker() {
    return keyMarker;
  }

  public String startAfter() {
    return keyMarker;
  }

  public int maxKeys() {
    return maxKeys;
  }

  public String prefix() {
    return prefix;
  }

  public String continuationToken() {
    return continuationToken;
  }

  public boolean fetchOwner() {
    return fetchOwner;
  }

  public String versionIdMarker() {
    return versionIdMarker;
  }

  public boolean includeUserMetadata() {
    return includeUserMetadata;
  }

  public boolean recursive() {
    return recursive;
  }

  public boolean useApiVersion1() {
    return useApiVersion1;
  }

  public boolean includeVersions() {
    return includeVersions;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link ListObjectsArgs}. */
  public static final class Builder extends BucketArgs.Builder<Builder, ListObjectsArgs> {
    @Override
    protected void validate(ListObjectsArgs args) {
      super.validate(args);

      if (args.useApiVersion1() || args.includeVersions()) {
        if (args.continuationToken() != null || args.fetchOwner() || args.includeUserMetadata()) {
          throw new IllegalArgumentException(
              "continuation token/fetch owner/include metadata are supported only"
                  + " for list objects version 2");
        }
      }

      if (args.versionIdMarker != null && args.useApiVersion1()) {
        throw new IllegalArgumentException(
            "version ID marker is not supported for list objects version 1");
      }
    }

    public Builder delimiter(String delimiter) {
      operations.add(args -> args.delimiter = (delimiter == null ? "" : delimiter));
      return this;
    }

    public Builder useUrlEncodingType(boolean flag) {
      operations.add(args -> args.useUrlEncodingType = flag);
      return this;
    }

    public Builder keyMarker(String keyMarker) {
      Utils.validateNullOrNotEmptyString(keyMarker, "key marker");
      operations.add(args -> args.keyMarker = keyMarker);
      return this;
    }

    public Builder marker(String marker) {
      operations.add(args -> args.keyMarker = marker);
      return this;
    }

    public Builder startAfter(String startAfter) {
      operations.add(args -> args.keyMarker = startAfter);
      return this;
    }

    public Builder maxKeys(int maxKeys) {
      if (maxKeys < 1 || maxKeys > 1000) {
        throw new IllegalArgumentException("max keys must be between 1 and 1000");
      }

      operations.add(args -> args.maxKeys = maxKeys);
      return this;
    }

    public Builder prefix(String prefix) {
      operations.add(args -> args.prefix = (prefix == null ? "" : prefix));
      return this;
    }

    public Builder continuationToken(String continuationToken) {
      Utils.validateNullOrNotEmptyString(continuationToken, "continuation token");
      operations.add(args -> args.continuationToken = continuationToken);
      return this;
    }

    public Builder fetchOwner(boolean fetchOwner) {
      operations.add(args -> args.fetchOwner = fetchOwner);
      return this;
    }

    public Builder versionIdMarker(String versionIdMarker) {
      Utils.validateNullOrNotEmptyString(versionIdMarker, "version ID marker");
      operations.add(args -> args.versionIdMarker = versionIdMarker);
      return this;
    }

    public Builder includeUserMetadata(boolean includeUserMetadata) {
      operations.add(args -> args.includeUserMetadata = includeUserMetadata);
      return this;
    }

    public Builder recursive(boolean recursive) {
      operations.add(args -> args.recursive = recursive);
      return this;
    }

    public Builder useApiVersion1(boolean useApiVersion1) {
      operations.add(args -> args.useApiVersion1 = useApiVersion1);
      return this;
    }

    public Builder includeVersions(boolean includeVersions) {
      operations.add(args -> args.includeVersions = includeVersions);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ListObjectsArgs)) return false;
    if (!super.equals(o)) return false;
    ListObjectsArgs that = (ListObjectsArgs) o;
    return useUrlEncodingType == that.useUrlEncodingType
        && maxKeys == that.maxKeys
        && fetchOwner == that.fetchOwner
        && includeUserMetadata == that.includeUserMetadata
        && recursive == that.recursive
        && useApiVersion1 == that.useApiVersion1
        && includeVersions == that.includeVersions
        && Objects.equals(delimiter, that.delimiter)
        && Objects.equals(keyMarker, that.keyMarker)
        && Objects.equals(prefix, that.prefix)
        && Objects.equals(continuationToken, that.continuationToken)
        && Objects.equals(versionIdMarker, that.versionIdMarker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        delimiter,
        useUrlEncodingType,
        keyMarker,
        maxKeys,
        prefix,
        continuationToken,
        fetchOwner,
        versionIdMarker,
        includeUserMetadata,
        recursive,
        useApiVersion1,
        includeVersions);
  }
}
