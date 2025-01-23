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

/** Argument class of {@link MinioAsyncClient#listParts} and {@link MinioClient#listParts}. */
public class ListPartsArgs extends BucketArgs {
  private String uploadId;
  private Integer maxParts;
  private Integer partNumberMarker;

  public String uploadId() {
    return uploadId;
  }

  public int maxParts() {
    return maxParts;
  }

  public Integer partNumberMarker() {
    return partNumberMarker;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link ListPartsArgs}. */
  public static final class Builder extends BucketArgs.Builder<Builder, ListPartsArgs> {
    public Builder uploadId(String uploadId) {
      Utils.validateNotEmptyString(uploadId, "upload ID");
      operations.add(args -> args.uploadId = uploadId);
      return this;
    }

    public Builder maxParts(Integer maxParts) {
      if (maxParts != null && maxParts < 1) {
        throw new IllegalArgumentException("valid max parts must be provided");
      }

      operations.add(args -> args.maxParts = maxParts);
      return this;
    }

    public Builder partNumberMarker(Integer partNumberMarker) {
      if (partNumberMarker != null && (partNumberMarker < 1 || partNumberMarker > 10000)) {
        throw new IllegalArgumentException("valid part number marker must be provided");
      }
      operations.add(args -> args.partNumberMarker = partNumberMarker);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ListPartsArgs)) return false;
    if (!super.equals(o)) return false;
    ListPartsArgs that = (ListPartsArgs) o;
    return Objects.equals(uploadId, that.uploadId)
        && Objects.equals(maxParts, that.maxParts)
        && Objects.equals(partNumberMarker, that.partNumberMarker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), uploadId, maxParts, partNumberMarker);
  }
}
