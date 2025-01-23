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

/** Argument class of {@link MinioAsyncClient#listBuckets} and {@link MinioClient#listBuckets}. */
public class ListBucketsArgs extends BaseArgs {
  private String bucketRegion;
  private int maxBuckets = 10000;
  private String prefix;
  private String continuationToken;

  public String bucketRegion() {
    return bucketRegion;
  }

  public int maxBuckets() {
    return maxBuckets;
  }

  public String prefix() {
    return prefix;
  }

  public String continuationToken() {
    return continuationToken;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link ListBucketsArgs}. */
  public static final class Builder extends BaseArgs.Builder<Builder, ListBucketsArgs> {
    @Override
    protected void validate(ListBucketsArgs args) {}

    public Builder bucketRegion(String region) {
      Utils.validateNullOrNotEmptyString(region, "bucket region");
      operations.add(args -> args.bucketRegion = region);
      return this;
    }

    public Builder maxBuckets(int maxBuckets) {
      if (maxBuckets < 1 || maxBuckets > 10000) {
        throw new IllegalArgumentException("max buckets must be between 1 and 10000");
      }

      operations.add(args -> args.maxBuckets = maxBuckets);
      return this;
    }

    public Builder prefix(String prefix) {
      Utils.validateNullOrNotEmptyString(prefix, "prefix");
      operations.add(args -> args.prefix = prefix);
      return this;
    }

    public Builder continuationToken(String continuationToken) {
      Utils.validateNullOrNotEmptyString(continuationToken, "continuation token");
      operations.add(args -> args.continuationToken = continuationToken);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ListBucketsArgs)) return false;
    if (!super.equals(o)) return false;
    ListBucketsArgs that = (ListBucketsArgs) o;
    return Objects.equals(bucketRegion, that.bucketRegion)
        && maxBuckets == that.maxBuckets
        && Objects.equals(prefix, that.prefix)
        && Objects.equals(continuationToken, that.continuationToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), bucketRegion, maxBuckets, prefix, continuationToken);
  }
}
