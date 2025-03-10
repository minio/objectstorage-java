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

import com.google.common.collect.Multimap;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#uploadPartCopy} and {@link MinioClient#uploadPartCopy}.
 */
public class UploadPartCopyArgs extends ObjectArgs {
  private String uploadId;
  private int partNumber;
  private Multimap<String, String> headers;

  public String uploadId() {
    return uploadId;
  }

  public int partNumber() {
    return partNumber;
  }

  public Multimap<String, String> headers() {
    return headers;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link UploadPartCopyArgs}. */
  public static final class Builder extends ObjectArgs.Builder<Builder, UploadPartCopyArgs> {
    @Override
    protected void validate(UploadPartCopyArgs args) {
      super.validate(args);
      Utils.validateNotEmptyString(args.uploadId, "upload ID");
      if (args.partNumber <= 0) {
        throw new IllegalArgumentException("valid part number must be provided");
      }
      Utils.validateNotNull(args.headers, "headers");
      if (args.headers.size() < 2) {
        throw new IllegalArgumentException("valid headers must be provided");
      }
    }

    public Builder uploadId(String uploadId) {
      Utils.validateNotEmptyString(uploadId, "upload ID");
      operations.add(args -> args.uploadId = uploadId);
      return this;
    }

    public Builder partNumber(int partNumber) {
      if (partNumber <= 0) throw new IllegalArgumentException("valid part number must be provided");
      operations.add(args -> args.partNumber = partNumber);
      return this;
    }

    public Builder headers(Multimap<String, String> headers) {
      Utils.validateNotNull(headers, "headers");
      if (headers.size() < 2) throw new IllegalArgumentException("valid headers must be provided");
      operations.add(args -> args.headers = headers);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UploadPartCopyArgs)) return false;
    if (!super.equals(o)) return false;
    UploadPartCopyArgs that = (UploadPartCopyArgs) o;
    return Objects.equals(uploadId, that.uploadId)
        && Objects.equals(partNumber, that.partNumber)
        && Objects.equals(headers, that.headers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), uploadId, partNumber, headers);
  }
}
