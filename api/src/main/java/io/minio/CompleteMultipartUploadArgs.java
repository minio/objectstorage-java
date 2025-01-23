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

import io.minio.messages.Part;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#completeMultipartUpload} and {@link
 * MinioClient#completeMultipartUpload}.
 */
public class CompleteMultipartUploadArgs extends ObjectArgs {
  private String uploadId;
  private Part[] parts;

  public String uploadId() {
    return uploadId;
  }

  public Part[] parts() {
    return parts;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link CompleteMultipartUploadArgs}. */
  public static final class Builder
      extends ObjectArgs.Builder<Builder, CompleteMultipartUploadArgs> {
    @Override
    protected void validate(CompleteMultipartUploadArgs args) {
      super.validate(args);
      Utils.validateNotEmptyString(args.uploadId, "upload ID");
      Utils.validateNotNull(args.parts, "parts");
    }

    public Builder uploadId(String uploadId) {
      Utils.validateNotEmptyString(uploadId, "upload ID");
      operations.add(args -> args.uploadId = uploadId);
      return this;
    }

    public Builder parts(Part[] parts) {
      Utils.validateNotNull(parts, "parts");
      operations.add(args -> args.parts = parts);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CompleteMultipartUploadArgs)) return false;
    if (!super.equals(o)) return false;
    CompleteMultipartUploadArgs that = (CompleteMultipartUploadArgs) o;
    return Objects.equals(uploadId, that.uploadId) && Objects.equals(parts, that.parts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), uploadId, parts);
  }
}
