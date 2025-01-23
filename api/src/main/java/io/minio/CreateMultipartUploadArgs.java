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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#createMultipartUpload} and {@link
 * MinioClient#createMultipartUpload}.
 */
public class CreateMultipartUploadArgs extends ObjectArgs {
  private Multimap<String, String> headers = Multimaps.unmodifiableMultimap(HashMultimap.create());

  public Multimap<String, String> headers() {
    return headers;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link CreateMultipartUploadArgs}. */
  public static final class Builder extends ObjectArgs.Builder<Builder, CreateMultipartUploadArgs> {
    @Override
    protected void validate(CreateMultipartUploadArgs args) {
      super.validate(args);
    }

    public Builder headers(Multimap<String, String> headers) {
      operations.add(args -> args.headers = headers);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CreateMultipartUploadArgs)) return false;
    if (!super.equals(o)) return false;
    CreateMultipartUploadArgs that = (CreateMultipartUploadArgs) o;
    return Objects.equals(headers, that.headers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), headers);
  }
}
