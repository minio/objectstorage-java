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

import io.minio.messages.VersioningConfiguration;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#setBucketVersioning} and {@link
 * MinioClient#setBucketVersioning}.
 */
public class SetBucketVersioningArgs extends BucketArgs {
  private VersioningConfiguration config;

  public VersioningConfiguration config() {
    return config;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link SetBucketVersioningArgs}. */
  public static final class Builder extends BucketArgs.Builder<Builder, SetBucketVersioningArgs> {
    private void validateConfig(VersioningConfiguration config) {
      Utils.validateNotNull(config, "versioning configuration");
    }

    protected void validate(SetBucketVersioningArgs args) {
      super.validate(args);
      validateConfig(args.config);
    }

    public Builder config(VersioningConfiguration config) {
      validateConfig(config);
      operations.add(args -> args.config = config);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SetBucketVersioningArgs)) return false;
    if (!super.equals(o)) return false;
    SetBucketVersioningArgs that = (SetBucketVersioningArgs) o;
    return Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), config);
  }
}
