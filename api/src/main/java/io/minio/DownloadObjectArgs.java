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

/**
 * Argument class of {@link MinioAsyncClient#downloadObject} and {@link MinioClient#downloadObject}.
 */
public class DownloadObjectArgs extends ObjectReadArgs {
  private String filename;
  private boolean overwrite;

  public String filename() {
    return filename;
  }

  public boolean overwrite() {
    return overwrite;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument class of {@link DownloadObjectArgs}. */
  public static final class Builder extends ObjectReadArgs.Builder<Builder, DownloadObjectArgs> {
    private void validateFilename(String filename) {
      Utils.validateNotEmptyString(filename, "filename");
    }

    public Builder filename(String filename) {
      validateFilename(filename);
      operations.add(args -> args.filename = filename);
      return this;
    }

    public Builder overwrite(boolean flag) {
      operations.add(args -> args.overwrite = flag);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DownloadObjectArgs)) return false;
    if (!super.equals(o)) return false;
    DownloadObjectArgs that = (DownloadObjectArgs) o;
    if (!Objects.equals(filename, that.filename)) return false;
    return overwrite == that.overwrite;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), filename, overwrite);
  }
}
