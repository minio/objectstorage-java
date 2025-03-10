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

import io.minio.messages.DeleteRequest;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#deleteObjects} and {@link MinioClient#deleteObjects}.
 */
public class DeleteObjectsArgs extends BucketArgs {
  private boolean quiet;
  private boolean bypassGovernanceMode;
  private List<DeleteRequest.Object> objects = new LinkedList<>();

  public boolean quiet() {
    return quiet;
  }

  public boolean bypassGovernanceMode() {
    return bypassGovernanceMode;
  }

  public List<DeleteRequest.Object> objects() {
    return objects;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link DeleteObjectsArgs}. */
  public static final class Builder extends BucketArgs.Builder<Builder, DeleteObjectsArgs> {
    @Override
    protected void validate(DeleteObjectsArgs args) {
      super.validate(args);
      Utils.validateNotNull(args.objects, "objects");
      if (args.objects.size() > 1000) {
        throw new IllegalArgumentException("list of objects must not be more than 1000");
      }
    }

    public Builder quiet(boolean flag) {
      operations.add(args -> args.quiet = flag);
      return this;
    }

    public Builder bypassGovernanceMode(boolean flag) {
      operations.add(args -> args.bypassGovernanceMode = flag);
      return this;
    }

    public Builder objects(List<DeleteRequest.Object> objects) {
      Utils.validateNotNull(objects, "objects");
      if (objects.size() > 1000) {
        throw new IllegalArgumentException("list of objects must not be more than 1000");
      }
      operations.add(args -> args.objects = objects);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DeleteObjectsArgs)) return false;
    if (!super.equals(o)) return false;
    DeleteObjectsArgs that = (DeleteObjectsArgs) o;
    return quiet == that.quiet
        && bypassGovernanceMode == that.bypassGovernanceMode
        && Objects.equals(objects, that.objects);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), quiet, bypassGovernanceMode, objects);
  }
}
