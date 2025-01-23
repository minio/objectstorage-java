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

import io.minio.messages.DeleteRequest;
import java.util.LinkedList;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#removeObjects} and {@link MinioClient#removeObjects}.
 */
public class RemoveObjectsArgs extends BucketArgs {
  private boolean bypassGovernanceMode;
  private Iterable<DeleteRequest.Object> objects = new LinkedList<>();

  public boolean bypassGovernanceMode() {
    return bypassGovernanceMode;
  }

  public Iterable<DeleteRequest.Object> objects() {
    return objects;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link RemoveObjectsArgs}. */
  public static final class Builder extends BucketArgs.Builder<Builder, RemoveObjectsArgs> {
    public Builder bypassGovernanceMode(boolean flag) {
      operations.add(args -> args.bypassGovernanceMode = flag);
      return this;
    }

    public Builder objects(Iterable<DeleteRequest.Object> objects) {
      Utils.validateNotNull(objects, "objects");
      operations.add(args -> args.objects = objects);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RemoveObjectsArgs)) return false;
    if (!super.equals(o)) return false;
    RemoveObjectsArgs that = (RemoveObjectsArgs) o;
    return bypassGovernanceMode == that.bypassGovernanceMode
        && Objects.equals(objects, that.objects);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), bypassGovernanceMode, objects);
  }
}
