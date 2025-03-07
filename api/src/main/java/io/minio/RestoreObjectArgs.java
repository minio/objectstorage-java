/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2022 MinIO, Inc.
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

import io.minio.messages.RestoreRequest;
import java.util.Objects;

/** Argument class of {@link MinioClient#restoreObject}. */
public class RestoreObjectArgs extends ObjectVersionArgs {
  private RestoreRequest request;

  public RestoreRequest request() {
    return request;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link RestoreObjectArgs}. */
  public static final class Builder extends ObjectVersionArgs.Builder<Builder, RestoreObjectArgs> {
    private void validateRequest(RestoreRequest request) {
      Utils.validateNotNull(request, "request");
    }

    public Builder request(RestoreRequest request) {
      validateRequest(request);
      operations.add(args -> args.request = request);
      return this;
    }

    @Override
    protected void validate(RestoreObjectArgs args) {
      super.validate(args);
      validateRequest(args.request());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RestoreObjectArgs)) return false;
    if (!super.equals(o)) return false;
    RestoreObjectArgs that = (RestoreObjectArgs) o;
    return Objects.equals(request, that.request);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), request);
  }
}
