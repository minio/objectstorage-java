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
import java.util.concurrent.TimeUnit;

/**
 * Argument class of {@link MinioAsyncClient#getPresignedObjectUrl} and {@link
 * MinioClient#getPresignedObjectUrl}.
 */
public class GetPresignedObjectUrlArgs extends ObjectVersionArgs {
  // default expiration for a presigned URL is 7 days in seconds
  public static final int DEFAULT_EXPIRY_TIME = (int) TimeUnit.DAYS.toSeconds(7);

  private Http.Method method;
  private int expiry = DEFAULT_EXPIRY_TIME;

  public Http.Method method() {
    return method;
  }

  public int expiry() {
    return expiry;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link GetPresignedObjectUrlArgs}. */
  public static final class Builder
      extends ObjectVersionArgs.Builder<Builder, GetPresignedObjectUrlArgs> {
    private void validateMethod(Http.Method method) {
      Utils.validateNotNull(method, "method");
    }

    private void validateExpiry(int expiry) {
      if (expiry < 1 || expiry > DEFAULT_EXPIRY_TIME) {
        throw new IllegalArgumentException(
            "expiry must be minimum 1 second to maximum "
                + TimeUnit.SECONDS.toDays(DEFAULT_EXPIRY_TIME)
                + " days");
      }
    }

    /* method HTTP {@link Method} to generate presigned URL. */
    public Builder method(Http.Method method) {
      validateMethod(method);
      operations.add(args -> args.method = method);
      return this;
    }

    /*expires Expiry in seconds; defaults to 7 days. */
    public Builder expiry(int expiry) {
      validateExpiry(expiry);
      operations.add(args -> args.expiry = expiry);
      return this;
    }

    public Builder expiry(int duration, TimeUnit unit) {
      return expiry((int) unit.toSeconds(duration));
    }

    @Override
    protected void validate(GetPresignedObjectUrlArgs args) {
      super.validate(args);
      validateMethod(args.method);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GetPresignedObjectUrlArgs)) return false;
    if (!super.equals(o)) return false;
    GetPresignedObjectUrlArgs that = (GetPresignedObjectUrlArgs) o;
    return expiry == that.expiry && method == that.method;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), method, expiry);
  }
}
