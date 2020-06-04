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

import io.minio.http.Method;
import java.util.concurrent.TimeUnit;

/** Argument class of MinioClient.getPresignedObjectUrl(). */
public class GetPresignedObjectUrlArgs extends ObjectArgs {

  // default expiration for a presigned URL is 7 days in seconds
  public static final int DEFAULT_EXPIRY_TIME = 7 * 24 * 3600;
  private Method method;
  // set default expiry as 7 days if not specified.
  private int expiry = DEFAULT_EXPIRY_TIME;

  public Method method() {
    return method;
  }

  public int expiry() {
    return expiry;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link GetPresignedObjectUrlArgs}. */
  public static final class Builder extends ObjectArgs.Builder<Builder, GetPresignedObjectUrlArgs> {
    private void validateMethod(Method method) {
      validateNotNull(method, "method");
    }

    private void validateExpiry(int expiry) {
      if (expiry < 1 || expiry > DEFAULT_EXPIRY_TIME) {
        throw new IllegalArgumentException(
            "expires must be in range of 1 to " + DEFAULT_EXPIRY_TIME);
      }
    }

    /* method HTTP {@link Method} to generate presigned URL. */
    public Builder method(Method method) {
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
      int expiry = (int) TimeUnit.SECONDS.convert(duration, unit);
      validateExpiry(expiry);
      operations.add(args -> args.expiry = expiry);
      return this;
    }
  }
}
