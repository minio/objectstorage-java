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

import io.minio.messages.NotificationConfiguration;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#setBucketNotification} and {@link
 * MinioClient#setBucketNotification}.
 */
public class SetBucketNotificationArgs extends BucketArgs {
  private NotificationConfiguration config;

  public NotificationConfiguration config() {
    return config;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link SetBucketNotificationArgs}. */
  public static final class Builder extends BucketArgs.Builder<Builder, SetBucketNotificationArgs> {
    private void validateConfig(NotificationConfiguration config) {
      Utils.validateNotNull(config, "notification configuration");
    }

    protected void validate(SetBucketNotificationArgs args) {
      super.validate(args);
      validateConfig(args.config);
    }

    public Builder config(NotificationConfiguration config) {
      validateConfig(config);
      operations.add(args -> args.config = config);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SetBucketNotificationArgs)) return false;
    if (!super.equals(o)) return false;
    SetBucketNotificationArgs that = (SetBucketNotificationArgs) o;
    return Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), config);
  }
}
