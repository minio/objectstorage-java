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

import io.minio.messages.ReplicationConfiguration;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#setBucketReplication} and {@link
 * MinioClient#setBucketReplication}.
 */
public class SetBucketReplicationArgs extends BucketArgs {
  private ReplicationConfiguration config;
  private String objectLockToken;

  public ReplicationConfiguration config() {
    return config;
  }

  public String objectLockToken() {
    return objectLockToken;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link SetBucketReplicationArgs}. */
  public static final class Builder extends BucketArgs.Builder<Builder, SetBucketReplicationArgs> {
    private void validateConfig(ReplicationConfiguration config) {
      Utils.validateNotNull(config, "replication configuration");
    }

    private void validateObjectLockToken(String token) {
      Utils.validateNullOrNotEmptyString(token, "object lock token");
    }

    @Override
    protected void validate(SetBucketReplicationArgs args) {
      super.validate(args);
      validateConfig(args.config);
      validateObjectLockToken(args.objectLockToken);
    }

    public Builder config(ReplicationConfiguration config) {
      validateConfig(config);
      operations.add(args -> args.config = config);
      return this;
    }

    public Builder objectLockToken(String token) {
      validateObjectLockToken(token);
      operations.add(args -> args.objectLockToken = token);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SetBucketReplicationArgs)) return false;
    if (!super.equals(o)) return false;
    SetBucketReplicationArgs that = (SetBucketReplicationArgs) o;
    return Objects.equals(config, that.config)
        && Objects.equals(objectLockToken, that.objectLockToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), config, objectLockToken);
  }
}
