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

import io.minio.messages.Retention;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#setObjectRetention} and {@link
 * MinioClient#setObjectRetention}.
 */
public class SetObjectRetentionArgs extends ObjectVersionArgs {
  private Retention config;
  private boolean bypassGovernanceMode;

  public Retention config() {
    return config;
  }

  public boolean bypassGovernanceMode() {
    return bypassGovernanceMode;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link SetObjectRetentionArgs}. */
  public static final class Builder
      extends ObjectVersionArgs.Builder<Builder, SetObjectRetentionArgs> {
    private void validateConfig(Retention config) {
      Utils.validateNotNull(config, "retention configuration");
    }

    protected void validate(SetObjectRetentionArgs args) {
      super.validate(args);
      validateConfig(args.config);
    }

    public Builder config(Retention config) {
      validateConfig(config);
      operations.add(args -> args.config = config);
      return this;
    }

    public Builder bypassGovernanceMode(boolean bypassGovernanceMode) {
      operations.add(args -> args.bypassGovernanceMode = bypassGovernanceMode);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SetObjectRetentionArgs)) return false;
    if (!super.equals(o)) return false;
    SetObjectRetentionArgs that = (SetObjectRetentionArgs) o;
    return bypassGovernanceMode == that.bypassGovernanceMode && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), config, bypassGovernanceMode);
  }
}
