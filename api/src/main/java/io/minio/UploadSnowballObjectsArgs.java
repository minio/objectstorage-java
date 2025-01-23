/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2021 MinIO, Inc.
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

import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;

/**
 * Argument class of {@link MinioAsyncClient#uploadSnowballObjects} and {@link
 * MinioClient#uploadSnowballObjects}.
 */
public class UploadSnowballObjectsArgs extends ObjectWriteArgs {
  private static final Random random = new Random(new SecureRandom().nextLong());

  private Iterable<SnowballObject> objects;
  private String stagingFilename;
  private boolean compression;

  public Iterable<SnowballObject> objects() {
    return this.objects;
  }

  public String stagingFilename() {
    return stagingFilename;
  }

  public boolean compression() {
    return compression;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link UploadSnowballObjectsArgs}. */
  public static final class Builder
      extends ObjectWriteArgs.Builder<Builder, UploadSnowballObjectsArgs> {
    private void validateObjects(Iterable<SnowballObject> objects) {
      Utils.validateNotNull(objects, "objects");
    }

    @Override
    protected void validate(UploadSnowballObjectsArgs args) {
      args.objectName = "snowball." + random.nextLong() + ".tar";
      validateObjects(args.objects);
      super.validate(args);
    }

    public Builder objects(Iterable<SnowballObject> objects) {
      validateObjects(objects);
      operations.add(args -> args.objects = objects);
      return this;
    }

    public Builder stagingFilename(String stagingFilename) {
      if (stagingFilename != null && stagingFilename.isEmpty()) {
        throw new IllegalArgumentException("staging filename must not be empty");
      }
      operations.add(args -> args.stagingFilename = stagingFilename);
      return this;
    }

    public Builder compression(boolean compression) {
      operations.add(args -> args.compression = compression);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UploadSnowballObjectsArgs)) return false;
    if (!super.equals(o)) return false;
    UploadSnowballObjectsArgs that = (UploadSnowballObjectsArgs) o;
    return Objects.equals(objects, that.objects)
        && Objects.equals(stagingFilename, that.stagingFilename)
        && compression == that.compression;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), objects, stagingFilename, compression);
  }
}
