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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

/** Argument class of {@link MinioAsyncClient#uploadObject} and {@link MinioClient#uploadObject}. */
public class UploadObjectArgs extends PutObjectBaseArgs {
  private String filename;

  public String filename() {
    return filename;
  }

  /**
   * Gets content type. It returns if content type is set (or) value of "Content-Type" header (or)
   * probed content type of file (or) default "application/octet-stream".
   */
  public String contentType() throws IOException {
    String contentType = super.contentType();
    if (contentType != null) {
      return contentType;
    }

    contentType = Files.probeContentType(Paths.get(filename));
    return (contentType != null && !contentType.isEmpty())
        ? contentType
        : "application/octet-stream";
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link UploadObjectArgs}. */
  public static final class Builder extends PutObjectBaseArgs.Builder<Builder, UploadObjectArgs> {
    @Override
    protected void validate(UploadObjectArgs args) {
      super.validate(args);
      validateFilename(args.filename);
    }

    private void validateFilename(String filename) {
      Utils.validateNotEmptyString(filename, "filename");
      if (!Files.isRegularFile(Paths.get(filename))) {
        throw new IllegalArgumentException(filename + " not a regular file");
      }
    }

    public Builder filename(String filename, long partSize) throws IOException {
      validateFilename(filename);
      final long objectSize = Files.size(Paths.get(filename));

      long[] partinfo = getPartInfo(objectSize, partSize);
      final long pSize = partinfo[0];
      final int partCount = (int) partinfo[1];

      operations.add(args -> args.filename = filename);
      operations.add(args -> args.objectSize = objectSize);
      operations.add(args -> args.partSize = pSize);
      operations.add(args -> args.partCount = partCount);
      return this;
    }

    public Builder filename(String filename) throws IOException {
      return this.filename(filename, 0);
    }

    public Builder contentType(String contentType) {
      validateContentType(contentType);
      operations.add(args -> args.contentType = contentType);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UploadObjectArgs)) return false;
    if (!super.equals(o)) return false;
    UploadObjectArgs that = (UploadObjectArgs) o;
    return Objects.equals(filename, that.filename);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), filename);
  }
}
