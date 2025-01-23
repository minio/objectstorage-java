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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/** Argument class of {@link MinioAsyncClient#putObject} and {@link MinioClient#putObject}. */
public class PutObjectArgs extends PutObjectBaseArgs {
  private BufferedInputStream stream;

  public BufferedInputStream stream() {
    return stream;
  }

  /**
   * Gets content type. It returns if content type is set (or) value of "Content-Type" header (or)
   * default "application/octet-stream".
   */
  public String contentType() throws IOException {
    String contentType = super.contentType();
    return (contentType != null) ? contentType : "application/octet-stream";
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link PutObjectArgs}. */
  public static final class Builder extends PutObjectBaseArgs.Builder<Builder, PutObjectArgs> {
    @Override
    protected void validate(PutObjectArgs args) {
      super.validate(args);
      Utils.validateNotNull(args.stream, "stream");
    }

    /**
     * Sets stream to upload. Two ways to provide object/part sizes.
     *
     * <ul>
     *   <li>If object size is unknown, pass -1 to objectSize and pass valid partSize.
     *   <li>If object size is known, pass -1 to partSize for auto detect; else pass valid partSize
     *       to control memory usage and no. of parts in upload.
     *   <li>If partSize is greater than objectSize, objectSize is used as partSize.
     * </ul>
     *
     * <p>A valid part size is between 5MiB to 5GiB (both limits inclusive).
     */
    public Builder stream(InputStream stream, long objectSize, long partSize) {
      Utils.validateNotNull(stream, "stream");

      long[] partinfo = getPartInfo(objectSize, partSize);
      long pSize = partinfo[0];
      int pCount = (int) partinfo[1];

      final BufferedInputStream bis =
          (stream instanceof BufferedInputStream)
              ? (BufferedInputStream) stream
              : new BufferedInputStream(stream);
      return setStream(bis, objectSize, pSize, pCount);
    }

    private Builder setStream(
        BufferedInputStream stream, long objectSize, long partSize, int partCount) {
      operations.add(args -> args.stream = stream);
      operations.add(args -> args.objectSize = objectSize);
      operations.add(args -> args.partSize = partSize);
      operations.add(args -> args.partCount = partCount);
      return this;
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
    if (!(o instanceof PutObjectArgs)) return false;
    if (!super.equals(o)) return false;
    PutObjectArgs that = (PutObjectArgs) o;
    return Objects.equals(stream, that.stream);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), stream);
  }
}
