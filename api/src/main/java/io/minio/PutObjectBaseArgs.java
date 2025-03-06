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
import java.util.Objects;
import okhttp3.MediaType;

/** Base argument class for {@link PutObjectArgs} and {@link UploadObjectArgs}. */
public abstract class PutObjectBaseArgs extends ObjectWriteArgs {
  protected long objectSize;
  protected long partSize;
  protected int partCount;
  protected String contentType;
  protected boolean preloadData;

  public long objectSize() {
    return objectSize;
  }

  public long partSize() {
    return partSize;
  }

  public int partCount() {
    return partCount;
  }

  /** Gets content type. It returns if content type is set (or) value of "Content-Type" header. */
  public String contentType() throws IOException {
    if (contentType != null) {
      return contentType;
    }

    if (this.headers().containsKey("Content-Type")) {
      return this.headers().get("Content-Type").iterator().next();
    }

    return null;
  }

  public boolean preloadData() {
    return preloadData;
  }

  /** Base argument builder class for {@link PutObjectBaseArgs}. */
  @SuppressWarnings("unchecked") // Its safe to type cast to B as B is inherited by this class
  public abstract static class Builder<B extends Builder<B, A>, A extends PutObjectBaseArgs>
      extends ObjectWriteArgs.Builder<B, A> {
    protected void validateContentType(String contentType) {
      Utils.validateNotEmptyString(contentType, "content type");
      if (MediaType.parse(contentType) == null) {
        throw new IllegalArgumentException(
            "invalid content type '" + contentType + "' as per RFC 2045");
      }
    }

    private void validateSizes(long objectSize, long partSize) {
      if (partSize > 0) {
        if (partSize < MIN_MULTIPART_SIZE) {
          throw new IllegalArgumentException(
              "part size " + partSize + " is not supported; minimum allowed 5MiB");
        }

        if (partSize > MAX_PART_SIZE) {
          throw new IllegalArgumentException(
              "part size " + partSize + " is not supported; maximum allowed 5GiB");
        }
      }

      if (objectSize >= 0) {
        if (objectSize > MAX_OBJECT_SIZE) {
          throw new IllegalArgumentException(
              "object size " + objectSize + " is not supported; maximum allowed 5TiB");
        }
      } else if (partSize <= 0) {
        throw new IllegalArgumentException(
            "valid part size must be provided when object size is unknown");
      }
    }

    protected long[] getPartInfo(long objectSize, long partSize) {
      validateSizes(objectSize, partSize);

      if (objectSize < 0) return new long[] {partSize, -1};

      if (partSize <= 0) {
        // Calculate part size by multiple of MIN_MULTIPART_SIZE.
        double dPartSize = Math.ceil((double) objectSize / MAX_MULTIPART_COUNT);
        dPartSize = Math.ceil(dPartSize / MIN_MULTIPART_SIZE) * MIN_MULTIPART_SIZE;
        partSize = (long) dPartSize;
      }

      if (partSize > objectSize) partSize = objectSize;
      long partCount = partSize > 0 ? (long) Math.ceil((double) objectSize / partSize) : 1;
      if (partCount > MAX_MULTIPART_COUNT) {
        throw new IllegalArgumentException(
            "object size "
                + objectSize
                + " and part size "
                + partSize
                + " make more than "
                + MAX_MULTIPART_COUNT
                + "parts for upload");
      }

      return new long[] {partSize, partCount};
    }

    /**
     * Sets flag to control data preload of stream/file. When this flag is enabled, entire
     * part/object data is loaded into memory to enable connection retry on network failure in the
     * middle of upload.
     *
     * @deprecated As this behavior is enabled by default and cannot be turned off.
     */
    @Deprecated
    public B preloadData(boolean preloadData) {
      operations.add(args -> args.preloadData = preloadData);
      return (B) this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PutObjectBaseArgs)) return false;
    if (!super.equals(o)) return false;
    PutObjectBaseArgs that = (PutObjectBaseArgs) o;
    return objectSize == that.objectSize
        && partSize == that.partSize
        && partCount == that.partCount
        && Objects.equals(contentType, that.contentType)
        && preloadData == that.preloadData;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), objectSize, partSize, partCount, contentType, preloadData);
  }
}
