/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2025 MinIO, Inc.
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

import java.io.RandomAccessFile;
import java.util.Objects;

/** Argument class of {@link MinioAsyncClient#uploadPart} and {@link MinioClient#uploadPart}. */
public class UploadPartArgs extends ObjectArgs {
  private String uploadId;
  private int partNumber;
  private RandomAccessFile file;
  private ByteBuffer buffer;
  private byte[] data;
  private Long length;

  public String uploadId() {
    return uploadId;
  }

  public int partNumber() {
    return partNumber;
  }

  public RandomAccessFile file() {
    return file;
  }

  public ByteBuffer buffer() {
    return buffer;
  }

  public byte[] data() {
    return data;
  }

  public Long length() {
    return length;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link UploadPartArgs}. */
  public static final class Builder extends ObjectArgs.Builder<Builder, UploadPartArgs> {
    @Override
    protected void validate(UploadPartArgs args) {
      super.validate(args);
      Utils.validateNotEmptyString(args.uploadId, "upload ID");
      if (args.partNumber <= 0) {
        throw new IllegalArgumentException("valid part number must be provided");
      }
      if (!((args.file != null) != (args.buffer != null) != (args.data != null)
          && !(args.file != null && args.buffer != null && args.data != null))) {
        throw new IllegalArgumentException("only one of file, buffer or data must be provided");
      }
    }

    public Builder uploadId(String uploadId) {
      Utils.validateNotEmptyString(uploadId, "upload ID");
      operations.add(args -> args.uploadId = uploadId);
      return this;
    }

    public Builder partNumber(int partNumber) {
      if (partNumber <= 0) throw new IllegalArgumentException("valid part number must be provided");
      operations.add(args -> args.partNumber = partNumber);
      return this;
    }

    public Builder file(RandomAccessFile file, long length) {
      Utils.validateNotNull(file, "file");
      if (length < 0) throw new IllegalArgumentException("valid length must be provided");
      operations.add(args -> args.file = file);
      operations.add(args -> args.length = length);
      return this;
    }

    public Builder buffer(ByteBuffer buffer) {
      Utils.validateNotNull(buffer, "buffer");
      operations.add(args -> args.buffer = buffer);
      return this;
    }

    public Builder data(byte[] data, int length) {
      Utils.validateNotNull(data, "data");
      if (length < 0) throw new IllegalArgumentException("valid length must be provided");
      operations.add(args -> args.data = data);
      operations.add(args -> args.length = (long) length);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UploadPartArgs)) return false;
    if (!super.equals(o)) return false;
    UploadPartArgs that = (UploadPartArgs) o;
    return Objects.equals(uploadId, that.uploadId)
        && Objects.equals(partNumber, that.partNumber)
        && Objects.equals(file, that.file)
        && Objects.equals(buffer, that.buffer)
        && Objects.equals(data, that.data)
        && Objects.equals(length, that.length);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), uploadId, partNumber, file, buffer, data, length);
  }
}
