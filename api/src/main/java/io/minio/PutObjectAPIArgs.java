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

import com.google.common.collect.Multimap;
import java.io.RandomAccessFile;
import java.util.Objects;

/** Argument class of {@link MinioAsyncClient#putObjectAPI} and {@link MinioClient#putObjectAPI}. */
public class PutObjectAPIArgs extends ObjectArgs {
  private RandomAccessFile file;
  private ByteBuffer buffer;
  private byte[] data;
  private Long length;
  private Multimap<String, String> headers;

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

  public Multimap<String, String> headers() {
    return headers;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link PutObjectAPIArgs}. */
  public static final class Builder extends ObjectArgs.Builder<Builder, PutObjectAPIArgs> {
    @Override
    protected void validate(PutObjectAPIArgs args) {
      super.validate(args);
      if (!((args.file != null) != (args.buffer != null) != (args.data != null)
          && !(args.file != null && args.buffer != null && args.data != null))) {
        throw new IllegalArgumentException("only one of file, buffer or data must be provided");
      }
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

    public Builder length(byte[] data, int length) {
      Utils.validateNotNull(data, "data");
      if (length < 0) throw new IllegalArgumentException("valid length must be provided");
      operations.add(args -> args.data = data);
      operations.add(args -> args.length = (long) length);
      return this;
    }

    public Builder headers(Multimap<String, String> headers) {
      operations.add(args -> args.headers = headers);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PutObjectAPIArgs)) return false;
    if (!super.equals(o)) return false;
    PutObjectAPIArgs that = (PutObjectAPIArgs) o;
    return Objects.equals(file, that.file)
        && Objects.equals(buffer, that.buffer)
        && Objects.equals(data, that.data)
        && Objects.equals(length, that.length)
        && Objects.equals(headers, that.headers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), file, buffer, data, length, headers);
  }
}
