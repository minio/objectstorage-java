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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.minio.messages.Checksum;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import okhttp3.HttpUrl;

/**
 * Argument class of {@link MinioAsyncClient#putObjectFanOut} and {@link
 * MinioClient#putObjectFanOut}.
 */
public class PutObjectFanOutArgs extends BucketArgs {
  private static final ObjectMapper objectMapper =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
          .build();

  private InputStream stream;
  private long size;
  private List<PutObjectFanOutEntry> entries = null;
  private Checksum checksum;
  private ServerSideEncryption sse;

  public InputStream stream() {
    return stream;
  }

  public long size() {
    return size;
  }

  public List<PutObjectFanOutEntry> entries() {
    return entries;
  }

  public Checksum checksum() {
    return checksum;
  }

  public ServerSideEncryption sse() {
    return sse;
  }

  public String fanOutList() throws JsonProcessingException {
    if (entries == null) return null;

    StringBuilder builder = new StringBuilder();
    for (PutObjectFanOutEntry entry : entries) {
      builder.append(objectMapper.writeValueAsString(entry));
    }
    return builder.toString();
  }

  public void validateSse(HttpUrl url) {
    checkSse(sse, url);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link PutObjectFanOutArgs}. */
  public static final class Builder extends BucketArgs.Builder<Builder, PutObjectFanOutArgs> {
    @Override
    protected void validate(PutObjectFanOutArgs args) {
      super.validate(args);
      Utils.validateNotNull(args.stream, "stream");
      Utils.validateNotNull(args.entries, "fan-out entries");
    }

    public Builder stream(InputStream stream, long size) {
      Utils.validateNotNull(stream, "stream");
      if (size < 0) {
        throw new IllegalArgumentException("invalid stream size " + size);
      }
      if (size > ObjectWriteArgs.MAX_PART_SIZE) {
        throw new IllegalArgumentException(
            "size " + size + " is not supported; maximum allowed 5GiB");
      }

      operations.add(args -> args.stream = stream);
      operations.add(args -> args.size = size);
      return this;
    }

    public Builder entries(List<PutObjectFanOutEntry> entries) {
      operations.add(args -> args.entries = entries);
      return this;
    }

    public Builder checksum(Checksum checksum) {
      operations.add(args -> args.checksum = checksum);
      return this;
    }

    public Builder sse(ServerSideEncryption sse) {
      operations.add(args -> args.sse = sse);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PutObjectFanOutArgs)) return false;
    if (!super.equals(o)) return false;
    PutObjectFanOutArgs that = (PutObjectFanOutArgs) o;
    return Objects.equals(stream, that.stream)
        && size == that.size
        && Objects.equals(entries, that.entries)
        && Objects.equals(checksum, that.checksum)
        && Objects.equals(sse, that.sse);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), stream, size, entries, checksum, sse);
  }
}
