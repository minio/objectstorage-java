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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.minio.messages.RetentionMode;
import io.minio.messages.Tags;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** An object entry for {@link PutObjectFanOutArgs}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PutObjectFanOutEntry extends BaseArgs {
  @JsonProperty("key")
  String key;

  @JsonProperty("metadata")
  Map<String, String> userMetadata;

  @JsonProperty("tags")
  Map<String, String> tags;

  @JsonProperty("contentType")
  String contentType;

  @JsonProperty("contentEncoding")
  String contentEncoding;

  @JsonProperty("contentDisposition")
  String contentDisposition;

  @JsonProperty("contentLanguage")
  String contentLanguage;

  @JsonProperty("cacheControl")
  String cacheControl;

  @JsonProperty("retention")
  RetentionMode retention;

  @JsonProperty("retainUntil")
  ZonedDateTime retainUntilDate;

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link PutObjectFanOutEntry}. */
  public static final class Builder extends BaseArgs.Builder<Builder, PutObjectFanOutEntry> {
    @Override
    protected void validate(PutObjectFanOutEntry args) {
      Utils.validateNotEmptyString(args.key, "key");
    }

    public Builder key(String key) {
      Utils.validateNotEmptyString(key, "key");
      operations.add(args -> args.key = key);
      return this;
    }

    public Builder userMetadata(Map<String, String> userMetadata) {
      final Map<String, String> userMetadataCopy = new HashMap<>();
      if (userMetadata != null) {
        for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
          String key = entry.getKey();
          userMetadataCopy.put(
              (key.toLowerCase(Locale.US).startsWith("x-amz-meta-") ? "" : "x-amz-meta-") + key,
              entry.getValue());
        }
      }

      operations.add(args -> args.userMetadata = userMetadata == null ? null : userMetadataCopy);
      return this;
    }

    public Builder tags(Map<String, String> map) {
      Tags.newObjectTags(map);
      operations.add(args -> args.tags = map);
      return this;
    }

    public Builder tags(Tags tags) {
      operations.add(args -> args.tags = tags == null ? null : tags.get());
      return this;
    }

    public Builder contentType(String contentType) {
      operations.add(args -> args.contentType = contentType);
      return this;
    }

    public Builder contentEncoding(String contentEncoding) {
      operations.add(args -> args.contentEncoding = contentEncoding);
      return this;
    }

    public Builder contentDisposition(String contentDisposition) {
      operations.add(args -> args.contentDisposition = contentDisposition);
      return this;
    }

    public Builder contentLanguage(String contentLanguage) {
      operations.add(args -> args.contentLanguage = contentLanguage);
      return this;
    }

    public Builder cacheControl(String cacheControl) {
      operations.add(args -> args.cacheControl = cacheControl);
      return this;
    }

    public Builder retention(RetentionMode retention) {
      operations.add(args -> args.retention = retention);
      return this;
    }

    public Builder retainUntilDate(ZonedDateTime retainUntilDate) {
      operations.add(args -> args.retainUntilDate = retainUntilDate);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PutObjectFanOutEntry)) return false;
    if (!super.equals(o)) return false;
    PutObjectFanOutEntry that = (PutObjectFanOutEntry) o;
    return Objects.equals(key, that.key)
        && Objects.equals(userMetadata, that.userMetadata)
        && Objects.equals(tags, that.tags)
        && Objects.equals(contentType, that.contentType)
        && Objects.equals(contentEncoding, that.contentEncoding)
        && Objects.equals(contentDisposition, that.contentDisposition)
        && Objects.equals(contentLanguage, that.contentLanguage)
        && Objects.equals(cacheControl, that.cacheControl)
        && Objects.equals(retention, that.retention)
        && Objects.equals(retainUntilDate, that.retainUntilDate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        key,
        userMetadata,
        tags,
        contentType,
        contentEncoding,
        contentDisposition,
        contentLanguage,
        cacheControl,
        retention,
        retainUntilDate);
  }
}
