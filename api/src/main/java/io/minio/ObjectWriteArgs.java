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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.minio.messages.Retention;
import io.minio.messages.Tags;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import okhttp3.HttpUrl;

/** Base argument class for writing object. */
public abstract class ObjectWriteArgs extends ObjectArgs {
  // allowed maximum object size is 5TiB.
  public static final long MAX_OBJECT_SIZE = 5L * 1024 * 1024 * 1024 * 1024;
  // allowed minimum part size is 5MiB in multipart upload.
  public static final int MIN_MULTIPART_SIZE = 5 * 1024 * 1024;
  // allowed maximum part size is 5GiB in multipart upload.
  public static final long MAX_PART_SIZE = 5L * 1024 * 1024 * 1024;
  public static final int MAX_MULTIPART_COUNT = 10000;

  protected Multimap<String, String> headers =
      Multimaps.unmodifiableMultimap(HashMultimap.create());
  protected Multimap<String, String> userMetadata =
      Multimaps.unmodifiableMultimap(HashMultimap.create());
  protected ServerSideEncryption sse;
  protected Tags tags = new Tags();
  protected Retention retention;
  protected boolean legalHold;

  public Multimap<String, String> headers() {
    return headers;
  }

  public Multimap<String, String> userMetadata() {
    return userMetadata;
  }

  public ServerSideEncryption sse() {
    return sse;
  }

  public Tags tags() {
    return tags;
  }

  public Retention retention() {
    return retention;
  }

  public boolean legalHold() {
    return legalHold;
  }

  public Multimap<String, String> genHeaders() {
    Multimap<String, String> headers = HashMultimap.create();

    headers.putAll(this.headers);
    headers.putAll(userMetadata);

    if (sse != null) {
      headers.putAll(Multimaps.forMap(sse.headers()));
    }

    String tagging =
        tags.get().entrySet().stream()
            .map(e -> Utils.encode(e.getKey()) + "=" + Utils.encode(e.getValue()))
            .collect(Collectors.joining("&"));
    if (!tagging.isEmpty()) {
      headers.put("x-amz-tagging", tagging);
    }

    if (retention != null && retention.mode() != null) {
      headers.put("x-amz-object-lock-mode", retention.mode().name());
      headers.put(
          "x-amz-object-lock-retain-until-date",
          retention.retainUntilDate().format(Time.ISO8601UTC_FORMAT));
    }

    if (legalHold) {
      headers.put("x-amz-object-lock-legal-hold", "ON");
    }

    return headers;
  }

  protected void validateSse(HttpUrl url) {
    checkSse(sse, url);
  }

  /** Base argument builder class for {@link ObjectWriteArgs}. */
  @SuppressWarnings("unchecked") // Its safe to type cast to B as B is inherited by this class
  public abstract static class Builder<B extends Builder<B, A>, A extends ObjectWriteArgs>
      extends ObjectArgs.Builder<B, A> {
    public B headers(Map<String, String> headers) {
      final Multimap<String, String> headersCopy = Utils.newMultimap(headers);
      operations.add(args -> args.headers = headersCopy);
      return (B) this;
    }

    public B headers(Multimap<String, String> headers) {
      final Multimap<String, String> headersCopy = Utils.newMultimap(headers);
      operations.add(args -> args.headers = headersCopy);
      return (B) this;
    }

    public B userMetadata(Map<String, String> userMetadata) {
      return userMetadata((userMetadata == null) ? null : Multimaps.forMap(userMetadata));
    }

    public B userMetadata(Multimap<String, String> userMetadata) {
      Multimap<String, String> userMetadataCopy = HashMultimap.create();
      if (userMetadata != null) {
        for (String key : userMetadata.keySet()) {
          userMetadataCopy.putAll(
              (key.toLowerCase(Locale.US).startsWith("x-amz-meta-") ? "" : "x-amz-meta-") + key,
              userMetadata.get(key));
        }
      }

      final Multimap<String, String> finalUserMetadata =
          Multimaps.unmodifiableMultimap(userMetadataCopy);
      operations.add(args -> args.userMetadata = finalUserMetadata);
      return (B) this;
    }

    public B sse(ServerSideEncryption sse) {
      operations.add(args -> args.sse = sse);
      return (B) this;
    }

    public B tags(Map<String, String> map) {
      final Tags tags = (map == null) ? new Tags() : Tags.newObjectTags(map);
      operations.add(args -> args.tags = tags);
      return (B) this;
    }

    public B tags(Tags tags) {
      Tags tagsCopy = (tags == null) ? new Tags() : Tags.newObjectTags(tags.get());
      operations.add(args -> args.tags = tagsCopy);
      return (B) this;
    }

    public B retention(Retention retention) {
      operations.add(args -> args.retention = retention);
      return (B) this;
    }

    public B legalHold(boolean flag) {
      operations.add(args -> args.legalHold = flag);
      return (B) this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ObjectWriteArgs)) return false;
    if (!super.equals(o)) return false;
    ObjectWriteArgs that = (ObjectWriteArgs) o;
    return legalHold == that.legalHold
        && Objects.equals(headers, that.headers)
        && Objects.equals(userMetadata, that.userMetadata)
        && Objects.equals(sse, that.sse)
        && Objects.equals(tags, that.tags)
        && Objects.equals(retention, that.retention);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), headers, userMetadata, sse, tags, retention, legalHold);
  }
}
