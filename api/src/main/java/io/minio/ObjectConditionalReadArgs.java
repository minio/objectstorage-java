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
import java.time.ZonedDateTime;
import java.util.Objects;

/** Base argument class holds condition properties for reading object. */
public abstract class ObjectConditionalReadArgs extends ObjectReadArgs {
  protected Long offset;
  protected Long length;
  protected String matchETag;
  protected String notMatchETag;
  protected ZonedDateTime modifiedSince;
  protected ZonedDateTime unmodifiedSince;

  public Long offset() {
    return offset;
  }

  public Long length() {
    return length;
  }

  public String matchETag() {
    return matchETag;
  }

  public String notMatchETag() {
    return notMatchETag;
  }

  public ZonedDateTime modifiedSince() {
    return modifiedSince;
  }

  public ZonedDateTime unmodifiedSince() {
    return unmodifiedSince;
  }

  public Multimap<String, String> getHeaders() {
    Long offset = this.offset;
    Long length = this.length;
    if (length != null && offset == null) {
      offset = 0L;
    }

    String range = null;
    if (offset != null) {
      range = "bytes=" + offset + "-";
      if (length != null) {
        range = range + (offset + length - 1);
      }
    }

    Multimap<String, String> headers = HashMultimap.create();

    if (range != null) headers.put("Range", range);
    if (matchETag != null) headers.put("if-match", matchETag);
    if (notMatchETag != null) headers.put("if-none-match", notMatchETag);

    if (modifiedSince != null) {
      headers.put("if-modified-since", modifiedSince.format(Time.HTTP_HEADER_DATE_FORMAT));
    }

    if (unmodifiedSince != null) {
      headers.put("if-unmodified-since", unmodifiedSince.format(Time.HTTP_HEADER_DATE_FORMAT));
    }

    if (ssec != null) headers.putAll(Multimaps.forMap(ssec.headers()));

    return headers;
  }

  public Multimap<String, String> genCopyHeaders() {
    Multimap<String, String> headers = HashMultimap.create();

    String copySource = Utils.encodePath("/" + bucketName + "/" + objectName);
    if (versionId != null) {
      copySource += "?versionId=" + Utils.encode(versionId);
    }

    headers.put("x-amz-copy-source", copySource);

    if (ssec != null) headers.putAll(Multimaps.forMap(ssec.copySourceHeaders()));
    if (matchETag != null) headers.put("x-amz-copy-source-if-match", matchETag);
    if (notMatchETag != null) headers.put("x-amz-copy-source-if-none-match", notMatchETag);

    if (modifiedSince != null) {
      headers.put(
          "x-amz-copy-source-if-modified-since",
          modifiedSince.format(Time.HTTP_HEADER_DATE_FORMAT));
    }

    if (unmodifiedSince != null) {
      headers.put(
          "x-amz-copy-source-if-unmodified-since",
          unmodifiedSince.format(Time.HTTP_HEADER_DATE_FORMAT));
    }

    return headers;
  }

  /** Base argument builder class for {@link ObjectConditionalReadArgs}. */
  @SuppressWarnings("unchecked") // Its safe to type cast to B as B is inherited by this class
  public abstract static class Builder<B extends Builder<B, A>, A extends ObjectConditionalReadArgs>
      extends ObjectReadArgs.Builder<B, A> {
    private void validateLength(Long length) {
      if (length != null && length <= 0) {
        throw new IllegalArgumentException("length should be greater than zero");
      }
    }

    private void validateOffset(Long offset) {
      if (offset != null && offset < 0) {
        throw new IllegalArgumentException("offset should be zero or greater");
      }
    }

    public B offset(Long offset) {
      validateOffset(offset);
      operations.add(args -> args.offset = offset);
      return (B) this;
    }

    public B length(Long length) {
      validateLength(length);
      operations.add(args -> args.length = length);
      return (B) this;
    }

    public B matchETag(String etag) {
      Utils.validateNullOrNotEmptyString(etag, "etag");
      operations.add(args -> args.matchETag = etag);
      return (B) this;
    }

    public B notMatchETag(String etag) {
      Utils.validateNullOrNotEmptyString(etag, "etag");
      operations.add(args -> args.notMatchETag = etag);
      return (B) this;
    }

    public B modifiedSince(ZonedDateTime modifiedTime) {
      operations.add(args -> args.modifiedSince = modifiedTime);
      return (B) this;
    }

    public B unmodifiedSince(ZonedDateTime unmodifiedTime) {
      operations.add(args -> args.unmodifiedSince = unmodifiedTime);
      return (B) this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ObjectConditionalReadArgs)) return false;
    if (!super.equals(o)) return false;
    ObjectConditionalReadArgs that = (ObjectConditionalReadArgs) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(length, that.length)
        && Objects.equals(matchETag, that.matchETag)
        && Objects.equals(notMatchETag, that.notMatchETag)
        && Objects.equals(modifiedSince, that.modifiedSince)
        && Objects.equals(unmodifiedSince, that.unmodifiedSince);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), offset, length, matchETag, notMatchETag, modifiedSince, unmodifiedSince);
  }
}
