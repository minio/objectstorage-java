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

import io.minio.messages.LegalHold;
import io.minio.messages.RetentionMode;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import okhttp3.Headers;

/** Response of {@link S3Base#statObjectAsync}. */
public class StatObjectResponse extends GenericResponse {
  private String etag;
  private long size;
  private ZonedDateTime lastModified;
  private RetentionMode retentionMode;
  private ZonedDateTime retentionRetainUntilDate;
  private LegalHold legalHold;
  private boolean deleteMarker;
  private Map<String, String> userMetadata;

  public StatObjectResponse(Headers headers, String bucket, String region, String object) {
    super(headers, bucket, region, object);
    String value;

    value = headers.get("ETag");
    this.etag = (value != null ? value.replaceAll("\"", "") : "");

    value = headers.get("Content-Length");
    this.size = (value != null ? Long.parseLong(value) : -1);

    this.lastModified =
        ZonedDateTime.parse(headers.get("Last-Modified"), Time.HTTP_HEADER_DATE_FORMAT);

    value = headers.get("x-amz-object-lock-mode");
    this.retentionMode = (value != null ? RetentionMode.valueOf(value) : null);

    value = headers.get("x-amz-object-lock-retain-until-date");
    this.retentionRetainUntilDate =
        value == null ? null : Time.S3Time.fromString(value).toZonedDateTime();

    this.legalHold = new LegalHold("ON".equals(headers.get("x-amz-object-lock-legal-hold")));

    this.deleteMarker = Boolean.parseBoolean(headers.get("x-amz-delete-marker"));

    Map<String, String> userMetadata = new HashMap<>();
    for (String key : headers.names()) {
      if (key.toLowerCase(Locale.US).startsWith("x-amz-meta-")) {
        userMetadata.put(
            key.toLowerCase(Locale.US).substring("x-amz-meta-".length(), key.length()),
            headers.get(key));
      }
    }

    this.userMetadata = Utils.unmodifiableMap(userMetadata);
  }

  public String etag() {
    return etag;
  }

  public long size() {
    return size;
  }

  public ZonedDateTime lastModified() {
    return lastModified;
  }

  public RetentionMode retentionMode() {
    return retentionMode;
  }

  public ZonedDateTime retentionRetainUntilDate() {
    return retentionRetainUntilDate;
  }

  public LegalHold legalHold() {
    return legalHold;
  }

  public boolean deleteMarker() {
    return deleteMarker;
  }

  public String versionId() {
    return this.headers().get("x-amz-version-id");
  }

  public String contentType() {
    return this.headers().get("Content-Type");
  }

  public Map<String, String> userMetadata() {
    return userMetadata;
  }

  @Override
  public String toString() {
    return "ObjectStat{"
        + "bucket="
        + bucket()
        + ", object="
        + object()
        + ", last-modified="
        + lastModified
        + ", size="
        + size
        + "}";
  }
}
