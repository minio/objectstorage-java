/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.minio.credentials;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.minio.Time;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/** Object representation of credentials access key, secret key and session token. */
@Root(name = "Credentials", strict = false)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Credentials {
  @Element(name = "AccessKeyId")
  @JsonProperty("accessKey")
  private final String accessKey;

  @Element(name = "SecretAccessKey")
  @JsonProperty("secretKey")
  private final String secretKey;

  @Element(name = "SessionToken")
  @JsonProperty("sessionToken")
  private final String sessionToken;

  @Element(name = "Expiration")
  @JsonProperty("expiration")
  private final Time.S3Time expiration;

  public Credentials(
      @Nonnull @Element(name = "AccessKeyId") @JsonProperty("accessKey") String accessKey,
      @Nonnull @Element(name = "SecretAccessKey") @JsonProperty("secretKey") String secretKey,
      @Nullable @Element(name = "SessionToken") @JsonProperty("sessionToken") String sessionToken,
      @Nullable @Element(name = "Expiration") @JsonProperty("expiration") Time.S3Time expiration) {
    this.accessKey = Objects.requireNonNull(accessKey, "AccessKey must not be null");
    this.secretKey = Objects.requireNonNull(secretKey, "SecretKey must not be null");
    if (accessKey.isEmpty() || secretKey.isEmpty()) {
      throw new IllegalArgumentException("AccessKey and SecretKey must not be empty");
    }
    this.sessionToken = sessionToken;
    this.expiration = expiration;
  }

  public String accessKey() {
    return accessKey;
  }

  public String secretKey() {
    return secretKey;
  }

  public String sessionToken() {
    return sessionToken;
  }

  public ZonedDateTime expiration() {
    return expiration == null ? null : expiration.toZonedDateTime();
  }

  public boolean isExpired() {
    if (expiration == null) {
      return false;
    }

    return ZonedDateTime.now().plus(Duration.ofSeconds(10)).isAfter(expiration.toZonedDateTime());
  }
}
