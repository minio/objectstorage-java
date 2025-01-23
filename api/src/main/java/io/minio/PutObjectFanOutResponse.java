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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.ZonedDateTime;
import java.util.List;
import okhttp3.Headers;

/**
 * Response class of {@link MinioAsyncClient#putObjectFanOut} and {@link
 * MinioClient#putObjectFanOut}.
 */
public class PutObjectFanOutResponse extends GenericUploadResponse {
  private List<Result> results;

  public PutObjectFanOutResponse(
      Headers headers, String bucket, String region, List<Result> results) {
    super(headers, bucket, region, null, null);
    this.results = results;
  }

  public List<Result> results() {
    return results;
  }

  public static class Result {
    @JsonProperty("key")
    private String key;

    @JsonProperty("etag")
    private String etag;

    @JsonProperty("versionId")
    private String versionId;

    @JsonProperty("lastModified")
    private Time.S3Time lastModified;

    @JsonProperty("error")
    private String error;

    public Result() {}

    public String key() {
      return key;
    }

    public String etag() {
      return etag;
    }

    public String versionId() {
      return versionId;
    }

    public ZonedDateTime lastModified() {
      return lastModified == null ? null : lastModified.toZonedDateTime();
    }

    public String error() {
      return error;
    }
  }
}
