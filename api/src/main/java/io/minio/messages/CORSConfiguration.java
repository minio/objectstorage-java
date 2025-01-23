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

package io.minio.messages;

import io.minio.Utils;
import java.util.List;
import javax.annotation.Nullable;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of request/response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html">PutBucketCors
 * API</a> and <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html">GetBucketCors
 * API</a>.
 */
@Root(name = "CORSConfiguration", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class CORSConfiguration {
  @ElementList(name = "CORSRule", inline = true, required = false)
  private List<CORSRule> rules;

  public CORSConfiguration(
      @Nullable @ElementList(name = "CORSRule", required = false) List<CORSRule> rules) {
    this.rules = rules;
  }

  public List<CORSRule> rules() {
    return Utils.unmodifiableList(rules);
  }

  @Override
  public String toString() {
    return String.format("CORSConfiguration{rules=%s}", Utils.stringify(rules));
  }

  public static class CORSRule {
    @ElementList(entry = "AllowedHeader", inline = true, required = false)
    private List<String> allowedHeaders;

    @ElementList(entry = "AllowedMethod", inline = true, required = false)
    private List<String> allowedMethods;

    @ElementList(entry = "AllowedOrigin", inline = true, required = false)
    private List<String> allowedOrigins;

    @ElementList(entry = "ExposeHeader", inline = true, required = false)
    private List<String> exposeHeaders;

    @Element(name = "ID", required = false)
    private String id;

    @Element(name = "MaxAgeSeconds", required = false)
    private Integer maxAgeSeconds;

    public CORSRule(
        @Nullable @ElementList(entry = "AllowedHeader", inline = true, required = false)
            List<String> allowedHeaders,
        @Nullable @ElementList(entry = "AllowedMethod", inline = true, required = false)
            List<String> allowedMethods,
        @Nullable @ElementList(entry = "AllowedOrigin", inline = true, required = false)
            List<String> allowedOrigins,
        @Nullable @ElementList(entry = "ExposeHeader", inline = true, required = false)
            List<String> exposeHeaders,
        @Nullable @Element(name = "ID", required = false) String id,
        @Nullable @Element(name = "MaxAgeSeconds", required = false) Integer maxAgeSeconds) {
      this.allowedHeaders = allowedHeaders;
      this.allowedMethods = allowedMethods;
      this.allowedOrigins = allowedOrigins;
      this.exposeHeaders = exposeHeaders;
      this.id = id;
      this.maxAgeSeconds = maxAgeSeconds;
    }

    public List<String> allowedHeaders() {
      return Utils.unmodifiableList(allowedHeaders);
    }

    public List<String> allowedMethods() {
      return Utils.unmodifiableList(allowedMethods);
    }

    public List<String> allowedOrigins() {
      return Utils.unmodifiableList(allowedOrigins);
    }

    public List<String> exposeHeaders() {
      return Utils.unmodifiableList(exposeHeaders);
    }

    public String id() {
      return id;
    }

    public Integer maxAgeSeconds() {
      return maxAgeSeconds;
    }

    @Override
    public String toString() {
      return String.format(
          "CORSRule{allowedHeaders=%s, allowedMethods=%s, allowedOrigins=%s, exposeHeaders=%s, "
              + "id=%s, maxAgeSeconds=%s}",
          Utils.stringify(allowedHeaders),
          Utils.stringify(allowedMethods),
          Utils.stringify(allowedOrigins),
          Utils.stringify(exposeHeaders),
          Utils.stringify(id),
          Utils.stringify(maxAgeSeconds));
    }
  }
}
