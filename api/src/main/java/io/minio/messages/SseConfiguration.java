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

package io.minio.messages;

import io.minio.Utils;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Path;
import org.simpleframework.xml.Root;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html">PutBucketEncryption
 * API</a> and response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html">GetBucketEncryption
 * API</a>.
 */
@Root(name = "ServerSideEncryptionConfiguration", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public class SseConfiguration {
  @Element(name = "Rule", required = false)
  private Rule rule;

  public SseConfiguration(@Nullable @Element(name = "Rule", required = false) Rule rule) {
    this.rule = rule;
  }

  public static SseConfiguration newConfigWithSseS3Rule() {
    return new SseConfiguration(new Rule(SseAlgorithm.AES256, null));
  }

  public static SseConfiguration newConfigWithSseKmsRule(@Nullable String kmsMasterKeyId) {
    return new SseConfiguration(new Rule(SseAlgorithm.AWS_KMS, kmsMasterKeyId));
  }

  public Rule rule() {
    return this.rule;
  }

  @Override
  public String toString() {
    return String.format("SseConfiguration{rule=%s}", Utils.stringify(rule));
  }

  @Root(name = "Rule", strict = false)
  @Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class Rule {
    @Path(value = "ApplyServerSideEncryptionByDefault")
    @Element(name = "KMSMasterKeyID", required = false)
    private String kmsMasterKeyId;

    @Path(value = "ApplyServerSideEncryptionByDefault")
    @Element(name = "SSEAlgorithm")
    private SseAlgorithm sseAlgorithm;

    /** Constructs new server-side encryption configuration rule. */
    public Rule(
        @Nonnull @Element(name = "SSEAlgorithm") SseAlgorithm sseAlgorithm,
        @Nullable @Element(name = "KMSMasterKeyID", required = false) String kmsMasterKeyId) {
      this.sseAlgorithm = Objects.requireNonNull(sseAlgorithm, "SSE Algorithm must be provided");
      this.kmsMasterKeyId = kmsMasterKeyId;
    }

    public String kmsMasterKeyId() {
      return this.kmsMasterKeyId;
    }

    public SseAlgorithm sseAlgorithm() {
      return this.sseAlgorithm;
    }

    @Override
    public String toString() {
      return String.format(
          "Rule{sseAlgorithm=%s, kmsMasterKeyId=%s}",
          Utils.stringify(sseAlgorithm), Utils.stringify(kmsMasterKeyId));
    }
  }
}
