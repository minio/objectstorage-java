/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2021 MinIO, Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import io.minio.Utils;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementMap;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.convert.Convert;
import org.simpleframework.xml.convert.Converter;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html">RestoreObject
 * API</a>.
 */
@Root(name = "RestoreRequest")
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public class RestoreRequest {
  @Element(name = "Days", required = false)
  private Integer days;

  @Element(name = "GlacierJobParameters", required = false)
  private GlacierJobParameters glacierJobParameters;

  @Element(name = "Type", required = false)
  private String type;

  @Element(name = "Tier", required = false)
  private Tier tier;

  @Element(name = "Description", required = false)
  private String description;

  @Element(name = "SelectParameters", required = false)
  private SelectParameters selectParameters;

  @Element(name = "OutputLocation", required = false)
  private OutputLocation outputLocation;

  /** Constructs new RestoreRequest object for given parameters. */
  public RestoreRequest(
      @Nullable Integer days,
      @Nullable GlacierJobParameters glacierJobParameters,
      @Nullable Tier tier,
      @Nullable String description,
      @Nullable SelectParameters selectParameters,
      @Nullable OutputLocation outputLocation) {
    this.days = days;
    this.glacierJobParameters = glacierJobParameters;
    if (selectParameters != null) this.type = "SELECT";
    this.tier = tier;
    this.description = description;
    this.selectParameters = selectParameters;
    this.outputLocation = outputLocation;
  }

  @Root(name = "Tier")
  @Convert(Tier.TierConverter.class)
  public static enum Tier {
    STANDARD("Standard"),
    BULK("Bulk"),
    EXPEDITED("Expedited");

    private final String value;

    private Tier(String value) {
      this.value = value;
    }

    public String toString() {
      return this.value;
    }

    /** Returns Tier of given string. */
    @JsonCreator
    public static Tier fromString(String tierString) {
      for (Tier tier : Tier.values()) {
        if (tierString.equals(tier.value)) {
          return tier;
        }
      }

      throw new IllegalArgumentException("Unknown tier '" + tierString + "'");
    }

    /** XML converter class. */
    public static class TierConverter implements Converter<Tier> {
      @Override
      public Tier read(InputNode node) throws Exception {
        return Tier.fromString(node.getValue());
      }

      @Override
      public void write(OutputNode node, Tier tier) throws Exception {
        node.setValue(tier.toString());
      }
    }
  }

  @Root(name = "GlacierJobParameters")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class GlacierJobParameters {
    @Element(name = "Tier")
    private Tier tier;

    public GlacierJobParameters(@Nonnull Tier tier) {
      this.tier = Objects.requireNonNull(tier, "Tier must not be null");
    }
  }

  @Root(name = "SelectParameters")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class SelectParameters extends BaseSelectParameters {
    public SelectParameters(
        @Nonnull String expression,
        @Nonnull InputSerialization is,
        @Nonnull OutputSerialization os) {
      super(expression, is, os);
    }
  }

  @Root(name = "OutputLocation")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class OutputLocation {
    @Element(name = "S3")
    private S3 s3;

    public OutputLocation(@Nonnull S3 s3) {
      this.s3 = Objects.requireNonNull(s3, "S3 must not be null");
    }
  }

  @Root(name = "S3")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class S3 {
    @Element(name = "AccessControlList", required = false)
    private AccessControlList accessControlList;

    @Element(name = "BucketName")
    private String bucketName;

    @Element(name = "CannedACL", required = false)
    private CannedAcl cannedAcl;

    @Element(name = "Encryption", required = false)
    private Encryption encryption;

    @Element(name = "Prefix")
    private String prefix;

    @Element(name = "StorageClass", required = false)
    private String storageClass;

    @Element(name = "Tagging", required = false)
    private Tags tagging;

    @Element(name = "UserMetadata", required = false)
    private UserMetadata userMetadata;

    public S3(
        @Nonnull String bucketName,
        @Nonnull String prefix,
        @Nullable AccessControlList accessControlList,
        @Nullable CannedAcl cannedAcl,
        @Nullable Encryption encryption,
        @Nullable String storageClass,
        @Nullable Tags tagging,
        @Nullable UserMetadata userMetadata) {
      this.bucketName = Objects.requireNonNull(bucketName, "Bucket name must not be null");
      this.prefix = Objects.requireNonNull(prefix, "Prefix must not be null");
      this.accessControlList = accessControlList;
      this.cannedAcl = cannedAcl;
      this.encryption = encryption;
      this.storageClass = storageClass;
      this.tagging = tagging;
      this.userMetadata = userMetadata;
    }
  }

  @Root(name = "CannedAcl")
  @Convert(CannedAcl.CannedAclConverter.class)
  public static enum CannedAcl {
    PRIVATE("private"),
    PUBLIC_READ("public-read"),
    PUBLIC_READ_WRITE("public-read-write"),
    AUTHENTICATED_READ("authenticated-read"),
    AWS_EXEC_READ("aws-exec-read"),
    BUCKET_OWNER_READ("bucket-owner-read"),
    BUCKET_OWNER_FULL_CONTROL("bucket-owner-full-control");

    private final String value;

    private CannedAcl(String value) {
      this.value = value;
    }

    public String toString() {
      return this.value;
    }

    /** Returns CannedAcl of given string. */
    @JsonCreator
    public static CannedAcl fromString(String cannedAclString) {
      for (CannedAcl cannedAcl : CannedAcl.values()) {
        if (cannedAclString.equals(cannedAcl.value)) {
          return cannedAcl;
        }
      }

      throw new IllegalArgumentException("Unknown canned ACL '" + cannedAclString + "'");
    }

    /** XML converter class. */
    public static class CannedAclConverter implements Converter<CannedAcl> {
      @Override
      public CannedAcl read(InputNode node) throws Exception {
        return CannedAcl.fromString(node.getValue());
      }

      @Override
      public void write(OutputNode node, CannedAcl cannedAcl) throws Exception {
        node.setValue(cannedAcl.toString());
      }
    }
  }

  @Root(name = "Encryption")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class Encryption {
    @Element(name = "EncryptionType")
    private SseAlgorithm encryptionType;

    @Element(name = "KMSContext", required = false)
    private String kmsContext;

    @Element(name = "KMSKeyId", required = false)
    private String kmsKeyId;

    public Encryption(
        @Nonnull SseAlgorithm encryptionType,
        @Nullable String kmsContext,
        @Nullable String kmsKeyId) {
      this.encryptionType =
          Objects.requireNonNull(encryptionType, "Encryption type must not be null");
      this.kmsContext = kmsContext;
      this.kmsKeyId = kmsKeyId;
    }
  }

  @Root(name = "UserMetadata", strict = false)
  @Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class UserMetadata {
    @ElementMap(
        attribute = false,
        entry = "MetadataEntry",
        inline = true,
        key = "Name",
        value = "Value",
        required = false)
    Map<String, String> metadataEntries;

    private UserMetadata(@Nonnull Map<String, String> metadataEntries) {
      Objects.requireNonNull(metadataEntries, "Metadata entries must not be null");
      if (metadataEntries.size() == 0) {
        throw new IllegalArgumentException("Metadata entries must not be empty");
      }
      this.metadataEntries = Utils.unmodifiableMap(metadataEntries);
    }
  }
}
