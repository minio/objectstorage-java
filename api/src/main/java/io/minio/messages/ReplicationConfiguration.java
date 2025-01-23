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
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.convert.Convert;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html">PutBucketReplication
 * API</a> and response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketReplication.html">GetBucketReplication
 * API</a>.
 */
@Root(name = "ReplicationConfiguration")
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ReplicationConfiguration {
  @Element(name = "Role", required = false)
  private String role;

  @ElementList(name = "Rule", inline = true)
  private List<Rule> rules;

  public ReplicationConfiguration(
      @Nullable @Element(name = "Role", required = false) String role,
      @Nonnull @ElementList(name = "Rule", inline = true) List<Rule> rules) {
    this.role = role; // Role is not applicable in MinIO server and it is optional.

    this.rules = Utils.unmodifiableList(Objects.requireNonNull(rules, "Rules must not be null"));
    if (rules.isEmpty()) {
      throw new IllegalArgumentException("Rules must not be empty");
    }
    if (rules.size() > 1000) {
      throw new IllegalArgumentException("More than 1000 rules are not supported");
    }
  }

  public String role() {
    return role;
  }

  public List<Rule> rules() {
    return Utils.unmodifiableList(rules);
  }

  @Override
  public String toString() {
    return String.format(
        "ReplicationConfiguration{role=%s, rules=%s}",
        Utils.stringify(role), Utils.stringify(rules));
  }

  @Root(name = "Rule")
  public static class Rule {
    @Element(name = "DeleteMarkerReplication", required = false)
    private DeleteMarkerReplication deleteMarkerReplication;

    @Element(name = "Destination")
    private Destination destination;

    @Element(name = "ExistingObjectReplication", required = false)
    private ExistingObjectReplication existingObjectReplication;

    @Element(name = "Filter", required = false)
    private Filter filter;

    @Element(name = "ID", required = false)
    private String id;

    @Element(name = "Prefix", required = false)
    @Convert(PrefixConverter.class)
    private String prefix;

    @Element(name = "Priority", required = false)
    private Integer priority;

    @Element(name = "SourceSelectionCriteria", required = false)
    private SourceSelectionCriteria sourceSelectionCriteria;

    @Element(name = "DeleteReplication", required = false)
    private DeleteReplication deleteReplication; // This is MinIO specific extension.

    @Element(name = "Status")
    private Status status;

    public Rule(
        @Nullable @Element(name = "DeleteMarkerReplication", required = false)
            DeleteMarkerReplication deleteMarkerReplication,
        @Nonnull @Element(name = "Destination") Destination destination,
        @Nullable @Element(name = "ExistingObjectReplication", required = false)
            ExistingObjectReplication existingObjectReplication,
        @Nullable @Element(name = "Filter", required = false) Filter filter,
        @Nullable @Element(name = "ID", required = false) String id,
        @Nullable @Element(name = "Prefix", required = false) String prefix,
        @Nullable @Element(name = "Priority", required = false) Integer priority,
        @Nullable @Element(name = "SourceSelectionCriteria", required = false)
            SourceSelectionCriteria sourceSelectionCriteria,
        @Nullable @Element(name = "DeleteReplication", required = false)
            DeleteReplication deleteReplication,
        @Nonnull @Element(name = "Status") Status status) {

      if (filter != null && deleteMarkerReplication == null) {
        deleteMarkerReplication = new DeleteMarkerReplication(null);
      }

      if (id != null) {
        id = id.trim();
        if (id.isEmpty()) throw new IllegalArgumentException("ID must be non-empty string");
        if (id.length() > 255)
          throw new IllegalArgumentException("ID must be exceed 255 characters");
      }

      this.deleteMarkerReplication = deleteMarkerReplication;
      this.destination = Objects.requireNonNull(destination, "Destination must not be null");
      this.existingObjectReplication = existingObjectReplication;
      this.filter = filter;
      this.id = id;
      this.prefix = prefix;
      this.priority = priority;
      this.sourceSelectionCriteria = sourceSelectionCriteria;
      this.deleteReplication = deleteReplication;
      this.status = Objects.requireNonNull(status, "Status must not be null");
    }

    public Rule(
        @Nullable @Element(name = "DeleteMarkerReplication", required = false)
            DeleteMarkerReplication deleteMarkerReplication,
        @Nonnull @Element(name = "Destination") Destination destination,
        @Nullable @Element(name = "ExistingObjectReplication", required = false)
            ExistingObjectReplication existingObjectReplication,
        @Nullable @Element(name = "Filter", required = false) Filter filter,
        @Nullable @Element(name = "ID", required = false) String id,
        @Nullable @Element(name = "Prefix", required = false) String prefix,
        @Nullable @Element(name = "Priority", required = false) Integer priority,
        @Nullable @Element(name = "SourceSelectionCriteria", required = false)
            SourceSelectionCriteria sourceSelectionCriteria,
        @Nonnull @Element(name = "Status") Status status) {
      this(
          deleteMarkerReplication,
          destination,
          existingObjectReplication,
          filter,
          id,
          prefix,
          priority,
          sourceSelectionCriteria,
          null,
          status);
    }

    public DeleteMarkerReplication deleteMarkerReplication() {
      return this.deleteMarkerReplication;
    }

    public Destination destination() {
      return this.destination;
    }

    public ExistingObjectReplication existingObjectReplication() {
      return this.existingObjectReplication;
    }

    public Filter filter() {
      return this.filter;
    }

    public String id() {
      return this.id;
    }

    public String prefix() {
      return this.prefix;
    }

    public Integer priority() {
      return this.priority;
    }

    public SourceSelectionCriteria sourceSelectionCriteria() {
      return this.sourceSelectionCriteria;
    }

    public DeleteReplication deleteReplication() {
      return this.deleteReplication;
    }

    public Status status() {
      return this.status;
    }

    @Override
    public String toString() {
      return String.format(
          "Rule{deleteMarkerReplication=%s, destination=%s, existingObjectReplication=%s,"
              + " filter=%s, id=%s, prefix=%s, priority=%s, sourceSelectionCriteria=%s,"
              + " deleteReplication=%s, status=%s}",
          Utils.stringify(deleteMarkerReplication),
          Utils.stringify(destination),
          Utils.stringify(existingObjectReplication),
          Utils.stringify(filter),
          Utils.stringify(id),
          Utils.stringify(prefix),
          Utils.stringify(priority),
          Utils.stringify(sourceSelectionCriteria),
          Utils.stringify(deleteReplication),
          Utils.stringify(status));
    }
  }

  @Root(name = "DeleteMarkerReplication")
  public static class DeleteMarkerReplication {
    @Element(name = "Status", required = false)
    private Status status;

    public DeleteMarkerReplication(
        @Nullable @Element(name = "Status", required = false) Status status) {
      this.status = (status == null) ? Status.DISABLED : status;
    }

    public Status status() {
      return status;
    }

    @Override
    public String toString() {
      return String.format("DeleteMarkerReplication{status=%s}", Utils.stringify(status));
    }
  }

  @Root(name = "Destination")
  public static class Destination {
    @Element(name = "AccessControlTranslation", required = false)
    private AccessControlTranslation accessControlTranslation;

    @Element(name = "Account", required = false)
    private String account;

    @Element(name = "Bucket")
    private String bucketArn;

    @Element(name = "EncryptionConfiguration", required = false)
    private EncryptionConfiguration encryptionConfiguration;

    @Element(name = "Metrics", required = false)
    private Metrics metrics;

    @Element(name = "ReplicationTime", required = false)
    private ReplicationTime replicationTime;

    @Element(name = "StorageClass", required = false)
    private String storageClass;

    public Destination(
        @Nullable @Element(name = "AccessControlTranslation", required = false)
            AccessControlTranslation accessControlTranslation,
        @Nullable @Element(name = "Account", required = false) String account,
        @Nonnull @Element(name = "Bucket") String bucketArn,
        @Nullable @Element(name = "EncryptionConfiguration", required = false)
            EncryptionConfiguration encryptionConfiguration,
        @Nullable @Element(name = "Metrics", required = false) Metrics metrics,
        @Nullable @Element(name = "ReplicationTime", required = false)
            ReplicationTime replicationTime,
        @Nullable @Element(name = "StorageClass", required = false) String storageClass) {
      this.accessControlTranslation = accessControlTranslation;
      this.account = account;
      this.bucketArn = Objects.requireNonNull(bucketArn, "Bucket ARN must not be null");
      this.encryptionConfiguration = encryptionConfiguration;
      this.metrics = metrics;
      this.replicationTime = replicationTime;
      this.storageClass = storageClass;
    }

    public AccessControlTranslation accessControlTranslation() {
      return this.accessControlTranslation;
    }

    public String account() {
      return this.account;
    }

    public String bucketArn() {
      return this.bucketArn;
    }

    public EncryptionConfiguration encryptionConfiguration() {
      return encryptionConfiguration;
    }

    public Metrics metrics() {
      return this.metrics;
    }

    public ReplicationTime replicationTime() {
      return this.replicationTime;
    }

    public String storageClass() {
      return this.storageClass;
    }

    @Override
    public String toString() {
      return String.format(
          "Destination{accessControlTranslation=%s, account=%s, bucketArn=%s,"
              + " encryptionConfiguration=%s, metrics=%s, replicationTime=%s, storageClass=%s}",
          Utils.stringify(accessControlTranslation),
          Utils.stringify(account),
          Utils.stringify(bucketArn),
          Utils.stringify(encryptionConfiguration),
          Utils.stringify(metrics),
          Utils.stringify(replicationTime),
          Utils.stringify(storageClass));
    }
  }

  @Root(name = "AccessControlTranslation")
  public static class AccessControlTranslation {
    @Element(name = "Owner")
    private String owner = "Destination";

    public AccessControlTranslation(@Nonnull @Element(name = "Owner") String owner) {
      this.owner = Objects.requireNonNull(owner, "Owner must not be null");
    }

    public String owner() {
      return this.owner;
    }

    @Override
    public String toString() {
      return String.format("AccessControlTranslation{owner=%s}", Utils.stringify(owner));
    }
  }

  @Root(name = "EncryptionConfiguration")
  public static class EncryptionConfiguration {
    @Element(name = "ReplicaKmsKeyID", required = false)
    private String replicaKmsKeyID;

    public EncryptionConfiguration(
        @Nullable @Element(name = "ReplicaKmsKeyID", required = false) String replicaKmsKeyID) {
      this.replicaKmsKeyID = replicaKmsKeyID;
    }

    public String replicaKmsKeyID() {
      return this.replicaKmsKeyID;
    }

    @Override
    public String toString() {
      return String.format(
          "EncryptionConfiguration{replicaKmsKeyID=%s}", Utils.stringify(replicaKmsKeyID));
    }
  }

  @Root(name = "Metrics")
  public static class Metrics {
    @Element(name = "EventThreshold")
    private ReplicationTimeValue eventThreshold;

    @Element(name = "Status")
    private Status status;

    public Metrics(
        @Nonnull @Element(name = "EventThreshold") ReplicationTimeValue eventThreshold,
        @Nonnull @Element(name = "Status") Status status) {
      this.eventThreshold =
          Objects.requireNonNull(eventThreshold, "Event threshold must not be null");
      this.status = Objects.requireNonNull(status, "Status must not be null");
    }

    public ReplicationTimeValue eventThreshold() {
      return this.eventThreshold;
    }

    public Status status() {
      return this.status;
    }

    @Override
    public String toString() {
      return String.format(
          "Metrics{eventThreshold=%s, status=%s}",
          Utils.stringify(eventThreshold), Utils.stringify(status));
    }
  }

  @Root(name = "ReplicationTime")
  public static class ReplicationTime {
    @Element(name = "Time")
    private ReplicationTimeValue time;

    @Element(name = "Status")
    private Status status;

    public ReplicationTime(
        @Nonnull @Element(name = "Time") ReplicationTimeValue time,
        @Nonnull @Element(name = "Status") Status status) {
      this.time = Objects.requireNonNull(time, "Time must not be null");
      this.status = Objects.requireNonNull(status, "Status must not be null");
    }

    public ReplicationTimeValue time() {
      return this.time;
    }

    public Status status() {
      return this.status;
    }

    @Override
    public String toString() {
      return String.format(
          "ReplicationTime{time=%s, status=%s}", Utils.stringify(time), Utils.stringify(status));
    }
  }

  @Root(name = "ReplicationTimeValue")
  public static class ReplicationTimeValue {
    @Element(name = "Minutes", required = false)
    private Integer minutes = 15;

    public ReplicationTimeValue(
        @Nullable @Element(name = "Minutes", required = false) Integer minutes) {
      this.minutes = minutes;
    }

    public Integer minutes() {
      return this.minutes;
    }

    @Override
    public String toString() {
      return String.format("ReplicationTimeValue{minutes=%s}", Utils.stringify(minutes));
    }
  }

  @Root(name = "ExistingObjectReplication")
  public static class ExistingObjectReplication {
    @Element(name = "Status")
    private Status status;

    public ExistingObjectReplication(@Nonnull @Element(name = "Status") Status status) {
      this.status = Objects.requireNonNull(status, "Status must not be null");
    }

    public Status status() {
      return this.status;
    }

    @Override
    public String toString() {
      return String.format("ExistingObjectReplication{status=%s}", Utils.stringify(status));
    }
  }

  @Root(name = "SourceSelectionCriteria")
  public static class SourceSelectionCriteria {
    @Element(name = "ReplicaModifications", required = false)
    private ReplicaModifications replicaModifications;

    @Element(name = "SseKmsEncryptedObjects", required = false)
    private SseKmsEncryptedObjects sseKmsEncryptedObjects;

    public SourceSelectionCriteria(
        @Nullable @Element(name = "SseKmsEncryptedObjects", required = false)
            SseKmsEncryptedObjects sseKmsEncryptedObjects,
        @Nullable @Element(name = "ReplicaModifications", required = false)
            ReplicaModifications replicaModifications) {
      this.sseKmsEncryptedObjects = sseKmsEncryptedObjects;
      this.replicaModifications = replicaModifications;
    }

    public SourceSelectionCriteria(@Nullable SseKmsEncryptedObjects sseKmsEncryptedObjects) {
      this(sseKmsEncryptedObjects, null);
    }

    public ReplicaModifications replicaModifications() {
      return this.replicaModifications;
    }

    public SseKmsEncryptedObjects sseKmsEncryptedObjects() {
      return this.sseKmsEncryptedObjects;
    }

    @Override
    public String toString() {
      return String.format(
          "SourceSelectionCriteria{replicaModifications=%s, sseKmsEncryptedObjects=%s}",
          Utils.stringify(replicaModifications), Utils.stringify(sseKmsEncryptedObjects));
    }
  }

  @Root(name = "ReplicaModifications")
  public static class ReplicaModifications {
    @Element(name = "Status")
    private Status status;

    public ReplicaModifications(@Nonnull @Element(name = "Status") Status status) {
      this.status = Objects.requireNonNull(status, "Status must not be null");
    }

    public Status status() {
      return this.status;
    }

    @Override
    public String toString() {
      return String.format("ReplicaModifications{status=%s}", Utils.stringify(status));
    }
  }

  @Root(name = "SseKmsEncryptedObjects")
  public static class SseKmsEncryptedObjects {
    @Element(name = "Status")
    private Status status;

    public SseKmsEncryptedObjects(@Nonnull @Element(name = "Status") Status status) {
      this.status = Objects.requireNonNull(status, "Status must not be null");
    }

    public Status status() {
      return this.status;
    }

    @Override
    public String toString() {
      return String.format("SseKmsEncryptedObjects{status=%s}", Utils.stringify(status));
    }
  }

  /** This is MinIO specific extension. */
  @Root(name = "DeleteReplication")
  public static class DeleteReplication {
    @Element(name = "Status", required = false)
    private Status status;

    public DeleteReplication(@Nullable @Element(name = "Status", required = false) Status status) {
      this.status = (status == null) ? Status.DISABLED : status;
    }

    public Status status() {
      return status;
    }

    @Override
    public String toString() {
      return String.format("DeleteReplication{status=%s}", Utils.stringify(status));
    }
  }
}
