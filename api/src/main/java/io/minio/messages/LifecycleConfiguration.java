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

import io.minio.Time;
import io.minio.Utils;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html">PutBucketLifecycleConfiguration
 * API</a> and response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html">GetBucketLifecycleConfiguration
 * API</a>.
 */
@Root(name = "LifecycleConfiguration")
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class LifecycleConfiguration {
  @ElementList(name = "Rule", inline = true)
  private List<Rule> rules;

  /** Constructs new lifecycle configuration. */
  public LifecycleConfiguration(
      @Nonnull @ElementList(name = "Rule", inline = true) List<Rule> rules) {
    if (Objects.requireNonNull(rules, "Rules must not be null").isEmpty()) {
      throw new IllegalArgumentException("Rules must not be empty");
    }
    this.rules = Utils.unmodifiableList(rules);
  }

  public List<Rule> rules() {
    return rules;
  }

  @Override
  public String toString() {
    return String.format("LifecycleConfiguration{rules=%s}", Utils.stringify(rules));
  }

  @Root(name = "Rule")
  public static class Rule {
    @Element(name = "AbortIncompleteMultipartUpload", required = false)
    private AbortIncompleteMultipartUpload abortIncompleteMultipartUpload;

    @Element(name = "Expiration", required = false)
    private Expiration expiration;

    @Element(name = "Filter", required = false)
    private Filter filter;

    @Element(name = "ID", required = false)
    private String id;

    @Element(name = "NoncurrentVersionExpiration", required = false)
    private NoncurrentVersionExpiration noncurrentVersionExpiration;

    @Element(name = "NoncurrentVersionTransition", required = false)
    private NoncurrentVersionTransition noncurrentVersionTransition;

    @Element(name = "Status")
    private Status status;

    @Element(name = "Transition", required = false)
    private Transition transition;

    /** Constructs new server-side encryption configuration rule. */
    public Rule(
        @Nonnull @Element(name = "Status") Status status,
        @Nullable @Element(name = "AbortIncompleteMultipartUpload", required = false)
            AbortIncompleteMultipartUpload abortIncompleteMultipartUpload,
        @Nullable @Element(name = "Expiration", required = false) Expiration expiration,
        @Nonnull @Element(name = "Filter", required = false) Filter filter,
        @Nullable @Element(name = "ID", required = false) String id,
        @Nullable @Element(name = "NoncurrentVersionExpiration", required = false)
            NoncurrentVersionExpiration noncurrentVersionExpiration,
        @Nullable @Element(name = "NoncurrentVersionTransition", required = false)
            NoncurrentVersionTransition noncurrentVersionTransition,
        @Nullable @Element(name = "Transition", required = false) Transition transition) {
      if (abortIncompleteMultipartUpload == null
          && expiration == null
          && noncurrentVersionExpiration == null
          && noncurrentVersionTransition == null
          && transition == null) {
        throw new IllegalArgumentException(
            "At least one of action (AbortIncompleteMultipartUpload, Expiration, "
                + "NoncurrentVersionExpiration, NoncurrentVersionTransition or Transition) must be "
                + "specified in a rule");
      }

      if (id != null) {
        id = id.trim();
        if (id.isEmpty()) throw new IllegalArgumentException("ID must be non-empty string");
        if (id.length() > 255)
          throw new IllegalArgumentException("ID must not exceed 255 characters");
      }

      this.abortIncompleteMultipartUpload = abortIncompleteMultipartUpload;
      this.expiration = expiration;
      this.filter = Objects.requireNonNull(filter, "Filter must not be null");
      this.id = id;
      this.noncurrentVersionExpiration = noncurrentVersionExpiration;
      this.noncurrentVersionTransition = noncurrentVersionTransition;
      this.status = Objects.requireNonNull(status, "Status must not be null");
      this.transition = transition;
    }

    public AbortIncompleteMultipartUpload abortIncompleteMultipartUpload() {
      return abortIncompleteMultipartUpload;
    }

    public Expiration expiration() {
      return expiration;
    }

    public Filter filter() {
      return this.filter;
    }

    public String id() {
      return this.id;
    }

    public NoncurrentVersionExpiration noncurrentVersionExpiration() {
      return noncurrentVersionExpiration;
    }

    public NoncurrentVersionTransition noncurrentVersionTransition() {
      return noncurrentVersionTransition;
    }

    public Status status() {
      return this.status;
    }

    public Transition transition() {
      return transition;
    }

    @Override
    public String toString() {
      return String.format(
          "Rule{abortIncompleteMultipartUpload=%s, expiration=%s, filter=%s, id=%s,"
              + " noncurrentVersionExpiration=%s, noncurrentVersionTransition=%s, status=%s,"
              + " transition=%s}",
          Utils.stringify(abortIncompleteMultipartUpload),
          Utils.stringify(expiration),
          Utils.stringify(filter),
          Utils.stringify(id),
          Utils.stringify(noncurrentVersionExpiration),
          Utils.stringify(noncurrentVersionTransition),
          Utils.stringify(status),
          Utils.stringify(transition));
    }
  }

  @Root(name = "AbortIncompleteMultipartUpload")
  public static class AbortIncompleteMultipartUpload {
    @Element(name = "DaysAfterInitiation")
    private int daysAfterInitiation;

    public AbortIncompleteMultipartUpload(
        @Element(name = "DaysAfterInitiation") int daysAfterInitiation) {
      this.daysAfterInitiation = daysAfterInitiation;
    }

    public int daysAfterInitiation() {
      return daysAfterInitiation;
    }

    @Override
    public String toString() {
      return String.format(
          "AbortIncompleteMultipartUpload{daysAfterInitiation=%d}", daysAfterInitiation);
    }
  }

  public abstract static class DateDays {
    @Element(name = "Date", required = false)
    protected Time.S3Time date;

    @Element(name = "Days", required = false)
    protected Integer days;

    public ZonedDateTime date() {
      return date == null ? null : date.toZonedDateTime();
    }

    public Integer days() {
      return days;
    }

    @Override
    public String toString() {
      return String.format("date=%s, days=%s", Utils.stringify(date), Utils.stringify(days));
    }
  }

  @Root(name = "Expiration")
  public static class Expiration extends DateDays {
    @Element(name = "ExpiredObjectDeleteMarker", required = false)
    private Boolean expiredObjectDeleteMarker;

    @Element(name = "ExpiredObjectAllVersions", required = false)
    private Boolean expiredObjectAllVersions; // This is MinIO specific extension.

    public Expiration(
        @Nullable @Element(name = "Date", required = false) Time.S3Time date,
        @Nullable @Element(name = "Days", required = false) Integer days,
        @Nullable @Element(name = "ExpiredObjectDeleteMarker", required = false)
            Boolean expiredObjectDeleteMarker) {
      if (expiredObjectDeleteMarker != null) {
        if (date != null || days != null) {
          throw new IllegalArgumentException(
              "ExpiredObjectDeleteMarker must not be provided along with Date and Days");
        }
      } else if (date != null ^ days != null) {
        this.date = date;
        this.days = days;
      } else {
        throw new IllegalArgumentException("Only one of date or days must be set");
      }

      this.expiredObjectDeleteMarker = expiredObjectDeleteMarker;
    }

    public Expiration(ZonedDateTime date, Integer days, Boolean expiredObjectDeleteMarker) {
      this(date == null ? null : new Time.S3Time(date), days, expiredObjectDeleteMarker);
    }

    public Expiration(
        @Nullable @Element(name = "Date", required = false) Time.S3Time date,
        @Nullable @Element(name = "Days", required = false) Integer days,
        @Nullable @Element(name = "ExpiredObjectDeleteMarker", required = false)
            Boolean expiredObjectDeleteMarker,
        @Element(name = "ExpiredObjectAllVersions", required = false)
            Boolean expiredObjectAllVersions) {
      this(date, days, expiredObjectDeleteMarker);
      this.expiredObjectAllVersions = expiredObjectAllVersions;
    }

    public Boolean expiredObjectDeleteMarker() {
      return expiredObjectDeleteMarker;
    }

    public Boolean expiredObjectAllVersions() {
      return expiredObjectAllVersions;
    }

    @Override
    public String toString() {
      return String.format(
          "Expiration{%s, expiredObjectDeleteMarker=%s, expiredObjectAllVersions=%s}",
          super.toString(),
          Utils.stringify(expiredObjectDeleteMarker),
          Utils.stringify(expiredObjectAllVersions));
    }
  }

  @Root(name = "NoncurrentVersionExpiration")
  public static class NoncurrentVersionExpiration {
    @Element(name = "NoncurrentDays")
    private int noncurrentDays;

    @Element(name = "NewerNoncurrentVersions", required = false)
    private Integer newerNoncurrentVersions;

    public NoncurrentVersionExpiration(
        @Element(name = "NoncurrentDays", required = false) int noncurrentDays) {
      this.noncurrentDays = noncurrentDays;
    }

    public NoncurrentVersionExpiration(
        @Element(name = "NoncurrentDays", required = false) int noncurrentDays,
        @Element(name = "NewerNoncurrentVersions", required = false)
            Integer newerNoncurrentVersions) {
      this.noncurrentDays = noncurrentDays;
      this.newerNoncurrentVersions = newerNoncurrentVersions;
    }

    public int noncurrentDays() {
      return noncurrentDays;
    }

    public Integer newerNoncurrentVersions() {
      return newerNoncurrentVersions;
    }

    protected String stringify() {
      return String.format(
          "noncurrentDays=%d, newerNoncurrentVersions=%s",
          noncurrentDays, Utils.stringify(newerNoncurrentVersions));
    }

    @Override
    public String toString() {
      return String.format("NoncurrentVersionExpiration{%s}", stringify());
    }
  }

  @Root(name = "NoncurrentVersionTransition")
  public static class NoncurrentVersionTransition extends NoncurrentVersionExpiration {
    @Element(name = "StorageClass")
    private String storageClass;

    public NoncurrentVersionTransition(
        @Element(name = "NoncurrentDays", required = false) int noncurrentDays,
        @Nonnull @Element(name = "StorageClass", required = false) String storageClass) {
      super(noncurrentDays, null);
      if (storageClass == null || storageClass.isEmpty()) {
        throw new IllegalArgumentException("StorageClass must be provided");
      }
      this.storageClass = storageClass;
    }

    public NoncurrentVersionTransition(
        @Element(name = "NoncurrentDays", required = false) int noncurrentDays,
        @Element(name = "NewerNoncurrentVersions", required = false)
            Integer newerNoncurrentVersions,
        @Nonnull @Element(name = "StorageClass", required = false) String storageClass) {
      super(noncurrentDays, newerNoncurrentVersions);
      if (storageClass == null || storageClass.isEmpty()) {
        throw new IllegalArgumentException("StorageClass must be provided");
      }
      this.storageClass = storageClass;
    }

    public String storageClass() {
      return storageClass;
    }

    @Override
    public String toString() {
      return String.format(
          "NoncurrentVersionTransition{%s, storageClass=%s}", super.stringify(), storageClass);
    }
  }

  @Root(name = "Transition")
  public static class Transition extends DateDays {
    @Element(name = "StorageClass")
    private String storageClass;

    public Transition(
        @Nullable @Element(name = "Date", required = false) Time.S3Time date,
        @Nullable @Element(name = "Days", required = false) Integer days,
        @Nonnull @Element(name = "StorageClass", required = false) String storageClass) {
      if (date != null ^ days != null) {
        this.date = date;
        this.days = days;
      } else {
        throw new IllegalArgumentException("Only one of date or days must be set");
      }
      if (storageClass == null || storageClass.isEmpty()) {
        throw new IllegalArgumentException("StorageClass must be provided");
      }
      this.storageClass = storageClass;
    }

    public Transition(ZonedDateTime date, Integer days, String storageClass) {
      this(date == null ? null : new Time.S3Time(date), days, storageClass);
    }

    public String storageClass() {
      return storageClass;
    }

    @Override
    public String toString() {
      return String.format("Transition{%s, storageClass=%s}", super.toString(), storageClass);
    }
  }
}
