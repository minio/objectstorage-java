/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2019 MinIO, Inc.
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
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementUnion;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Path;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.Text;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html">PutObjectLockConfiguration
 * API</a> and response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html">GetObjectLockConfiguration
 * API</a>.
 */
@Root(name = "ObjectLockConfiguration", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public class ObjectLockConfiguration {
  @Element(name = "ObjectLockEnabled")
  private String objectLockEnabled = "Enabled";

  @Element(name = "Rule", required = false)
  private Rule rule;

  public ObjectLockConfiguration() {}

  /** Constructs a new ObjectLockConfiguration object with given retention. */
  public ObjectLockConfiguration(RetentionMode mode, RetentionDuration duration)
      throws IllegalArgumentException {
    this.rule = new Rule(mode, duration);
  }

  /** Returns retention mode. */
  public RetentionMode mode() {
    return (rule != null) ? rule.mode() : null;
  }

  /** Returns retention duration. */
  public RetentionDuration duration() {
    return (rule != null) ? rule.duration() : null;
  }

  @Override
  public String toString() {
    return String.format(
        "ObjectLockConfiguration{objectLockEnabled=%s, rule=%s}",
        Utils.stringify(objectLockEnabled), Utils.stringify(rule));
  }

  @Root(name = "Rule", strict = false)
  public static class Rule {
    @Path(value = "DefaultRetention")
    @Element(name = "Mode", required = false)
    private RetentionMode mode;

    @Path(value = "DefaultRetention")
    @ElementUnion({
      @Element(name = "Days", type = RetentionDurationDays.class, required = false),
      @Element(name = "Years", type = RetentionDurationYears.class, required = false)
    })
    private RetentionDuration duration;

    public Rule(
        @Element(name = "Mode", required = false) RetentionMode mode,
        @ElementUnion({
              @Element(name = "Days", type = RetentionDurationDays.class, required = false),
              @Element(name = "Years", type = RetentionDurationYears.class, required = false)
            })
            RetentionDuration duration) {
      if (mode != null && duration != null) {
        this.mode = mode;
        this.duration = duration;
      } else if (mode != null || duration != null) {
        if (mode == null) {
          throw new IllegalArgumentException("mode is null");
        }
        throw new IllegalArgumentException("duration is null");
      }
    }

    public RetentionMode mode() {
      return mode;
    }

    public RetentionDuration duration() {
      return duration;
    }

    @Override
    public String toString() {
      return String.format(
          "Rule{mode=%s, duration=%s}", Utils.stringify(mode), Utils.stringify(duration));
    }
  }

  public static interface RetentionDuration {
    public RetentionDurationUnit unit();

    public int duration();
  }

  public static enum RetentionDurationUnit {
    DAYS,
    YEARS;
  }

  @Root(name = "Days")
  public static class RetentionDurationDays implements RetentionDuration {
    @Text(required = false)
    private Integer days;

    public RetentionDurationDays() {}

    public RetentionDurationDays(int days) {
      this.days = Integer.valueOf(days);
    }

    public RetentionDurationUnit unit() {
      return RetentionDurationUnit.DAYS;
    }

    public int duration() {
      return days;
    }

    @Override
    public String toString() {
      return String.format("RetentionDurationDays{%s}", Utils.stringify(days));
    }
  }

  @Root(name = "Years")
  public static class RetentionDurationYears implements RetentionDuration {
    @Text(required = false)
    private Integer years;

    public RetentionDurationYears() {}

    public RetentionDurationYears(int years) {
      this.years = Integer.valueOf(years);
    }

    public RetentionDurationUnit unit() {
      return RetentionDurationUnit.YEARS;
    }

    public int duration() {
      return years;
    }

    @Override
    public String toString() {
      return String.format("RetentionDurationYears{%s}", Utils.stringify(years));
    }
  }
}
