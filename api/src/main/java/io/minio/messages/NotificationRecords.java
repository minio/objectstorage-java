/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2020 MinIO, Inc.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.minio.Time;
import io.minio.Utils;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * Object representation of JSON format of <a
 * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html">Notification
 * Content Structure</a>.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
    value = "UwF",
    justification = "Everything in this class is initialized by JSON unmarshalling.")
public class NotificationRecords {
  @JsonProperty("Records")
  private List<Event> events;

  public List<Event> events() {
    return Utils.unmodifiableList(events);
  }

  @Override
  public String toString() {
    return String.format("NotificationRecords{events=%s}", Utils.stringify(events));
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      value = "UuF",
      justification = "eventVersion and eventSource are available for completeness")
  public static class Event {
    @JsonProperty private String eventVersion;
    @JsonProperty private String eventSource;
    @JsonProperty private String awsRegion;
    @JsonProperty private String eventName;
    @JsonProperty private Identity userIdentity;
    @JsonProperty private Map<String, String> requestParameters;
    @JsonProperty private Map<String, String> responseElements;
    @JsonProperty private S3 s3;
    @JsonProperty private Source source;
    @JsonProperty private Time.S3Time eventTime;

    public String eventVersion() {
      return eventVersion;
    }

    public String eventSource() {
      return eventSource;
    }

    public String awsRegion() {
      return awsRegion;
    }

    public String eventName() {
      return eventName;
    }

    public String userIdentity() {
      return userIdentity == null ? null : userIdentity.principalId();
    }

    public Map<String, String> requestParameters() {
      return Utils.unmodifiableMap(requestParameters);
    }

    public Map<String, String> responseElements() {
      return Utils.unmodifiableMap(responseElements);
    }

    public Bucket bucket() {
      return s3 == null ? null : s3.bucket();
    }

    public Object object() {
      return s3 == null ? null : s3.object();
    }

    public Source source() {
      return source;
    }

    public ZonedDateTime eventTime() {
      return eventTime == null ? null : eventTime.toZonedDateTime();
    }

    @Override
    public String toString() {
      return String.format(
          "Event{eventVersion=%s, eventSource=%s, awsRegion=%s, eventName=%s, userIdentity=%s,"
              + " requestParameters=%s, responseElements=%s, s3=%s, source=%s, eventTime=%s}",
          Utils.stringify(eventVersion),
          Utils.stringify(eventSource),
          Utils.stringify(awsRegion),
          Utils.stringify(eventName),
          Utils.stringify(userIdentity),
          Utils.stringify(requestParameters),
          Utils.stringify(responseElements),
          Utils.stringify(s3),
          Utils.stringify(source),
          Utils.stringify(eventTime));
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "UwF",
        justification = "Everything in this class is initialized by JSON unmarshalling.")
    public static class Identity {
      @JsonProperty private String principalId;

      public String principalId() {
        return principalId;
      }

      @Override
      public String toString() {
        return String.format("Identity{principalId=%s}", Utils.stringify(principalId));
      }
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "UwF",
        justification = "Everything in this class is initialized by JSON unmarshalling.")
    public static class Bucket {
      @JsonProperty private String name;
      @JsonProperty private Identity ownerIdentity;
      @JsonProperty private String arn;

      public String name() {
        return name;
      }

      public String ownerIdentity() {
        return ownerIdentity == null ? null : ownerIdentity.principalId();
      }

      public String arn() {
        return arn;
      }

      @Override
      public String toString() {
        return String.format(
            "Bucket{name=%s, ownerIdentity=%s, arn=%s}",
            Utils.stringify(name), Utils.stringify(ownerIdentity), Utils.stringify(arn));
      }
    }

    public static class Object {
      @JsonProperty private String key;
      @JsonProperty private long size;
      @JsonProperty private String eTag;
      @JsonProperty private String versionId;
      @JsonProperty private String sequencer;
      @JsonProperty private Map<String, String> userMetadata; // MinIO specific extension.

      public String key() {
        return key;
      }

      public long size() {
        return size;
      }

      public String etag() {
        return eTag;
      }

      public String versionId() {
        return versionId;
      }

      public String sequencer() {
        return sequencer;
      }

      public Map<String, String> userMetadata() {
        return Utils.unmodifiableMap(userMetadata);
      }

      @Override
      public String toString() {
        return String.format(
            "Object{key=%s, size=%d, eTag=%s, versionId=%s, sequencer=%s, userMetadata=%s}",
            Utils.stringify(key),
            size,
            Utils.stringify(eTag),
            Utils.stringify(versionId),
            Utils.stringify(sequencer),
            Utils.stringify(userMetadata));
      }
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = {"UwF", "UuF"},
        justification =
            "Everything in this class is initialized by JSON unmarshalling "
                + "and s3SchemaVersion/configurationId are available for completeness.")
    public static class S3 {
      @JsonProperty private String s3SchemaVersion;
      @JsonProperty private String configurationId;
      @JsonProperty private Bucket bucket;
      @JsonProperty private Object object;

      public Bucket bucket() {
        return bucket;
      }

      public Object object() {
        return object;
      }

      @Override
      public String toString() {
        return String.format(
            "S3{s3SchemaVersion=%s, configurationId=%s, bucket=%s, object=%s}",
            Utils.stringify(s3SchemaVersion),
            Utils.stringify(configurationId),
            Utils.stringify(bucket),
            Utils.stringify(object));
      }
    }

    /** This is MinIO extension. */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "UwF",
        justification = "Everything in this class is initialized by JSON unmarshalling.")
    public static class Source {
      @JsonProperty private String host;
      @JsonProperty private String port;
      @JsonProperty private String userAgent;

      public String host() {
        return host;
      }

      public String port() {
        return port;
      }

      public String userAgent() {
        return userAgent;
      }

      @Override
      public String toString() {
        return String.format(
            "Source{host=%s, port=%s, userAgent=%s}",
            Utils.stringify(host), Utils.stringify(port), Utils.stringify(userAgent));
      }
    }
  }
}
