/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2015 MinIO, Inc.
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

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html">CreateBucket
 * API</a>.
 */
@Root(name = "CreateBucketConfiguration")
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public class CreateBucketConfiguration {
  @Element(name = "LocationConstraint")
  private String locationConstraint;

  @Element(name = "Location", required = false)
  private Location location;

  @Element(name = "Bucket", required = false)
  private Bucket bucket;

  /** Constructs a new CreateBucketConfiguration object with given location constraint. */
  public CreateBucketConfiguration(String locationConstraint) {
    this.locationConstraint = locationConstraint;
  }

  public CreateBucketConfiguration(String locationConstraint, Location location, Bucket bucket) {
    this.locationConstraint = locationConstraint;
    this.location = location;
    this.bucket = bucket;
  }

  @Root(name = "Location", strict = false)
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class Location {
    @Element(name = "Name", required = false)
    private String name;

    @Element(name = "Type", required = false)
    private String type;

    public Location(String name, String type) {
      this.name = name;
      this.type = type;
    }
  }

  @Root(name = "Bucket", strict = false)
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class Bucket {
    @Element(name = "DataRedundancy", required = false)
    private String dataRedundancy;

    @Element(name = "Type", required = false)
    private String type;

    public Bucket(String dataRedundancy, String type) {
      this.dataRedundancy = dataRedundancy;
      this.type = type;
    }
  }
}
