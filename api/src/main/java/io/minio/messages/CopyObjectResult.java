/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2017 MinIO, Inc.
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
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html">CopyObject API</a>.
 */
@Root(name = "CopyObjectResult", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class CopyObjectResult extends Checksum {
  @Element(name = "ETag")
  private String etag;

  @Element(name = "LastModified")
  private Time.S3Time lastModified;

  public CopyObjectResult() {}

  /** Returns ETag of the object. */
  public String etag() {
    return etag;
  }

  /** Returns last modified time. */
  public ZonedDateTime lastModified() {
    return lastModified == null ? null : lastModified.toZonedDateTime();
  }

  protected String stringify() {
    return String.format(
        "etag=%s, lastModified=%s, %s",
        Utils.stringify(etag), Utils.stringify(lastModified), super.stringify());
  }

  @Override
  public String toString() {
    return String.format("CopyObjectResult{%s}", stringify());
  }
}
