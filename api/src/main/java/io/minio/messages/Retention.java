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
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html">PutObjectRetention
 * API</a> and response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html">GetObjectRetention
 * API</a>.
 */
@Root(name = "Retention", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class Retention {
  @Element(name = "Mode", required = false)
  private RetentionMode mode;

  @Element(name = "RetainUntilDate", required = false)
  private Time.S3Time retainUntilDate;

  public Retention() {}

  /** Constructs a new Retention object with given retention until date and mode. */
  public Retention(RetentionMode mode, ZonedDateTime retainUntilDate) {
    if (mode == null) {
      throw new IllegalArgumentException("null mode is not allowed");
    }

    if (retainUntilDate == null) {
      throw new IllegalArgumentException("null retainUntilDate is not allowed");
    }

    this.mode = mode;
    this.retainUntilDate = new Time.S3Time(retainUntilDate);
  }

  /** Returns mode. */
  public RetentionMode mode() {
    return this.mode;
  }

  /** Returns retain until date. */
  public ZonedDateTime retainUntilDate() {
    return retainUntilDate == null ? null : retainUntilDate.toZonedDateTime();
  }

  @Override
  public String toString() {
    return String.format(
        "Retention{mode=%s, retainUntilDate=%s}",
        Utils.stringify(mode), Utils.stringify(retainUntilDate));
  }
}
