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
import org.simpleframework.xml.Root;

/**
 * Object representation of request XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html">PutBucketVersioning
 * API</a> and response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html">GetBucketVersioning
 * API</a>.
 */
@Root(name = "VersioningConfiguration", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class VersioningConfiguration {
  @Element(name = "Status", required = false)
  private String status;

  @Element(name = "MfaDelete", required = false)
  private String mfaDelete;

  public VersioningConfiguration() {}

  /** Constructs a new VersioningConfiguration object with given status. */
  public VersioningConfiguration(@Nonnull Status status, @Nullable Boolean mfaDelete) {
    Objects.requireNonNull(status, "Status must not be null");
    if (status == Status.OFF) {
      throw new IllegalArgumentException("Status must be ENABLED or SUSPENDED");
    }
    this.status = status.toString();

    if (mfaDelete != null) {
      this.mfaDelete = mfaDelete ? "Enabled" : "Disabled";
    }
  }

  public Status status() {
    return Status.fromString(status);
  }

  public Boolean isMfaDeleteEnabled() {
    Boolean flag = (mfaDelete != null) ? Boolean.valueOf("Enabled".equals(mfaDelete)) : null;
    return flag;
  }

  @Override
  public String toString() {
    return String.format(
        "VersioningConfiguration{status=%s, mfaDelete=%s}",
        Utils.stringify(status), Utils.stringify(mfaDelete));
  }

  public static enum Status {
    OFF(""),
    ENABLED("Enabled"),
    SUSPENDED("Suspended");

    private final String value;

    private Status(String value) {
      this.value = value;
    }

    public String toString() {
      return this.value;
    }

    public static Status fromString(String statusString) {
      if ("Enabled".equals(statusString)) {
        return ENABLED;
      }

      if ("Suspended".equals(statusString)) {
        return SUSPENDED;
      }

      return OFF;
    }
  }
}
