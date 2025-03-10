/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2025 MinIO, Inc.
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
import java.time.ZonedDateTime;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html">GetObjectAttributes
 * API</a>.
 */
@Root(name = "GetObjectAttributesOutput", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class GetObjectAttributesOutput {
  @Element(name = "ETag", required = false)
  private String etag;

  @Element(name = "Checksum", required = false)
  private Checksum checksum;

  @Element(name = "ObjectParts", required = false)
  private ObjectParts objectParts;

  @Element(name = "StorageClass", required = false)
  private String storageClass;

  @Element(name = "ObjectSize", required = false)
  private Long objectSize;

  private Boolean deleteMarker;
  private ZonedDateTime lastModified;
  private String versionId;

  public GetObjectAttributesOutput() {}

  public void setDeleteMarker(boolean deleteMarker) {
    this.deleteMarker = deleteMarker;
  }

  public void setLastModified(ZonedDateTime lastModified) {
    this.lastModified = lastModified;
  }

  public void setVersionId(String versionId) {
    this.versionId = versionId;
  }

  public Boolean deleteMarker() {
    return deleteMarker;
  }

  public ZonedDateTime lastModified() {
    return lastModified;
  }

  public String versionId() {
    return versionId;
  }

  public String etag() {
    return etag;
  }

  public Checksum checksum() {
    return checksum;
  }

  public ObjectParts objectParts() {
    return objectParts;
  }

  public String storageClass() {
    return storageClass;
  }

  public Long objectSize() {
    return objectSize;
  }

  @Override
  public String toString() {
    return String.format(
        "GetObjectAttributesOutput{etag=%s, checksum=%s, objectParts=%s, storageClass=%s,"
            + " objectSize=%s, deleteMarker=%s, lastModified=%s, versionId=%s}",
        Utils.stringify(etag),
        Utils.stringify(checksum),
        Utils.stringify(objectParts),
        Utils.stringify(storageClass),
        Utils.stringify(objectSize),
        Utils.stringify(deleteMarker),
        Utils.stringify(lastModified),
        Utils.stringify(versionId));
  }

  @Root(name = "ObjectParts", strict = false)
  public static class ObjectParts extends BasePartsResult {
    @Element(name = "PartsCount", required = false)
    private Integer partsCount;

    public ObjectParts() {
      super();
    }

    public Integer partsCount() {
      return partsCount;
    }

    @Override
    public String toString() {
      return String.format(
          "ObjectParts{partsCount=%s, %s}", Utils.stringify(partsCount), super.toString());
    }
  }
}
