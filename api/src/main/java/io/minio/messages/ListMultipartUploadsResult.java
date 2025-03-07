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

import io.minio.Time;
import io.minio.Utils;
import java.time.ZonedDateTime;
import java.util.List;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Namespace;
import org.simpleframework.xml.Root;

/**
 * Object representation of response XML of <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html">ListMultipartUploads
 * API</a>.
 */
@Root(name = "ListMultipartUploadsResult", strict = false)
@Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListMultipartUploadsResult {
  @Element(name = "Bucket")
  private String bucketName;

  @Element(name = "EncodingType", required = false)
  private String encodingType;

  @Element(name = "KeyMarker", required = false)
  private String keyMarker;

  @Element(name = "UploadIdMarker", required = false)
  private String uploadIdMarker;

  @Element(name = "NextKeyMarker", required = false)
  private String nextKeyMarker;

  @Element(name = "NextUploadIdMarker", required = false)
  private String nextUploadIdMarker;

  @Element(name = "MaxUploads")
  private int maxUploads;

  @Element(name = "IsTruncated", required = false)
  private boolean isTruncated;

  @ElementList(name = "Upload", inline = true, required = false)
  List<Upload> uploads;

  public ListMultipartUploadsResult() {}

  /** Returns whether the result is truncated or not. */
  public boolean isTruncated() {
    return isTruncated;
  }

  /** Returns bucket name. */
  public String bucketName() {
    return bucketName;
  }

  /** Returns key marker. */
  public String keyMarker() {
    return Utils.urlDecode(keyMarker, encodingType);
  }

  /** Returns upload ID marker. */
  public String uploadIdMarker() {
    return uploadIdMarker;
  }

  /** Returns next key marker. */
  public String nextKeyMarker() {
    return Utils.urlDecode(nextKeyMarker, encodingType);
  }

  /** Returns next upload ID marker. */
  public String nextUploadIdMarker() {
    return nextUploadIdMarker;
  }

  /** Returns max uploads received. */
  public int maxUploads() {
    return maxUploads;
  }

  public String encodingType() {
    return encodingType;
  }

  /** Returns List of Upload. */
  public List<Upload> uploads() {
    return Utils.unmodifiableList(uploads);
  }

  @Override
  public String toString() {
    return String.format(
        "ListMultipartUploadsResult{bucketName=%s, encodingType=%s, keyMarker=%s,"
            + " uploadIdMarker=%s, nextKeyMarker=%s, nextUploadIdMarker=%s, maxUploads=%s,"
            + " isTruncated=%s, uploads=%s}",
        Utils.stringify(bucketName),
        Utils.stringify(encodingType),
        Utils.stringify(keyMarker),
        Utils.stringify(uploadIdMarker),
        Utils.stringify(nextKeyMarker),
        Utils.stringify(nextUploadIdMarker),
        Utils.stringify(maxUploads),
        Utils.stringify(isTruncated),
        Utils.stringify(uploads));
  }

  @Root(name = "Upload", strict = false)
  @Namespace(reference = "http://s3.amazonaws.com/doc/2006-03-01/")
  public static class Upload {
    @Element(name = "Key")
    private String objectName;

    @Element(name = "UploadId")
    private String uploadId;

    @Element(name = "Initiator")
    private Initiator initiator;

    @Element(name = "Owner")
    private Owner owner;

    @Element(name = "StorageClass")
    private String storageClass;

    @Element(name = "Initiated")
    private Time.S3Time initiated;

    @Element(name = "ChecksumAlgorithm", required = false)
    private String checksumAlgorithm;

    @Element(name = "ChecksumType", required = false)
    private String checksumType;

    private long aggregatedPartSize;
    private String encodingType = null;

    public Upload() {}

    /** Returns object name. */
    public String objectName() {
      return Utils.urlDecode(objectName, encodingType);
    }

    /** Returns upload ID. */
    public String uploadId() {
      return uploadId;
    }

    /** Returns initiator information. */
    public Initiator initiator() {
      return initiator;
    }

    /** Returns owner information. */
    public Owner owner() {
      return owner;
    }

    /** Returns storage class. */
    public String storageClass() {
      return storageClass;
    }

    /** Returns initiated time. */
    public ZonedDateTime initiated() {
      return initiated == null ? null : initiated.toZonedDateTime();
    }

    /** Returns aggregated part size. */
    public long aggregatedPartSize() {
      return aggregatedPartSize;
    }

    /** Sets given aggregated part size. */
    public void setAggregatedPartSize(long size) {
      this.aggregatedPartSize = size;
    }

    public void setEncodingType(String encodingType) {
      this.encodingType = encodingType;
    }

    public String checksumAlgorithm() {
      return checksumAlgorithm;
    }

    public String checksumType() {
      return checksumType;
    }

    @Override
    public String toString() {
      return String.format(
          "Upload{objectName=%s, uploadId=%s, initiator=%s, owner=%s, storageClass=%s, "
              + "initiated=%s, checksumAlgorithm=%s, checksumType=%s, aggregatedPartSize=%s}",
          Utils.stringify(objectName),
          Utils.stringify(uploadId),
          Utils.stringify(initiator),
          Utils.stringify(owner),
          Utils.stringify(storageClass),
          Utils.stringify(initiated),
          Utils.stringify(checksumAlgorithm),
          Utils.stringify(checksumType),
          Utils.stringify(aggregatedPartSize));
    }
  }
}
