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

package io.minio;

import java.util.Objects;
import okhttp3.HttpUrl;

/** Argument class of {@link MinioAsyncClient#copyObject} and {@link MinioClient#copyObject}. */
public class CopyObjectArgs extends ObjectWriteArgs {
  private CopySource source = null;
  private Directive metadataDirective;
  private Directive taggingDirective;

  protected CopyObjectArgs() {}

  public CopyObjectArgs(ComposeObjectArgs args) {
    this.extraHeaders = args.extraHeaders;
    this.extraQueryParams = args.extraQueryParams;
    this.bucketName = args.bucketName;
    this.region = args.region;
    this.objectName = args.objectName;
    this.headers = args.headers;
    this.userMetadata = args.userMetadata;
    this.sse = args.sse;
    this.tags = args.tags;
    this.retention = args.retention;
    this.legalHold = args.legalHold;
    this.source = new CopySource(args.sources().get(0));
  }

  public CopySource source() {
    return source;
  }

  public Directive metadataDirective() {
    return metadataDirective;
  }

  public Directive taggingDirective() {
    return taggingDirective;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void validateSse(HttpUrl url) {
    super.validateSse(url);
    source.validateSsec(url);
  }

  /** Argument builder of {@link CopyObjectArgs}. */
  public static final class Builder extends ObjectWriteArgs.Builder<Builder, CopyObjectArgs> {
    @Override
    protected void validate(CopyObjectArgs args) {
      super.validate(args);
      Utils.validateNotNull(args.source, "copy source");
      if (args.source.offset() != null || args.source.length() != null) {
        if (args.metadataDirective != null && args.metadataDirective == Directive.COPY) {
          throw new IllegalArgumentException(
              "COPY metadata directive is not applicable to source object with range");
        }
        if (args.taggingDirective != null && args.taggingDirective == Directive.COPY) {
          throw new IllegalArgumentException(
              "COPY tagging directive is not applicable to source object with range");
        }
      }
    }

    public Builder source(CopySource source) {
      Utils.validateNotNull(source, "copy source");
      operations.add(args -> args.source = source);
      return this;
    }

    public Builder metadataDirective(Directive directive) {
      operations.add(args -> args.metadataDirective = directive);
      return this;
    }

    public Builder taggingDirective(Directive directive) {
      operations.add(args -> args.taggingDirective = directive);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CopyObjectArgs)) return false;
    if (!super.equals(o)) return false;
    CopyObjectArgs that = (CopyObjectArgs) o;
    return Objects.equals(source, that.source)
        && metadataDirective == that.metadataDirective
        && taggingDirective == that.taggingDirective;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), source, metadataDirective, taggingDirective);
  }
}
