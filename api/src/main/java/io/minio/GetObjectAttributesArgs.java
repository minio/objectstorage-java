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

package io.minio;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#getObjectAttributes} and {@link
 * MinioClient#getObjectAttributes}.
 */
public class GetObjectAttributesArgs extends ObjectReadArgs {
  private List<String> objectAttributes;
  private Integer maxParts;
  private Integer partNumberMarker;

  public List<String> objectAttributes() {
    return objectAttributes;
  }

  public Integer maxParts() {
    return maxParts;
  }

  public Integer partNumberMarker() {
    return partNumberMarker;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link GetObjectAttributesArgs}. */
  public static final class Builder
      extends ObjectReadArgs.Builder<Builder, GetObjectAttributesArgs> {
    @Override
    protected void validate(GetObjectAttributesArgs args) {
      super.validate(args);
      Utils.validateNotNull(args.objectAttributes, "object attributes");
    }

    public Builder objectAttributes(String[] objectAttributes) {
      operations.add(
          args ->
              args.objectAttributes =
                  objectAttributes == null ? null : Arrays.asList(objectAttributes));
      return this;
    }

    public Builder objectAttributes(List<String> objectAttributes) {
      operations.add(args -> args.objectAttributes = objectAttributes);
      return this;
    }

    public Builder maxParts(Integer maxParts) {
      operations.add(args -> args.maxParts = maxParts);
      return this;
    }

    public Builder partNumberMarker(Integer partNumberMarker) {
      operations.add(args -> args.partNumberMarker = partNumberMarker);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof GetObjectAttributesArgs)) return false;
    if (!super.equals(o)) return false;
    GetObjectAttributesArgs that = (GetObjectAttributesArgs) o;
    return Objects.equals(objectAttributes, that.objectAttributes)
        && Objects.equals(maxParts, that.maxParts)
        && Objects.equals(partNumberMarker, that.partNumberMarker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), objectAttributes, maxParts, partNumberMarker);
  }
}
