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

/** Base argument class for reading object. */
public abstract class ObjectReadArgs extends ObjectVersionArgs {
  protected ServerSideEncryption.CustomerKey ssec;

  public ServerSideEncryption.CustomerKey ssec() {
    return ssec;
  }

  protected void validateSsec(HttpUrl url) {
    checkSse(ssec, url);
  }

  /** Base argument builder class for {@link ObjectReadArgs}. */
  public abstract static class Builder<B extends Builder<B, A>, A extends ObjectReadArgs>
      extends ObjectVersionArgs.Builder<B, A> {
    @SuppressWarnings("unchecked") // Its safe to type cast to B as B is inherited by this class
    public B ssec(ServerSideEncryption.CustomerKey ssec) {
      operations.add(args -> args.ssec = ssec);
      return (B) this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ObjectReadArgs)) return false;
    if (!super.equals(o)) return false;
    ObjectReadArgs that = (ObjectReadArgs) o;
    return Objects.equals(ssec, that.ssec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), ssec);
  }
}
