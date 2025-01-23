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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import okhttp3.HttpUrl;

/** Base argument class. */
public abstract class BaseArgs {
  protected Multimap<String, String> extraHeaders =
      Multimaps.unmodifiableMultimap(HashMultimap.create());
  protected Multimap<String, String> extraQueryParams =
      Multimaps.unmodifiableMultimap(HashMultimap.create());

  public Multimap<String, String> extraHeaders() {
    return extraHeaders;
  }

  public Multimap<String, String> extraQueryParams() {
    return extraQueryParams;
  }

  protected void checkSse(ServerSideEncryption sse, HttpUrl url) {
    if (sse == null) {
      return;
    }

    if (sse.tlsRequired() && !url.isHttps()) {
      throw new IllegalArgumentException(
          sse + " operations must be performed over a secure connection.");
    }
  }

  /** Base builder which builds arguments. */
  public abstract static class Builder<B extends Builder<B, A>, A extends BaseArgs> {
    protected List<Consumer<A>> operations;

    protected abstract void validate(A args);

    public Builder() {
      this.operations = new ArrayList<>();
    }

    @SuppressWarnings("unchecked") // Its safe to type cast to B as B extends this class.
    public B extraHeaders(Multimap<String, String> headers) {
      final Multimap<String, String> extraHeaders = Utils.newMultimap(headers);
      operations.add(args -> args.extraHeaders = extraHeaders);
      return (B) this;
    }

    @SuppressWarnings("unchecked") // Its safe to type cast to B as B extends this class.
    public B extraQueryParams(Multimap<String, String> queryParams) {
      final Multimap<String, String> extraQueryParams = Utils.newMultimap(queryParams);
      operations.add(args -> args.extraQueryParams = extraQueryParams);
      return (B) this;
    }

    @SuppressWarnings("unchecked") // Its safe to type cast to B as B extends this class.
    public B extraHeaders(Map<String, String> headers) {
      final Multimap<String, String> extraHeaders = Utils.newMultimap(headers);
      operations.add(args -> args.extraHeaders = extraHeaders);
      return (B) this;
    }

    @SuppressWarnings("unchecked") // Its safe to type cast to B as B extends this class.
    public B extraQueryParams(Map<String, String> queryParams) {
      final Multimap<String, String> extraQueryParams = Utils.newMultimap(queryParams);
      operations.add(args -> args.extraQueryParams = extraQueryParams);
      return (B) this;
    }

    @SuppressWarnings("unchecked") // safe as B will always be the builder of the current args class
    private A newInstance() {
      try {
        for (Constructor<?> constructor :
            this.getClass().getEnclosingClass().getDeclaredConstructors()) {
          if (constructor.getParameterCount() == 0) {
            return (A) constructor.newInstance();
          }
        }

        throw new RuntimeException(
            this.getClass().getEnclosingClass() + " must have no argument constructor");
      } catch (InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | SecurityException e) {
        // Args class must have no argument constructor with at least protected access.
        throw new RuntimeException(e);
      }
    }

    /** Creates derived Args class with each attribute populated. */
    public A build() throws IllegalArgumentException {
      A args = newInstance();
      operations.forEach(operation -> operation.accept(args));
      validate(args);
      return args;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BaseArgs)) return false;
    BaseArgs baseArgs = (BaseArgs) o;
    return Objects.equals(extraHeaders, baseArgs.extraHeaders)
        && Objects.equals(extraQueryParams, baseArgs.extraQueryParams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(extraHeaders, extraQueryParams);
  }
}
