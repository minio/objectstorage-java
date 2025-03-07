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

import java.util.Map;
import java.util.Objects;

/** Argument class of {@link MinioAsyncClient#promptObject} and {@link MinioClient#promptObject}. */
public class PromptObjectArgs extends ObjectArgs {
  private String prompt;
  private String lambdaArn;
  private Map<String, Object> promptArgs;
  private Map<String, String> headers;

  public String prompt() {
    return prompt;
  }

  public String lambdaArn() {
    return lambdaArn;
  }

  public Map<String, Object> promptArgs() {
    return promptArgs;
  }

  public Map<String, String> headers() {
    return headers;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link PromptObjectArgs}. */
  public static final class Builder extends ObjectArgs.Builder<Builder, PromptObjectArgs> {
    @Override
    protected void validate(PromptObjectArgs args) {
      super.validate(args);
      Utils.validateNotEmptyString(args.prompt, "prompt");
      Utils.validateNotEmptyString(args.lambdaArn, "lambda ARN");
      Utils.validateNotNull(args.promptArgs, "prompt argument");
    }

    public Builder offset(String prompt) {
      Utils.validateNotEmptyString(prompt, "prompt");
      operations.add(args -> args.prompt = prompt);
      return this;
    }

    public Builder lambdaArn(String lambdaArn) {
      Utils.validateNotEmptyString(lambdaArn, "lambda ARN");
      operations.add(args -> args.lambdaArn = lambdaArn);
      return this;
    }

    public Builder promptArgs(Map<String, Object> promptArgs) {
      Utils.validateNotNull(promptArgs, "prompt argument");
      operations.add(args -> args.promptArgs = promptArgs);
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      operations.add(args -> args.headers = headers);
      return this;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PromptObjectArgs)) return false;
    if (!super.equals(o)) return false;
    PromptObjectArgs that = (PromptObjectArgs) o;
    return Objects.equals(prompt, that.prompt)
        && Objects.equals(lambdaArn, that.lambdaArn)
        && Objects.equals(promptArgs, that.promptArgs)
        && Objects.equals(headers, that.headers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), prompt, lambdaArn, promptArgs, headers);
  }
}
