/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2020 MinIO, Inc.
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

import io.minio.messages.InputSerialization;
import io.minio.messages.OutputSerialization;
import java.util.Objects;

/**
 * Argument class of {@link MinioAsyncClient#selectObjectContent} and {@link
 * MinioClient#selectObjectContent}.
 */
public class SelectObjectContentArgs extends ObjectReadArgs {
  private String sqlExpression;
  private InputSerialization inputSerialization;
  private OutputSerialization outputSerialization;
  private Boolean requestProgress;
  private Long scanStartRange;
  private Long scanEndRange;

  public Long scanEndRange() {
    return scanEndRange;
  }

  public Long scanStartRange() {
    return scanStartRange;
  }

  public Boolean requestProgress() {
    return requestProgress;
  }

  public OutputSerialization outputSerialization() {
    return outputSerialization;
  }

  public InputSerialization inputSerialization() {
    return inputSerialization;
  }

  public String sqlExpression() {
    return sqlExpression;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Argument builder of {@link SelectObjectContentArgs}. */
  public static final class Builder
      extends ObjectReadArgs.Builder<Builder, SelectObjectContentArgs> {
    private void validateSqlExpression(String se) {
      Utils.validateNotEmptyString(se, "sqlExpression");
    }

    public Builder sqlExpression(String sqlExpression) {
      validateSqlExpression(sqlExpression);
      operations.add(args -> args.sqlExpression = sqlExpression);
      return this;
    }

    private void validateInputSerialization(InputSerialization is) {
      Utils.validateNotNull(is, "inputSerialization");
    }

    public Builder inputSerialization(InputSerialization inputSerialization) {
      validateInputSerialization(inputSerialization);
      operations.add(args -> args.inputSerialization = inputSerialization);
      return this;
    }

    private void validateOutputSerialization(OutputSerialization os) {
      Utils.validateNotNull(os, "outputSerialization");
    }

    public Builder outputSerialization(OutputSerialization outputSerialization) {
      validateOutputSerialization(outputSerialization);
      operations.add(args -> args.outputSerialization = outputSerialization);
      return this;
    }

    public Builder requestProgress(Boolean requestProgress) {
      operations.add(args -> args.requestProgress = requestProgress);
      return this;
    }

    public Builder scanStartRange(Long scanStartRange) {
      operations.add(args -> args.scanStartRange = scanStartRange);
      return this;
    }

    public Builder scanEndRange(Long scanEndRange) {
      operations.add(args -> args.scanEndRange = scanEndRange);
      return this;
    }

    @Override
    protected void validate(SelectObjectContentArgs args) {
      super.validate(args);
      validateSqlExpression(args.sqlExpression());
      validateInputSerialization(args.inputSerialization());
      validateOutputSerialization(args.outputSerialization());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SelectObjectContentArgs)) return false;
    if (!super.equals(o)) return false;
    SelectObjectContentArgs that = (SelectObjectContentArgs) o;
    return Objects.equals(sqlExpression, that.sqlExpression)
        && Objects.equals(inputSerialization, that.inputSerialization)
        && Objects.equals(outputSerialization, that.outputSerialization)
        && Objects.equals(requestProgress, that.requestProgress)
        && Objects.equals(scanStartRange, that.scanStartRange)
        && Objects.equals(scanEndRange, that.scanEndRange);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        sqlExpression,
        inputSerialization,
        outputSerialization,
        requestProgress,
        scanStartRange,
        scanEndRange);
  }
}
