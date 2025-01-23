/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2021 MinIO, Inc.
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

import java.util.Objects;
import javax.annotation.Nonnull;
import org.simpleframework.xml.Element;

/**
 * Base class for {@link SelectObjectContentRequest} and {@link RestoreRequest.SelectParameters}.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public abstract class BaseSelectParameters {
  @Element(name = "Expression")
  private String expression;

  @Element(name = "ExpressionType")
  private String expressionType = "SQL";

  @Element(name = "InputSerialization")
  private InputSerialization inputSerialization;

  @Element(name = "OutputSerialization")
  private OutputSerialization outputSerialization;

  public BaseSelectParameters(
      @Nonnull String expression, @Nonnull InputSerialization is, @Nonnull OutputSerialization os) {
    this.expression = Objects.requireNonNull(expression, "Expression must not be null");
    this.inputSerialization = Objects.requireNonNull(is, "InputSerialization must not be null");
    this.outputSerialization = Objects.requireNonNull(os, "OutputSerialization must not be null");
  }
}
