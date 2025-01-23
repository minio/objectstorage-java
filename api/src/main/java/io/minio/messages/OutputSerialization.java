/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2019 MinIO, Inc.
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

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/**
 * Helper class to denote Output Serialization information of {@link SelectObjectContentRequest}.
 */
@Root(name = "OutputSerialization")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public class OutputSerialization {
  @Element(name = "CSV", required = false)
  private CSV csv;

  @Element(name = "JSON", required = false)
  private JSON json;

  private OutputSerialization(CSV csv, JSON json) {
    this.csv = csv;
    this.json = json;
  }

  /** Constructs a new OutputSerialization object with CSV. */
  public static OutputSerialization newCSV(
      Character fieldDelimiter,
      Character quoteCharacter,
      Character quoteEscapeCharacter,
      QuoteFields quoteFields,
      Character recordDelimiter) {
    return new OutputSerialization(
        new CSV(fieldDelimiter, quoteCharacter, quoteEscapeCharacter, quoteFields, recordDelimiter),
        null);
  }

  /** Constructs a new OutputSerialization object with JSON. */
  public static OutputSerialization newJSON(Character recordDelimiter) {
    return new OutputSerialization(null, new JSON(recordDelimiter));
  }

  @Root(name = "CSV")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class CSV {
    @Element(name = "FieldDelimiter", required = false)
    private Character fieldDelimiter;

    @Element(name = "QuoteCharacter", required = false)
    private Character quoteCharacter;

    @Element(name = "QuoteEscapeCharacter", required = false)
    private Character quoteEscapeCharacter;

    @Element(name = "QuoteFields", required = false)
    private QuoteFields quoteFields;

    @Element(name = "RecordDelimiter", required = false)
    private Character recordDelimiter;

    public CSV(
        Character fieldDelimiter,
        Character quoteCharacter,
        Character quoteEscapeCharacter,
        QuoteFields quoteFields,
        Character recordDelimiter) {
      this.fieldDelimiter = fieldDelimiter;
      this.quoteCharacter = quoteCharacter;
      this.quoteEscapeCharacter = quoteEscapeCharacter;
      this.quoteFields = quoteFields;
      this.recordDelimiter = recordDelimiter;
    }
  }

  @Root(name = "JSON")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class JSON {
    @Element(name = "RecordDelimiter", required = false)
    private Character recordDelimiter;

    public JSON(Character recordDelimiter) {
      this.recordDelimiter = recordDelimiter;
    }
  }

  /** Indicates whether to use quotation marks around output fields. */
  public static enum QuoteFields {
    ALWAYS,
    ASNEEDED;
  }
}
