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

/** Helper class to denote Input Serialization information of {@link SelectObjectContentRequest}. */
@Root(name = "InputSerialization")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
public class InputSerialization {
  @Element(name = "CompressionType", required = false)
  private CompressionType compressionType;

  @Element(name = "CSV", required = false)
  private CSV csv;

  @Element(name = "JSON", required = false)
  private JSON json;

  @Element(name = "Parquet", required = false)
  private Parquet parquet;

  private InputSerialization(CompressionType compressionType, CSV csv, JSON json, Parquet parquet) {
    this.compressionType = compressionType;
    this.csv = csv;
    this.json = json;
    this.parquet = parquet;
  }

  /** Constructs a new InputSerialization object with CSV. */
  public static InputSerialization newCSV(
      CompressionType compressionType,
      boolean allowQuotedRecordDelimiter,
      Character comments,
      Character fieldDelimiter,
      FileHeaderInfo fileHeaderInfo,
      Character quoteCharacter,
      Character quoteEscapeCharacter,
      Character recordDelimiter) {
    return new InputSerialization(
        compressionType,
        new CSV(
            allowQuotedRecordDelimiter,
            comments,
            fieldDelimiter,
            fileHeaderInfo,
            quoteCharacter,
            quoteEscapeCharacter,
            recordDelimiter),
        null,
        null);
  }

  /** Constructs a new InputSerialization object with JSON. */
  public static InputSerialization newJSON(CompressionType compressionType, JsonType type) {
    return new InputSerialization(compressionType, null, new JSON(type), null);
  }

  /** Constructs a new InputSerialization object with Parquet. */
  public static InputSerialization newParquet() {
    return new InputSerialization(null, null, null, new Parquet());
  }

  @Root(name = "CSV")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class CSV {
    @Element(name = "AllowQuotedRecordDelimiter", required = false)
    private boolean allowQuotedRecordDelimiter;

    @Element(name = "Comments", required = false)
    private Character comments;

    @Element(name = "FieldDelimiter", required = false)
    private Character fieldDelimiter;

    @Element(name = "FileHeaderInfo", required = false)
    private FileHeaderInfo fileHeaderInfo;

    @Element(name = "QuoteCharacter", required = false)
    private Character quoteCharacter;

    @Element(name = "QuoteEscapeCharacter", required = false)
    private Character quoteEscapeCharacter;

    @Element(name = "RecordDelimiter", required = false)
    private Character recordDelimiter;

    public CSV(
        boolean allowQuotedRecordDelimiter,
        Character comments,
        Character fieldDelimiter,
        FileHeaderInfo fileHeaderInfo,
        Character quoteCharacter,
        Character quoteEscapeCharacter,
        Character recordDelimiter) {
      this.allowQuotedRecordDelimiter = allowQuotedRecordDelimiter;
      this.comments = comments;
      this.fieldDelimiter = fieldDelimiter;
      this.fileHeaderInfo = fileHeaderInfo;
      this.quoteCharacter = quoteCharacter;
      this.quoteEscapeCharacter = quoteEscapeCharacter;
      this.recordDelimiter = recordDelimiter;
    }
  }

  @Root(name = "JSON")
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "URF_UNREAD_FIELD")
  public static class JSON {
    @Element(name = "Type", required = false)
    private JsonType type;

    public JSON(JsonType type) {
      this.type = type;
    }
  }

  @Root(name = "Parquet")
  public static class Parquet {}

  /** CSV/JSON object's compression format for select object content. */
  public enum CompressionType {
    NONE,
    GZIP,
    BZIP2;
  }

  /** Description the first line of input in CSV object. */
  public enum FileHeaderInfo {
    USE,
    IGNORE,
    NONE;
  }

  /** The type of JSON. */
  public enum JsonType {
    DOCUMENT,
    LINES;
  }
}
