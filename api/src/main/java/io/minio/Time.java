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

import com.fasterxml.jackson.annotation.JsonCreator;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.convert.Convert;
import org.simpleframework.xml.convert.Converter;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/** Time formatters for S3 APIs. */
public class Time {
  public static final ZoneId UTC = ZoneId.of("Z");

  public static final DateTimeFormatter AMZ_DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'", Locale.US).withZone(UTC);

  public static final DateTimeFormatter ISO8601UTC_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'", Locale.US).withZone(UTC);

  // Formatted string is convertible to LocalDate only, not to LocalDateTime or ZonedDateTime.
  // Below example shows how to use this to get ZonedDateTime.
  // LocalDate.parse("20200225", SIGNER_DATE_FORMAT).atStartOfDay(UTC);
  public static final DateTimeFormatter SIGNER_DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US).withZone(UTC);

  public static final DateTimeFormatter HTTP_HEADER_DATE_FORMAT =
      DateTimeFormatter.ofPattern("EEE',' dd MMM yyyy HH':'mm':'ss 'GMT'", Locale.US).withZone(UTC);

  private Time() {}

  /** Wrapped {@link ZonedDateTime} to handle ISO8601UTC format. */
  @Root
  @Convert(S3Time.S3TimeConverter.class)
  public static class S3Time {
    // ISO8601UTC format handles 0 or more digits of fraction-of-second
    private static final DateTimeFormatter FORMAT =
        new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH':'mm':'ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendPattern("'Z'")
            .toFormatter(Locale.US)
            .withZone(UTC);

    private ZonedDateTime value;

    public S3Time() {}

    public S3Time(ZonedDateTime value) {
      this.value = value;
    }

    public ZonedDateTime toZonedDateTime() {
      return value;
    }

    @Override
    public String toString() {
      return value == null ? null : value.format(ISO8601UTC_FORMAT);
    }

    @JsonCreator
    public static S3Time fromString(String value) {
      return new S3Time(ZonedDateTime.parse(value, FORMAT));
    }

    /** XML converter class. */
    public static class S3TimeConverter implements Converter<S3Time> {
      @Override
      public S3Time read(InputNode node) throws Exception {
        return S3Time.fromString(node.getValue());
      }

      @Override
      public void write(OutputNode node, S3Time time) {
        node.setValue(time.toString());
      }
    }
  }
}
