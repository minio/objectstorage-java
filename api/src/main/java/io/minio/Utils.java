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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import okhttp3.Headers;
import okhttp3.HttpUrl;

/** Collection of utility functions. */
public class Utils {
  private static final Escaper ESCAPER = UrlEscapers.urlPathSegmentEscaper();

  public static final String UTF_8 = StandardCharsets.UTF_8.toString();

  public static final String AWS_S3_PREFIX =
      "^(((bucket\\.|accesspoint\\.)"
          + "vpce(-(?!_)[a-z_\\d]+(?<!-)(?<!_))+\\.s3\\.)|"
          + "((?!s3)(?!-)(?!_)[a-z_\\d-]{1,63}(?<!-)(?<!_)\\.)"
          + "s3-control(-(?!_)[a-z_\\d]+(?<!-)(?<!_))*\\.|"
          + "(s3(-(?!_)[a-z_\\d]+(?<!-)(?<!_))*\\.))";

  public static final Pattern HOSTNAME_REGEX =
      Pattern.compile(
          "^((?!-)(?!_)[a-z_\\d-]{1,63}(?<!-)(?<!_)\\.)*"
              + "((?!_)(?!-)[a-z_\\d-]{1,63}(?<!-)(?<!_))$",
          Pattern.CASE_INSENSITIVE);
  public static final Pattern AWS_ENDPOINT_REGEX =
      Pattern.compile(".*\\.amazonaws\\.com(|\\.cn)$", Pattern.CASE_INSENSITIVE);
  public static final Pattern AWS_S3_ENDPOINT_REGEX =
      Pattern.compile(
          AWS_S3_PREFIX
              + "((?!s3)(?!-)(?!_)[a-z_\\d-]{1,63}(?<!-)(?<!_)\\.)*amazonaws\\.com(|\\.cn)$",
          Pattern.CASE_INSENSITIVE);
  public static final Pattern AWS_ELB_ENDPOINT_REGEX =
      Pattern.compile(
          "^(?!-)(?!_)[a-z_\\d-]{1,63}(?<!-)(?<!_)\\."
              + "(?!-)(?!_)[a-z_\\d-]{1,63}(?<!-)(?<!_)\\."
              + "elb\\.amazonaws\\.com$",
          Pattern.CASE_INSENSITIVE);
  public static final Pattern AWS_S3_PREFIX_REGEX =
      Pattern.compile(AWS_S3_PREFIX, Pattern.CASE_INSENSITIVE);
  public static final Pattern REGION_REGEX =
      Pattern.compile("^((?!_)(?!-)[a-z_\\d-]{1,63}(?<!-)(?<!_))$", Pattern.CASE_INSENSITIVE);

  public static final byte[] EMPTY_BODY = new byte[] {};

  public static <T> T validateNotNull(T arg, String argName) {
    return Objects.requireNonNull(arg, argName + " must not be null");
  }

  public static void validateNotEmptyString(String arg, String argName) {
    validateNotNull(arg, argName);
    if (arg.isEmpty()) {
      throw new IllegalArgumentException(argName + " must be a non-empty string.");
    }
  }

  public static void validateNullOrNotEmptyString(String arg, String argName) {
    if (arg != null && arg.isEmpty()) {
      throw new IllegalArgumentException(argName + " must be a non-empty string.");
    }
  }

  public static boolean isValidIPv4OrIPv6(String value) {
    return InetAddressValidator.getInstance().isValid(value);
  }

  public static boolean isValidIPv6(String value) {
    return InetAddressValidator.getInstance().isValidInet6Address(value);
  }

  public static boolean isValidIPv4(String value) {
    return InetAddressValidator.getInstance().isValidInet4Address(value);
  }

  public static void validateHostnameOrIPAddress(String endpoint) {
    if (isValidIPv4OrIPv6(endpoint)) return;

    if (!HOSTNAME_REGEX.matcher(endpoint).find()) {
      throw new IllegalArgumentException("invalid hostname " + endpoint);
    }
  }

  public static void validateUrl(HttpUrl url) {
    if (!url.encodedPath().equals("/")) {
      throw new IllegalArgumentException("no path allowed in endpoint " + url);
    }
  }

  public static HttpUrl getBaseUrl(String endpoint) {
    validateNotEmptyString(endpoint, "endpoint");
    HttpUrl url = HttpUrl.parse(endpoint);
    if (url == null) {
      validateHostnameOrIPAddress(endpoint);
      url = new HttpUrl.Builder().scheme("https").host(endpoint).build();
    } else {
      validateUrl(url);
    }

    return url;
  }

  public static String getHostHeader(HttpUrl url) {
    String host = url.host();
    if (isValidIPv6(host)) host = "[" + host + "]";

    // ignore port when port and service matches i.e HTTP -> 80, HTTPS -> 443
    if ((url.scheme().equals("http") && url.port() == 80)
        || (url.scheme().equals("https") && url.port() == 443)) {
      return host;
    }

    return host + ":" + url.port();
  }

  public static String urlDecode(String value, String type) {
    if (!"url".equals(type)) return value;
    try {
      return value == null ? null : URLDecoder.decode(value, UTF_8);
    } catch (UnsupportedEncodingException e) {
      // This never happens as 'enc' name comes from JDK's own StandardCharsets.
      throw new RuntimeException(e);
    }
  }

  public static <T> List<T> unmodifiableList(List<? extends T> value) {
    return Collections.unmodifiableList(value == null ? new LinkedList<T>() : value);
  }

  public static <K, V> Map<K, V> unmodifiableMap(Map<? extends K, ? extends V> value) {
    return Collections.unmodifiableMap(value == null ? new HashMap<K, V>() : value);
  }

  public static String stringify(Object value) {
    if (value == null) return "<null>";

    if (value.getClass().isArray()) {
      StringBuilder result = new StringBuilder("[");

      int length = Array.getLength(value);

      if (value.getClass().getComponentType().isPrimitive()) {
        for (int i = 0; i < length; i++) {
          if (i > 0) result.append(", ");
          result.append(Array.get(value, i));
        }
      } else {
        for (int i = 0; i < length; i++) {
          if (i > 0) result.append(", ");
          Object element = Array.get(value, i);
          result.append(stringify(element));
        }
      }

      result.append("]");
      return result.toString();
    }

    if (value instanceof CharSequence) {
      return "'" + value.toString() + "'";
    }

    return value.toString();
  }

  /** Merge two Multimaps. */
  public static Multimap<String, String> mergeMultimap(
      Multimap<String, String> m1, Multimap<String, String> m2) {
    Multimap<String, String> map = HashMultimap.create();
    if (m1 != null) map.putAll(m1);
    if (m2 != null) map.putAll(m2);
    return map;
  }

  /** Create new HashMultimap by alternating keys and values. */
  public static Multimap<String, String> newMultimap(String... keysAndValues) {
    if (keysAndValues.length % 2 != 0) {
      throw new IllegalArgumentException("Expected alternating keys and values");
    }

    Multimap<String, String> map = HashMultimap.create();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put(keysAndValues[i], keysAndValues[i + 1]);
    }

    return map;
  }

  /** Create new HashMultimap with copy of Map. */
  public static Multimap<String, String> newMultimap(Map<String, String> map) {
    return (map != null) ? Multimaps.forMap(map) : HashMultimap.create();
  }

  /** Create new HashMultimap with copy of Multimap. */
  public static Multimap<String, String> newMultimap(Multimap<String, String> map) {
    return (map != null) ? HashMultimap.create(map) : HashMultimap.create();
  }

  /** Convert Multimap to Headers. */
  public static Headers httpHeaders(Multimap<String, String> headerMap) {
    Headers.Builder builder = new Headers.Builder();
    if (headerMap == null) return builder.build();

    if (headerMap.containsKey("Content-Encoding")) {
      builder.add(
          "Content-Encoding",
          headerMap.get("Content-Encoding").stream()
              .distinct()
              .filter(encoding -> !encoding.isEmpty())
              .collect(Collectors.joining(",")));
    }

    for (Map.Entry<String, String> entry : headerMap.entries()) {
      if (!entry.getKey().equals("Content-Encoding")) {
        builder.addUnsafeNonAscii(entry.getKey(), entry.getValue());
      }
    }

    return builder.build();
  }

  /** Returns S3 encoded string. */
  public static String encode(String str) {
    if (str == null) {
      return "";
    }

    StringBuilder builder = new StringBuilder();
    for (char ch : ESCAPER.escape(str).toCharArray()) {
      switch (ch) {
        case '!':
          builder.append("%21");
          break;
        case '$':
          builder.append("%24");
          break;
        case '&':
          builder.append("%26");
          break;
        case '\'':
          builder.append("%27");
          break;
        case '(':
          builder.append("%28");
          break;
        case ')':
          builder.append("%29");
          break;
        case '*':
          builder.append("%2A");
          break;
        case '+':
          builder.append("%2B");
          break;
        case ',':
          builder.append("%2C");
          break;
        case '/':
          builder.append("%2F");
          break;
        case ':':
          builder.append("%3A");
          break;
        case ';':
          builder.append("%3B");
          break;
        case '=':
          builder.append("%3D");
          break;
        case '@':
          builder.append("%40");
          break;
        case '[':
          builder.append("%5B");
          break;
        case ']':
          builder.append("%5D");
          break;
        default:
          builder.append(ch);
      }
    }
    return builder.toString();
  }

  /** Returns S3 encoded string of given path where multiple '/' are trimmed. */
  public static String encodePath(String path) {
    final StringBuilder encodedPath = new StringBuilder();
    for (String pathSegment : path.split("/")) {
      if (!pathSegment.isEmpty()) {
        if (encodedPath.length() > 0) {
          encodedPath.append("/");
        }
        encodedPath.append(Utils.encode(pathSegment));
      }
    }

    if (path.startsWith("/")) encodedPath.insert(0, "/");
    if (path.endsWith("/")) encodedPath.append("/");

    return encodedPath.toString();
  }

  public static String getDefaultUserAgent() {
    return String.format(
        "MinIO (%s; %s) minio-java/%s",
        System.getProperty("os.name"),
        System.getProperty("os.arch"),
        MinioProperties.INSTANCE.getVersion());
  }

  /** Identifies and stores version information of minio-java package at run time. */
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "MS_EXPOSE_REP")
  public static enum MinioProperties {
    INSTANCE;

    private static final Logger LOGGER = Logger.getLogger(MinioProperties.class.getName());

    private final AtomicReference<String> version = new AtomicReference<>(null);

    public String getVersion() {
      String result = version.get();
      if (result != null) {
        return result;
      }
      setVersion();
      return version.get();
    }

    private synchronized void setVersion() {
      if (version.get() != null) {
        return;
      }
      version.set("dev");
      ClassLoader classLoader = getClass().getClassLoader();
      if (classLoader == null) {
        return;
      }

      try {
        Enumeration<URL> resources = classLoader.getResources("META-INF/MANIFEST.MF");
        while (resources.hasMoreElements()) {
          try (InputStream is = resources.nextElement().openStream()) {
            Manifest manifest = new Manifest(is);
            if ("minio".equals(manifest.getMainAttributes().getValue("Implementation-Title"))) {
              version.set(manifest.getMainAttributes().getValue("Implementation-Version"));
              return;
            }
          }
        }
      } catch (IOException e) {
        LOGGER.log(Level.SEVERE, "IOException occurred", e);
        version.set("unknown");
      }
    }
  }

  /*
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *      http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */

  /**
   * <b>Regular Expression</b> validation (using JDK 1.4+ regex support).
   *
   * <p>Construct the validator either for a single regular expression or a set (array) of regular
   * expressions. By default validation is <i>case sensitive</i> but constructors are provided to
   * allow <i>case in-sensitive</i> validation. For example to create a validator which does <i>case
   * in-sensitive</i> validation for a set of regular expressions:
   *
   * <pre>
   * <code>
   * String[] regexs = new String[] {...};
   * RegexValidator validator = new RegexValidator(regexs, false);
   * </code>
   * </pre>
   *
   * <p>
   *
   * <ul>
   *   <li>Validate <code>true</code> or <code>false</code>:
   *   <li>
   *       <ul>
   *         <li><code>boolean valid = validator.isValid(value);</code>
   *       </ul>
   *   <li>Validate returning an aggregated String of the matched groups:
   *   <li>
   *       <ul>
   *         <li><code>String result = validator.validate(value);</code>
   *       </ul>
   *   <li>Validate returning the matched groups:
   *   <li>
   *       <ul>
   *         <li><code>String[] result = validator.match(value);</code>
   *       </ul>
   * </ul>
   *
   * <p><b>Note that patterns are matched against the entire input.</b>
   *
   * <p>
   *
   * <p>Cached instances pre-compile and re-use {@link Pattern}(s) - which according to the {@link
   * Pattern} API are safe to use in a multi-threaded environment.
   *
   * @version $Revision$
   * @since Validator 1.4
   */
  public static class RegexValidator implements Serializable {

    private static final long serialVersionUID = -8832409930574867162L;

    private final Pattern[] patterns;

    /**
     * Construct a <i>case sensitive</i> validator for a single regular expression.
     *
     * @param regex The regular expression this validator will validate against
     */
    public RegexValidator(String regex) {
      this(regex, true);
    }

    /**
     * Construct a validator for a single regular expression with the specified case sensitivity.
     *
     * @param regex The regular expression this validator will validate against
     * @param caseSensitive when <code>true</code> matching is <i>case sensitive</i>, otherwise
     *     matching is <i>case in-sensitive</i>
     */
    public RegexValidator(String regex, boolean caseSensitive) {
      this(new String[] {regex}, caseSensitive);
    }

    /**
     * Construct a <i>case sensitive</i> validator that matches any one of the set of regular
     * expressions.
     *
     * @param regexs The set of regular expressions this validator will validate against
     */
    public RegexValidator(String[] regexs) {
      this(regexs, true);
    }

    /**
     * Construct a validator that matches any one of the set of regular expressions with the
     * specified case sensitivity.
     *
     * @param regexs The set of regular expressions this validator will validate against
     * @param caseSensitive when <code>true</code> matching is <i>case sensitive</i>, otherwise
     *     matching is <i>case in-sensitive</i>
     */
    public RegexValidator(String[] regexs, boolean caseSensitive) {
      if (regexs == null || regexs.length == 0) {
        throw new IllegalArgumentException("Regular expressions are missing");
      }
      patterns = new Pattern[regexs.length];
      int flags = (caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
      for (int i = 0; i < regexs.length; i++) {
        if (regexs[i] == null || regexs[i].length() == 0) {
          throw new IllegalArgumentException("Regular expression[" + i + "] is missing");
        }
        patterns[i] = Pattern.compile(regexs[i], flags);
      }
    }

    /**
     * Validate a value against the set of regular expressions.
     *
     * @param value The value to validate.
     * @return <code>true</code> if the value is valid otherwise <code>false</code>.
     */
    public boolean isValid(String value) {
      if (value == null) {
        return false;
      }
      for (int i = 0; i < patterns.length; i++) {
        if (patterns[i].matcher(value).matches()) {
          return true;
        }
      }
      return false;
    }

    /**
     * Validate a value against the set of regular expressions returning the array of matched
     * groups.
     *
     * @param value The value to validate.
     * @return String array of the <i>groups</i> matched if valid or <code>null</code> if invalid
     */
    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
        value = "PZLA",
        justification = "Null is checked, not empty array. API is clear as well.")
    public String[] match(String value) {
      if (value == null) {
        return null;
      }
      for (int i = 0; i < patterns.length; i++) {
        Matcher matcher = patterns[i].matcher(value);
        if (matcher.matches()) {
          int count = matcher.groupCount();
          String[] groups = new String[count];
          for (int j = 0; j < count; j++) {
            groups[j] = matcher.group(j + 1);
          }
          return groups;
        }
      }
      return null;
    }

    /**
     * Validate a value against the set of regular expressions returning a String value of the
     * aggregated groups.
     *
     * @param value The value to validate.
     * @return Aggregated String value comprised of the <i>groups</i> matched if valid or <code>null
     *     </code> if invalid
     */
    public String validate(String value) {
      if (value == null) {
        return null;
      }
      for (int i = 0; i < patterns.length; i++) {
        Matcher matcher = patterns[i].matcher(value);
        if (matcher.matches()) {
          int count = matcher.groupCount();
          if (count == 1) {
            return matcher.group(1);
          }
          StringBuilder buffer = new StringBuilder();
          for (int j = 0; j < count; j++) {
            String component = matcher.group(j + 1);
            if (component != null) {
              buffer.append(component);
            }
          }
          return buffer.toString();
        }
      }
      return null;
    }

    /**
     * Provide a String representation of this validator.
     *
     * @return A String representation of this validator
     */
    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("RegexValidator{");
      for (int i = 0; i < patterns.length; i++) {
        if (i > 0) {
          buffer.append(",");
        }
        buffer.append(patterns[i].pattern());
      }
      buffer.append("}");
      return buffer.toString();
    }
  }

  /*
   * Licensed to the Apache Software Foundation (ASF) under one or more
   * contributor license agreements.  See the NOTICE file distributed with
   * this work for additional information regarding copyright ownership.
   * The ASF licenses this file to You under the Apache License, Version 2.0
   * (the "License"); you may not use this file except in compliance with
   * the License.  You may obtain a copy of the License at
   *
   *      http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */

  /**
   * <b>InetAddress</b> validation and conversion routines (<code>java.net.InetAddress</code>).
   *
   * <p>
   *
   * <p>
   *
   * <p>This class provides methods to validate a candidate IP address.
   *
   * <p>
   *
   * <p>This class is a Singleton; you can retrieve the instance via the {@link #getInstance()}
   * method.
   *
   * @version $Revision$
   * @since Validator 1.4
   */
  public static class InetAddressValidator {

    private static final int IPV4_MAX_OCTET_VALUE = 255;

    private static final int MAX_UNSIGNED_SHORT = 0xffff;

    private static final int BASE_16 = 16;

    private static final long serialVersionUID = -919201640201914789L;

    private static final String IPV4_REGEX = "^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$";

    // Max number of hex groups (separated by :) in an IPV6 address
    private static final int IPV6_MAX_HEX_GROUPS = 8;

    // Max hex digits in each IPv6 group
    private static final int IPV6_MAX_HEX_DIGITS_PER_GROUP = 4;

    /** Singleton instance of this class. */
    private static final InetAddressValidator VALIDATOR = new InetAddressValidator();

    /** IPv4 RegexValidator. */
    private final RegexValidator ipv4Validator = new RegexValidator(IPV4_REGEX);

    private InetAddressValidator() {}

    /**
     * Returns the singleton instance of this validator.
     *
     * @return the singleton instance of this validator
     */
    public static InetAddressValidator getInstance() {
      return VALIDATOR;
    }

    /**
     * Checks if the specified string is a valid IP address.
     *
     * @param inetAddress the string to validate
     * @return true if the string validates as an IP address
     */
    public boolean isValid(String inetAddress) {
      return isValidInet4Address(inetAddress) || isValidInet6Address(inetAddress);
    }

    /**
     * Validates an IPv4 address. Returns true if valid.
     *
     * @param inet4Address the IPv4 address to validate
     * @return true if the argument contains a valid IPv4 address
     */
    public boolean isValidInet4Address(String inet4Address) {
      // verify that address conforms to generic IPv4 format
      String[] groups = ipv4Validator.match(inet4Address);

      if (groups == null) {
        return false;
      }

      // verify that address subgroups are legal
      for (String ipSegment : groups) {
        if (ipSegment == null || ipSegment.length() == 0) {
          return false;
        }

        int iIpSegment = 0;

        try {
          iIpSegment = Integer.parseInt(ipSegment);
        } catch (NumberFormatException e) {
          return false;
        }

        if (iIpSegment > IPV4_MAX_OCTET_VALUE) {
          return false;
        }

        if (ipSegment.length() > 1 && ipSegment.startsWith("0")) {
          return false;
        }
      }

      return true;
    }

    /**
     * Validates an IPv6 address. Returns true if valid.
     *
     * @param inet6Address the IPv6 address to validate
     * @return true if the argument contains a valid IPv6 address
     * @since 1.4.1
     */
    public boolean isValidInet6Address(String inet6Address) {
      boolean containsCompressedZeroes = inet6Address.contains("::");
      if (containsCompressedZeroes
          && inet6Address.indexOf("::") != inet6Address.lastIndexOf("::")) {
        return false;
      }
      if (inet6Address.startsWith(":") && !inet6Address.startsWith("::")
          || inet6Address.endsWith(":") && !inet6Address.endsWith("::")) {
        return false;
      }
      String[] octets = inet6Address.split(":");
      if (containsCompressedZeroes) {
        List<String> octetList = new ArrayList<String>(Arrays.asList(octets));
        if (inet6Address.endsWith("::")) {
          // String.split() drops ending empty segments
          octetList.add("");
        } else if (inet6Address.startsWith("::") && !octetList.isEmpty()) {
          octetList.remove(0);
        }
        octets = octetList.toArray(new String[octetList.size()]);
      }
      if (octets.length > IPV6_MAX_HEX_GROUPS) {
        return false;
      }
      int validOctets = 0;
      int emptyOctets = 0;
      for (int index = 0; index < octets.length; index++) {
        String octet = octets[index];
        if (octet.length() == 0) {
          emptyOctets++;
          if (emptyOctets > 1) {
            return false;
          }
        } else {
          emptyOctets = 0;
          if (octet.contains(".")) { // contains is Java 1.5+
            if (!inet6Address.endsWith(octet)) {
              return false;
            }
            if (index > octets.length - 1 || index > 6) { // CHECKSTYLE IGNORE MagicNumber
              // IPV4 occupies last two octets
              return false;
            }
            if (!isValidInet4Address(octet)) {
              return false;
            }
            validOctets += 2;
            continue;
          }
          if (octet.length() > IPV6_MAX_HEX_DIGITS_PER_GROUP) {
            return false;
          }
          int octetInt = 0;
          try {
            octetInt = Integer.valueOf(octet, BASE_16).intValue();
          } catch (NumberFormatException e) {
            return false;
          }
          if (octetInt < 0 || octetInt > MAX_UNSIGNED_SHORT) {
            return false;
          }
        }
        validOctets++;
      }
      if (validOctets < IPV6_MAX_HEX_GROUPS && !containsCompressedZeroes) {
        return false;
      }
      return true;
    }
  }
}
