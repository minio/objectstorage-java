/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2015 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.minio.MinioClient;
import io.minio.ServerSideEncryption;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.MinioException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import javax.crypto.KeyGenerator;

public class StatObject {
  /** MinioClient.statObject() example. */
  public static void main(String[] args)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    try {
      /* play.min.io for test and development. */
      MinioClient minioClient =
          MinioClient.builder()
              .endpoint("https://play.min.io")
              .credentials("Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
              .build();

      /* Amazon S3: */
      // MinioClient minioClient =
      //     MinioClient.builder()
      //         .endpoint("https://s3.amazonaws.com")
      //         .credentials("YOUR-ACCESSKEY", "YOUR-SECRETACCESSKEY")
      //         .build();

      KeyGenerator keyGen = KeyGenerator.getInstance("AES");
      keyGen.init(256);
      ServerSideEncryption.CustomerKey ssec =
          new ServerSideEncryption.CustomerKey(keyGen.generateKey());
      String versionId = "ac38316c-fe14-4f96-9f76-8f675ae5a79e";

      {
        // Get information of an object.
        StatObjectResponse stat =
            minioClient.statObject(
                StatObjectArgs.builder().bucket("my-bucketname").object("my-objectname").build());
        System.out.println(stat);
      }

      {
        // Get information of SSE-C encrypted object.
        StatObjectResponse stat =
            minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket("my-bucketname")
                    .object("my-encrypted-objectname")
                    .ssec(ssec) // Replace with actual key.
                    .build());
        System.out.println(stat);
      }

      {
        // Get information of a versioned object.
        StatObjectResponse stat =
            minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket("my-bucketname")
                    .object("my-versioned-objectname")
                    .versionId(versionId) // Replace with actual version ID.
                    .build());
        System.out.println(stat);
      }

      {
        // Get information of a SSE-C encrypted versioned object.
        StatObjectResponse stat =
            minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket("my-bucketname")
                    .object("my-encrypted-versioned-objectname")
                    .versionId(versionId) // Replace with actual version ID.
                    .ssec(ssec) // Replace with actual key.
                    .build());
        System.out.println(stat);
      }

      {
        // Get information of an object with extra headers and query parameters.
        HashMap<String, String> headers = new HashMap<>();
        headers.put("x-amz-request-payer", "requester");
        HashMap<String, String> queryParams = new HashMap<>();
        queryParams.put("partNumber", "1");

        StatObjectResponse stat =
            minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket("my-bucketname")
                    .object("my-objectname")
                    .extraHeaders(headers) // Replace with actual headers.
                    .extraQueryParams(queryParams) // Replace with actual query parameters.
                    .build());
        System.out.println(stat);
      }
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }
}
