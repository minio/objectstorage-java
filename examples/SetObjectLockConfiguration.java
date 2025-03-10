/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2019 MinIO, Inc.
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
import io.minio.SetObjectLockConfigurationArgs;
import io.minio.errors.MinioException;
import io.minio.messages.ObjectLockConfiguration;
import io.minio.messages.RetentionMode;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class SetObjectLockConfiguration {
  /** MinioClient.setObjectLockConfiguration() exanple. */
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

      // Declaring config with Retention mode as Compliance and duration as 100 days
      ObjectLockConfiguration config =
          new ObjectLockConfiguration(
              RetentionMode.COMPLIANCE, new ObjectLockConfiguration.RetentionDurationDays(100));

      minioClient.setObjectLockConfiguration(
          SetObjectLockConfigurationArgs.builder()
              .bucket("my-lock-enabled-bucketname")
              .config(config)
              .build());

      System.out.println("object-lock configuration is set successfully");
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }
}
