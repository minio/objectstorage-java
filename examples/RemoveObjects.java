/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage, (C) 2017 MinIO, Inc.
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
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.errors.MinioException;
import io.minio.messages.DeleteRequest;
import io.minio.messages.DeleteResult;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

public class RemoveObjects {
  /** MinioClient.removeObject() example removing multiple objects. */
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

      List<DeleteRequest.Object> objects = new LinkedList<>();
      objects.add(new DeleteRequest.Object("my-objectname1"));
      objects.add(new DeleteRequest.Object("my-objectname2"));
      objects.add(new DeleteRequest.Object("my-objectname3"));
      Iterable<Result<DeleteResult.Error>> results =
          minioClient.removeObjects(
              RemoveObjectsArgs.builder().bucket("my-bucketname").objects(objects).build());
      for (Result<DeleteResult.Error> result : results) {
        DeleteResult.Error error = result.get();
        System.out.println(
            "Error in deleting object " + error.objectName() + "; " + error.message());
      }
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
    }
  }
}
