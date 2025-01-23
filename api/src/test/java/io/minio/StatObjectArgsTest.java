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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.KeyGenerator;
import org.junit.Assert;
import org.junit.Test;

public class StatObjectArgsTest {
  @Test(expected = NullPointerException.class)
  public void testEmptyBuild() {
    StatObjectArgs.builder().build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyBucketBuild1() {
    StatObjectArgs.builder().object("myobject").build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyBucketBuild2() {
    StatObjectArgs.builder().object("myobject").bucket(null).build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyBucketBuild3() {
    StatObjectArgs.builder().bucket("mybucket").bucket(null).build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyRegionBuild() {
    StatObjectArgs.builder().region("").build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyObjectBuild1() {
    StatObjectArgs.builder().object(null).build();
    Assert.fail("exception should be thrown");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyObjectBuild2() {
    StatObjectArgs.builder().bucket("mybucket").ssec(null).object("").build();
    Assert.fail("exception should be thrown");
  }

  @Test
  public void testBuild() throws NoSuchAlgorithmException, InvalidKeyException {
    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(256);
    ServerSideEncryption.CustomerKey ssec =
        new ServerSideEncryption.CustomerKey(keyGen.generateKey());
    StatObjectArgs args =
        StatObjectArgs.builder()
            .bucket("mybucket")
            .ssec(ssec)
            .object("myobject")
            .region("myregion")
            .versionId("myversionid")
            .build();
    Assert.assertEquals("mybucket", args.bucket());
    Assert.assertEquals("myregion", args.region());
    Assert.assertEquals("myobject", args.object());
    Assert.assertEquals("myversionid", args.versionId());
    Assert.assertEquals(ssec, args.ssec());

    args = StatObjectArgs.builder().bucket("mybucket").object("myobject").build();
    Assert.assertEquals("mybucket", args.bucket());
    Assert.assertEquals(null, args.region());
    Assert.assertEquals("myobject", args.object());
    Assert.assertEquals(null, args.versionId());
    Assert.assertEquals(null, args.ssec());
  }
}
