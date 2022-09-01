/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.commons.compress.utils.IOUtils;

public class ZstandardCompressionCodec extends Codec {
  public static final String ZSTD_CODEC = "zstd";

  static class Option extends CodecFactory {
    private final int compressionLevel;
    private final boolean useChecksum;

    public Option(int compressionLevel, boolean useChecksum) {
      this.compressionLevel = compressionLevel;
      this.useChecksum = useChecksum;
    }

    @Override
    protected Codec createInstance() {
      return new ZstandardCompressionCodec(compressionLevel, useChecksum);
    }
  }

  public static CodecFactory getCodecFactory(int compressionLevel, boolean useChecksum) {
    return new Option(compressionLevel, useChecksum);
  }

  private final int compressionLevel;
  private final boolean useChecksum;
  private final int windowLog;
  private final int workers;
  private ByteArrayOutputStream outputBuffer;

  /** Create a ZstandardCodec instance with the given compressionLevel and checksum option. */
  public ZstandardCompressionCodec(int compressionLevel, boolean useChecksum) {
    this.compressionLevel =
        Math.max(
            Math.min(compressionLevel, Zstd.maxCompressionLevel()), Zstd.minCompressionLevel());
    this.useChecksum = useChecksum;
    this.windowLog = -1;
    this.workers = -1;
  }

  /** Create a ZstandardCodec instance with the given compressionLevel and checksum option. */
  public ZstandardCompressionCodec(
      int compressionLevel, boolean useChecksum, int windowLog, int workers) {
    this.compressionLevel =
        Math.max(
            Math.min(compressionLevel, Zstd.maxCompressionLevel()), Zstd.minCompressionLevel());
    this.useChecksum = useChecksum;
    this.windowLog = windowLog;
    this.workers = workers;
  }

  @Override
  public String getName() {
    return ZSTD_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    data.position(data.position() - bytes.length);

    ByteArrayOutputStream baos = getOutputBuffer(data.remaining());
    ZstdOutputStream zstdOutputStream = new ZstdOutputStream(baos, compressionLevel);
    zstdOutputStream.setCloseFrameOnFlush(false);
    zstdOutputStream.setChecksum(useChecksum);
    if (windowLog != -1) {
      zstdOutputStream.setLong(windowLog);
    }
    if (workers != -1) {
      zstdOutputStream.setWorkers(workers);
    }

    try (OutputStream outputStream = zstdOutputStream) {
      outputStream.write(bytes, 0, bytes.length);
    }
    return ByteBuffer.wrap(baos.toByteArray());
  }

  @Override
  public ByteBuffer decompress(ByteBuffer compressedData) throws IOException {
    byte[] bytes = new byte[compressedData.remaining()];
    compressedData.get(bytes);
    compressedData.position(compressedData.position() - bytes.length);

    ByteArrayOutputStream baos = getOutputBuffer(compressedData.remaining());
    InputStream bytesIn = new ByteArrayInputStream(bytes, 0, bytes.length);

    try (InputStream ios = new ZstdInputStream(bytesIn)) {
      IOUtils.copy(ios, baos);
    }
    return ByteBuffer.wrap(baos.toByteArray());
  }

  // get and initialize the output buffer for use.
  private ByteArrayOutputStream getOutputBuffer(int suggestedLength) {
    if (outputBuffer == null) {
      outputBuffer = new ByteArrayOutputStream(suggestedLength);
    }
    outputBuffer.reset();
    return outputBuffer;
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj) || (obj != null && obj.getClass() == this.getClass());
  }

  @Override
  public String toString() {
    return this.getName() + "-" + this.compressionLevel;
  }
}
