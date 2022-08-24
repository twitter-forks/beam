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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
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
  private ByteArrayOutputStream outputBuffer;

  /** Create a ZstandardCodec instance with the given compressionLevel and checksum option. */
  public ZstandardCompressionCodec(int compressionLevel, boolean useChecksum) {
    this.compressionLevel = compressionLevel;
    this.useChecksum = useChecksum;
  }

  private int computeOffset(ByteBuffer data) {
    return data.arrayOffset() + data.position();
  }

  @Override
  public String getName() {
    return ZSTD_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    ByteArrayOutputStream baos = getOutputBuffer(data.remaining());
    byte[] bytes = new byte[data.remaining()];
    data.get(bytes);
    data.position(data.position() - bytes.length);

    try (OutputStream outputStream =
        new ZstdCompressorOutputStream(baos, compressionLevel, true, useChecksum)) {
      outputStream.write(bytes, computeOffset(data), data.remaining());
    }
    return ByteBuffer.wrap(baos.toByteArray());
  }

  @Override
  public ByteBuffer decompress(ByteBuffer compressedData) throws IOException {
    ByteArrayOutputStream baos = getOutputBuffer(compressedData.remaining());
    byte[] bytes = new byte[compressedData.remaining()];
    compressedData.get(bytes);
    compressedData.position(compressedData.position() - bytes.length);

    InputStream bytesIn =
        new ByteArrayInputStream(bytes, computeOffset(compressedData), compressedData.remaining());

    try (InputStream ios = new ZstdCompressorInputStream(bytesIn)) {
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
