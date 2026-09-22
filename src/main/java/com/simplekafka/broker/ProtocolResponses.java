package com.simplekafka.broker;

/**
 * 兼容性占位类。
 *
 * <p>协议请求编码与响应解码逻辑已全部合并回 {@link Protocol}，与
 * build-your-own-kafka-main 参考实现保持一致。该类不再包含任何逻辑，
 * 仅为兼容旧的项目结构而保留。
 */
final class ProtocolResponses {
  private ProtocolResponses() {
  }
}
