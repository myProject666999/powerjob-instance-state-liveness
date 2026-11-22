package tech.powerjob.server.core.scheduler;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 普通任务实例失联检查的配置项。
 * 默认值与历史行为保持一致（原 RUNNING 上报超时 60s、Worker 心跳超时 60s），
 * 探测相关数值为初版默认值，具体取值待确认后再调整。
 *
 * @author tjq
 * @since 2026/11/23
 */
@Data
@Component
public class InstanceLivenessProperties {

    /**
     * RUNNING 实例的上报停滞阈值（毫秒）：最后修改时间早于该阈值即进入失联检查。
     * 默认 60000，与历史 RUNNING_TIMEOUT_MS 一致。
     */
    @Value("${oms.instance.check.report-timeout-ms:60000}")
    private long reportTimeoutMs = 60000;

    /**
     * Worker 心跳超时阈值（毫秒）：超过该值未收到 Worker 心跳视为 Worker 失联。
     * 默认 60000，与 WorkerInfo 历史判定一致。
     */
    @Value("${oms.instance.check.heartbeat-timeout-ms:60000}")
    private long heartbeatTimeoutMs = 60000;

    /**
     * 单次探测（ServerQueryInstanceStatusReq）的超时时间（毫秒）。
     * 默认 3000，具体数值待确认。
     */
    @Value("${oms.instance.check.probe-timeout-ms:3000}")
    private long probeTimeoutMs = 3000;

    /**
     * 探测线程池的并发上限。默认 16，具体数值待确认。
     */
    @Value("${oms.instance.check.probe-concurrency:16}")
    private int probeConcurrency = 16;
}
