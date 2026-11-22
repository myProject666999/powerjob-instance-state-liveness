package tech.powerjob.server.core.scheduler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.SystemInstanceResult;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.SwitchableStatus;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.model.InstanceDetail;
import tech.powerjob.common.request.ServerQueryInstanceStatusReq;
import tech.powerjob.common.response.AskResponse;
import tech.powerjob.remote.framework.base.RemotingException;
import tech.powerjob.server.common.module.WorkerInfo;
import tech.powerjob.server.core.DispatchService;
import tech.powerjob.server.core.instance.InstanceManager;
import tech.powerjob.server.core.workflow.WorkflowInstanceManager;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.JobInfoDO;
import tech.powerjob.server.persistence.remote.model.brief.BriefInstanceInfo;
import tech.powerjob.server.persistence.remote.repository.*;
import tech.powerjob.server.remote.transporter.ProtocolInfo;
import tech.powerjob.server.remote.transporter.TransportService;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * RUNNING 实例失联检查测试：区分 Worker 心跳超时与心跳正常但上报停滞。
 * 纯 Mockito（不起数据库），注入固定时钟与同步探测执行器，不睡眠。
 */
class InstanceStatusCheckServiceLivenessTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final String TRACKER_ADDRESS = "worker1:27777";

    private final InstanceInfoRepository instances = mock(InstanceInfoRepository.class);
    private final JobInfoRepository jobs = mock(JobInfoRepository.class);
    private final AppInfoRepository apps = mock(AppInfoRepository.class);
    private final TransportService transport = mock(TransportService.class);
    private final InstanceManager completion = mock(InstanceManager.class);
    private final DispatchService dispatch = mock(DispatchService.class);
    private final WorkerClusterQueryService workers = mock(WorkerClusterQueryService.class);

    private InstanceStatusCheckService checker;
    private InstanceInfoDO instance;
    private JobInfoDO job;
    private BriefInstanceInfo row;

    @BeforeEach
    void setUp() {
        ProtocolInfo protocol = new ProtocolInfo();
        protocol.setAddress("fixture-server:10010");
        when(transport.defaultProtocol()).thenReturn(protocol);
        when(apps.listAppIdByCurrentServer(protocol.getAddress())).thenReturn(Collections.singletonList(10L));

        InstanceLivenessProperties properties = new InstanceLivenessProperties();
        properties.setProbeTimeoutMs(100);
        checker = new InstanceStatusCheckService(transport, dispatch, completion,
                mock(WorkflowInstanceManager.class), apps, jobs, instances,
                mock(WorkflowInfoRepository.class), mock(WorkflowInstanceInfoRepository.class),
                workers, properties);
        // 固定时钟 + 同步执行器：探测在检查线程内联执行，断言无需睡眠
        checker.setClock(Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC));
        checker.setProbeExecutor(Runnable::run);

        instance = new InstanceInfoDO();
        instance.setId(20L); instance.setAppId(10L); instance.setJobId(30L); instance.setInstanceId(40L);
        instance.setStatus(InstanceStatus.RUNNING.getV());
        instance.setRunningTimes(1L); instance.setGmtModified(new Date(1L));
        instance.setTaskTrackerAddress(TRACKER_ADDRESS);

        job = new JobInfoDO();
        job.setId(30L); job.setStatus(SwitchableStatus.ENABLE.getV());
        job.setTimeExpressionType(TimeExpressionType.API.getV()); job.setInstanceRetryNum(1);

        row = new BriefInstanceInfo(10L, 20L, 30L, 40L, 1L, TRACKER_ADDRESS);

        when(jobs.findByIdIn(any())).thenReturn(Collections.singletonList(job));
        when(instances.findById(20L)).thenReturn(Optional.of(instance));
        when(instances.selectBriefInfoByAppIdInAndStatusAndGmtModifiedBefore(anyList(),
                eq(InstanceStatus.RUNNING.getV()), any(Date.class), any()))
                .thenReturn(Collections.singletonList(row), Collections.emptyList());
        when(instances.update4FailByInstanceIdAndOriginStatus(eq(40L), eq(InstanceStatus.RUNNING.getV()),
                eq(InstanceStatus.FAILED.getV()), anyString(), anyLong(), any())).thenReturn(1);
    }

    private WorkerInfo aliveWorker() {
        WorkerInfo worker = new WorkerInfo();
        worker.setAddress(TRACKER_ADDRESS);
        worker.setProtocol("HTTP");
        // 心跳刚刚到达，未超时
        worker.setLastActiveTime(System.currentTimeMillis());
        return worker;
    }

    private void mockProbeReply(AskResponse response) {
        when(transport.ask(eq("HTTP"), any(), any(ServerQueryInstanceStatusReq.class), eq(AskResponse.class)))
                .thenReturn(CompletableFuture.completedFuture(response));
    }

    @Test
    void heartbeatTimeoutSkipsProbeAndRedispatchesWithExclusion() {
        // Worker 失联（查不到 WorkerInfo）：跳过探测，直接重派并排除原地址
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.empty());
        checker.checkRunningInstance();
        verify(dispatch).redispatchAsyncExcludingWorker(40L, InstanceStatus.RUNNING.getV(), TRACKER_ADDRESS);
        verify(transport, never()).ask(any(), any(), any(), any());
        verify(instances, never()).update4FailByInstanceIdAndOriginStatus(anyLong(), anyInt(), anyInt(), anyString(), anyLong(), any());
    }

    @Test
    void heartbeatTimeoutWithoutRetryBudgetFailsWithWorkerLost() {
        // Worker 心跳超时（lastActiveTime 远早于阈值）：跳过探测，失败文案使用 WORKER_LOST
        job.setInstanceRetryNum(0);
        WorkerInfo lostWorker = aliveWorker();
        lostWorker.setLastActiveTime(1L);
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(lostWorker));
        checker.checkRunningInstance();
        verify(transport, never()).ask(any(), any(), any(), any());
        verify(instances).update4FailByInstanceIdAndOriginStatus(eq(40L), eq(InstanceStatus.RUNNING.getV()),
                eq(InstanceStatus.FAILED.getV()), eq(SystemInstanceResult.WORKER_LOST), anyLong(), any());
        verify(completion).processFinishedInstance(40L, null, InstanceStatus.FAILED, SystemInstanceResult.WORKER_LOST);
        verify(dispatch, never()).redispatchAsyncExcludingWorker(anyLong(), anyInt(), any());
    }

    @Test
    void stalledReportWithAliveWorkerProbesAndRefreshesWhenStillRunning() {
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(aliveWorker()));
        InstanceDetail detail = new InstanceDetail();
        detail.setStatus(InstanceStatus.RUNNING.getV());
        mockProbeReply(AskResponse.succeed(detail));

        checker.checkRunningInstance();

        // 探测确认仍运行：刷新上报时间（使用注入的固定时钟），不重派、不置失败
        verify(instances).refreshReportTimeByInstanceIdAndStatus(eq(NOW), eq(new Date(NOW)), eq(40L), eq(InstanceStatus.RUNNING.getV()));
        verify(dispatch, never()).redispatchAsyncExcludingWorker(anyLong(), anyInt(), any());
        verify(instances, never()).update4FailByInstanceIdAndOriginStatus(anyLong(), anyInt(), anyInt(), anyString(), anyLong(), any());
    }

    @Test
    void stalledReportProbeNotFoundRedispatchesWithExclusion() {
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(aliveWorker()));
        mockProbeReply(AskResponse.failed("can't find TaskTracker"));

        checker.checkRunningInstance();

        verify(dispatch).redispatchAsyncExcludingWorker(40L, InstanceStatus.RUNNING.getV(), TRACKER_ADDRESS);
        verify(instances, never()).refreshReportTimeByInstanceIdAndStatus(anyLong(), any(), anyLong(), anyInt());
    }

    @Test
    void stalledReportProbeExceptionRedispatchesWithExclusion() {
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(aliveWorker()));
        when(transport.ask(eq("HTTP"), any(), any(ServerQueryInstanceStatusReq.class), eq(AskResponse.class)))
                .thenThrow(new RemotingException("connection reset"));

        checker.checkRunningInstance();

        verify(dispatch).redispatchAsyncExcludingWorker(40L, InstanceStatus.RUNNING.getV(), TRACKER_ADDRESS);
    }

    @Test
    void stalledReportProbeTimeoutRedispatchesWithExclusion() {
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(aliveWorker()));
        // 永不完成的 future：探测超时（100ms）后进入原重试路径
        when(transport.ask(eq("HTTP"), any(), any(ServerQueryInstanceStatusReq.class), eq(AskResponse.class)))
                .thenReturn(new CompletableFuture<>());

        checker.checkRunningInstance();

        verify(dispatch).redispatchAsyncExcludingWorker(40L, InstanceStatus.RUNNING.getV(), TRACKER_ADDRESS);
    }

    @Test
    void stalledReportProbeFailureWithoutBudgetFailsWithReportTimeout() {
        job.setInstanceRetryNum(0);
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(aliveWorker()));
        mockProbeReply(AskResponse.failed("can't find TaskTracker"));

        checker.checkRunningInstance();

        verify(instances).update4FailByInstanceIdAndOriginStatus(eq(40L), eq(InstanceStatus.RUNNING.getV()),
                eq(InstanceStatus.FAILED.getV()), eq(SystemInstanceResult.REPORT_TIMEOUT), anyLong(), any());
        verify(completion).processFinishedInstance(40L, null, InstanceStatus.FAILED, SystemInstanceResult.REPORT_TIMEOUT);
    }

    @Test
    void probeHappensAtMostOncePerInstancePerCheckCycle() {
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(aliveWorker()));
        InstanceDetail detail = new InstanceDetail();
        detail.setStatus(InstanceStatus.RUNNING.getV());
        mockProbeReply(AskResponse.succeed(detail));
        // 模拟 DB 尚未刷新：第二轮查询仍返回同一实例，本周期不得重复探测
        when(instances.selectBriefInfoByAppIdInAndStatusAndGmtModifiedBefore(anyList(),
                eq(InstanceStatus.RUNNING.getV()), any(Date.class), any()))
                .thenReturn(Collections.singletonList(row), Collections.singletonList(row), Collections.emptyList());

        checker.checkRunningInstance();

        verify(transport, times(1)).ask(any(), any(), any(), any());
    }

    @Test
    void concurrentTerminalWriteSkipsFailureHandling() {
        // 与手动停止竞争：条件更新返回 0（手动停止先生效），不得重复触发完成处理
        job.setInstanceRetryNum(0);
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.empty());
        when(instances.update4FailByInstanceIdAndOriginStatus(eq(40L), eq(InstanceStatus.RUNNING.getV()),
                eq(InstanceStatus.FAILED.getV()), anyString(), anyLong(), any())).thenReturn(0);

        checker.checkRunningInstance();

        verify(completion, never()).processFinishedInstance(anyLong(), any(), any(), any());
    }
}
