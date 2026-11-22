package tech.powerjob.server.core.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.context.ApplicationContext;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.request.ServerStopInstanceReq;
import tech.powerjob.common.request.TaskTrackerReportInstanceStatusReq;
import tech.powerjob.server.common.module.WorkerInfo;
import tech.powerjob.server.common.utils.SpringUtils;
import tech.powerjob.server.core.alarm.AlarmCenter;
import tech.powerjob.server.core.service.UserService;
import tech.powerjob.server.core.workflow.WorkflowInstanceManager;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.JobInfoDO;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;
import tech.powerjob.server.remote.transporter.TransportService;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;

import java.util.Collections;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 普通任务实例的终态保护与状态迁移测试（纯 Mockito，不起数据库）
 */
class InstanceManagerTerminalStateTest {

    private static final String TRACKER_ADDRESS = "worker1:27777";

    private final InstanceInfoRepository instances = mock(InstanceInfoRepository.class);
    private final InstanceMetadataService metadata = mock(InstanceMetadataService.class);
    private final WorkflowInstanceManager workflowInstanceManager = mock(WorkflowInstanceManager.class);
    private final WorkerClusterQueryService workers = mock(WorkerClusterQueryService.class);
    private final TransportService transport = mock(TransportService.class);
    private final AlarmCenter alarmCenter = mock(AlarmCenter.class);

    private InstanceManager manager;
    private InstanceInfoDO instance;
    private JobInfoDO job;

    @BeforeEach
    void setUp() throws Exception {
        // alert() 通过 SpringUtils 获取 UserService，单测环境注入 mock 上下文
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        UserService userService = mock(UserService.class);
        when(applicationContext.getBean(UserService.class)).thenReturn(userService);
        when(userService.fetchNotifyUserList(any())).thenReturn(Collections.emptyList());
        new SpringUtils().setApplicationContext(applicationContext);

        manager = new InstanceManager(alarmCenter, mock(InstanceLogService.class), metadata, instances, workflowInstanceManager, workers);
        manager.setTransportService(transport);

        job = new JobInfoDO();
        job.setId(30L); job.setAppId(10L);
        job.setTimeExpressionType(TimeExpressionType.API.getV());
        job.setInstanceRetryNum(1);

        instance = runningInstance();
        when(metadata.fetchJobInfoByInstanceId(40L)).thenReturn(job);
        when(instances.findByInstanceId(40L)).thenReturn(instance);
    }

    private InstanceInfoDO runningInstance() {
        InstanceInfoDO entity = new InstanceInfoDO();
        entity.setId(20L); entity.setAppId(10L); entity.setJobId(30L); entity.setInstanceId(40L);
        entity.setStatus(InstanceStatus.RUNNING.getV());
        entity.setRunningTimes(1L); entity.setLastReportTime(100L);
        entity.setTaskTrackerAddress(TRACKER_ADDRESS);
        return entity;
    }

    private TaskTrackerReportInstanceStatusReq report(long reportTime, InstanceStatus status, String sourceAddress) {
        TaskTrackerReportInstanceStatusReq req = new TaskTrackerReportInstanceStatusReq();
        req.setInstanceId(40L); req.setJobId(30L); req.setAppId(10L);
        req.setWfInstanceId(66L);
        req.setReportTime(reportTime);
        req.setInstanceStatus(status.getV());
        req.setSourceAddress(sourceAddress);
        req.setResult("fixture-result");
        return req;
    }

    private void verifyNoStateChange() {
        verify(instances, never()).saveAndFlush(any());
        verify(instances, never()).update4FinishByInstanceIdAndStatus(anyLong(), any(), anyLong(), anyInt(), any(), anyLong(), anyLong(), anyInt());
        verify(instances, never()).updateStatusChangeInfoByInstanceIdAndStatus(anyLong(), any(), anyLong(), anyInt(), anyLong(), anyInt());
        verify(workflowInstanceManager, never()).move(anyLong(), anyLong(), any(), any());
        verify(alarmCenter, never()).alarmFailed(any(), anyList());
    }

    @ParameterizedTest(name = "terminal {0} ignores late {1} report")
    @EnumSource(value = InstanceStatus.class, names = {"FAILED", "SUCCEED", "CANCELED", "STOPPED"})
    void terminalStateIgnoresLateTerminalReport(InstanceStatus terminal) throws Exception {
        instance.setStatus(terminal.getV());
        manager.updateStatus(report(200L, InstanceStatus.SUCCEED, TRACKER_ADDRESS));
        manager.updateStatus(report(300L, InstanceStatus.FAILED, TRACKER_ADDRESS));
        // 终态不可改写：状态保持、无数据库写入、不重复触发完成/告警/工作流推进
        assertEquals(terminal.getV(), instance.getStatus());
        verifyNoStateChange();
        verifyNoInteractions(transport);
    }

    @ParameterizedTest(name = "terminal {0} notifies stray TaskTracker to stop")
    @EnumSource(value = InstanceStatus.class, names = {"FAILED", "SUCCEED", "CANCELED", "STOPPED"})
    void terminalStateNotifiesStrayTrackerToStop(InstanceStatus terminal) throws Exception {
        instance.setStatus(terminal.getV());
        WorkerInfo worker = new WorkerInfo();
        worker.setAddress(TRACKER_ADDRESS); worker.setProtocol("HTTP");
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(worker));

        manager.updateStatus(report(200L, InstanceStatus.RUNNING, TRACKER_ADDRESS));

        // 核对来源确为原 TaskTracker 后，仅向该地址发送停止请求，不改写实例状态
        verify(transport).tell(eq("HTTP"), argThat(url -> TRACKER_ADDRESS.equals(url.getAddress().toFullAddress())), any(ServerStopInstanceReq.class));
        assertEquals(terminal.getV(), instance.getStatus());
        verifyNoStateChange();
    }

    @Test
    void reportFromOtherTaskTrackerIsDroppedWithoutStop() throws Exception {
        instance.setStatus(InstanceStatus.FAILED.getV());
        // 脑裂/旧地址之外的上报：直接丢弃，且绝不向别的地址发送停止请求
        manager.updateStatus(report(200L, InstanceStatus.RUNNING, "worker2:27777"));
        assertEquals(InstanceStatus.FAILED.getV(), instance.getStatus());
        verifyNoStateChange();
        verifyNoInteractions(transport);
        verifyNoInteractions(workers);
    }

    @Test
    void expiredReportIsDropped() throws Exception {
        // reportTime 不大于 lastReportTime 的过期上报直接丢弃
        manager.updateStatus(report(100L, InstanceStatus.RUNNING, TRACKER_ADDRESS));
        verifyNoStateChange();
    }

    @Test
    void concurrentFinishIsProcessedOnlyOnce() throws Exception {
        // 模拟两个线程并发收尾：都读到 RUNNING，条件更新只有一个成功
        when(instances.findByInstanceId(40L)).thenAnswer(inv -> runningInstance());
        when(instances.update4FinishByInstanceIdAndStatus(anyLong(), any(), anyLong(),
                eq(InstanceStatus.SUCCEED.getV()), any(), anyLong(), eq(40L), eq(InstanceStatus.RUNNING.getV())))
                .thenReturn(1, 0);

        manager.updateStatus(report(200L, InstanceStatus.SUCCEED, TRACKER_ADDRESS));
        manager.updateStatus(report(200L, InstanceStatus.SUCCEED, TRACKER_ADDRESS));

        // 完成处理（工作流推进等）只触发一次
        verify(workflowInstanceManager, times(1)).move(66L, 40L, InstanceStatus.SUCCEED, "fixture-result");
    }

    @Test
    void failedReportWithinRetryBudgetResetsToWaitingDispatch() throws Exception {
        when(instances.updateStatusChangeInfoByInstanceIdAndStatus(anyLong(), any(), anyLong(), anyInt(), anyLong(), anyInt())).thenReturn(1);
        manager.updateStatus(report(200L, InstanceStatus.FAILED, TRACKER_ADDRESS));
        assertEquals(InstanceStatus.WAITING_DISPATCH.getV(), instance.getStatus());
        verify(instances).updateStatusChangeInfoByInstanceIdAndStatus(anyLong(), any(), anyLong(),
                eq(InstanceStatus.WAITING_DISPATCH.getV()), eq(40L), eq(InstanceStatus.RUNNING.getV()));
        verify(workflowInstanceManager, never()).move(anyLong(), anyLong(), any(), any());
        verify(alarmCenter, never()).alarmFailed(any(), anyList());
    }

    @Test
    void failedReportBeyondRetryBudgetFinishesAndAlertsOnce() throws Exception {
        instance.setRunningTimes(2L);
        when(instances.update4FinishByInstanceIdAndStatus(anyLong(), any(), anyLong(), anyInt(), any(), anyLong(), anyLong(), anyInt())).thenReturn(1);
        manager.updateStatus(report(200L, InstanceStatus.FAILED, TRACKER_ADDRESS));
        assertEquals(InstanceStatus.FAILED.getV(), instance.getStatus());
        verify(workflowInstanceManager, times(1)).move(66L, 40L, InstanceStatus.FAILED, "fixture-result");
        verify(alarmCenter, times(1)).alarmFailed(any(), anyList());
    }

    @Test
    void firstTerminalReportFromWaitingWorkerReceiveCountsTheAttempt() throws Exception {
        // 首次上报即终态（任务极速结束）同样计一次运行次数，否则失败重试预算永不消耗会导致无限重试
        instance.setStatus(InstanceStatus.WAITING_WORKER_RECEIVE.getV());
        when(instances.update4FinishByInstanceIdAndStatus(anyLong(), any(), anyLong(), anyInt(), any(), anyLong(), anyLong(), anyInt())).thenReturn(1);
        manager.updateStatus(report(200L, InstanceStatus.SUCCEED, TRACKER_ADDRESS));
        assertEquals(2L, instance.getRunningTimes().longValue());
        verify(instances).update4FinishByInstanceIdAndStatus(anyLong(), any(Date.class), eq(2L),
                eq(InstanceStatus.SUCCEED.getV()), eq("fixture-result"), anyLong(), eq(40L), eq(InstanceStatus.WAITING_WORKER_RECEIVE.getV()));
    }
}
