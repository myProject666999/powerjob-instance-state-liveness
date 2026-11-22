package tech.powerjob.server.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.ProcessorType;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.request.ServerScheduleJobReq;
import tech.powerjob.server.common.module.WorkerInfo;
import tech.powerjob.server.core.instance.InstanceManager;
import tech.powerjob.server.core.instance.InstanceMetadataService;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.JobInfoDO;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;
import tech.powerjob.server.remote.transporter.TransportService;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;
import tech.powerjob.server.remote.worker.selector.TaskTrackerSelectorService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 失联重派的一次性 Worker 排除测试：本次重派排除原地址，后续派发自动恢复候选，不永久屏蔽。
 */
class DispatchServiceExclusionTest {

    private static final String OLD_WORKER = "worker1:27777";
    private static final String NEW_WORKER = "worker2:27777";

    private final TransportService transport = mock(TransportService.class);
    private final WorkerClusterQueryService workers = mock(WorkerClusterQueryService.class);
    private final InstanceManager instanceManager = mock(InstanceManager.class);
    private final InstanceMetadataService metadata = mock(InstanceMetadataService.class);
    private final InstanceInfoRepository instances = mock(InstanceInfoRepository.class);
    private final TaskTrackerSelectorService selector = mock(TaskTrackerSelectorService.class);

    private DispatchService dispatchService;
    private JobInfoDO job;
    private InstanceInfoDO instance;
    private WorkerInfo oldWorker;
    private WorkerInfo newWorker;

    @BeforeEach
    void setUp() {
        dispatchService = new DispatchService(transport, workers, instanceManager, metadata, instances, selector);

        job = new JobInfoDO();
        job.setId(30L); job.setAppId(10L);
        job.setMaxInstanceNum(0); job.setMaxWorkerCount(0);
        job.setConcurrency(1); job.setTaskRetryNum(0);
        job.setTimeExpressionType(TimeExpressionType.API.getV());
        job.setExecuteType(ExecuteType.STANDALONE.getV());
        job.setProcessorType(ProcessorType.BUILT_IN.getV());

        instance = new InstanceInfoDO();
        instance.setId(20L); instance.setAppId(10L); instance.setJobId(30L); instance.setInstanceId(40L);
        instance.setStatus(InstanceStatus.WAITING_DISPATCH.getV());

        oldWorker = worker(OLD_WORKER);
        newWorker = worker(NEW_WORKER);
        // 每次返回全新的可变列表，模拟实时查询
        when(workers.geAvailableWorkers(job)).thenAnswer(inv -> new ArrayList<>(Arrays.asList(oldWorker, newWorker)));
        // 选择器始终选择候选列表中的第一台，便于断言候选集合
        when(selector.select(eq(job), eq(instance), anyList())).thenAnswer(inv -> inv.<List<WorkerInfo>>getArgument(2).get(0));
    }

    private WorkerInfo worker(String address) {
        WorkerInfo workerInfo = new WorkerInfo();
        workerInfo.setAddress(address);
        workerInfo.setProtocol("HTTP");
        return workerInfo;
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<List<WorkerInfo>> workerListCaptor() {
        return ArgumentCaptor.forClass(List.class);
    }

    @Test
    void redispatchExcludesPreviousWorkerOnceThenRestoresCandidacy() {
        // 失联重派：记录状态翻转与一次性排除地址
        dispatchService.redispatchAsyncExcludingWorker(40L, InstanceStatus.RUNNING.getV(), OLD_WORKER);
        verify(instances).updateStatusAndGmtModifiedByInstanceIdAndOriginStatus(eq(40L),
                eq(InstanceStatus.RUNNING.getV()), eq(InstanceStatus.WAITING_DISPATCH.getV()), any());

        // 本次派发：原 Worker 被排除，任务派往其他 Worker
        dispatchService.dispatch(job, 40L, Optional.of(instance), Optional.empty());
        ArgumentCaptor<List<WorkerInfo>> captor = workerListCaptor();
        verify(selector).select(eq(job), eq(instance), captor.capture());
        assertEquals(Arrays.asList(NEW_WORKER), captor.getValue().stream().map(WorkerInfo::getAddress).collect(Collectors.toList()));
        verify(transport).tell(eq("HTTP"), argThat(url -> NEW_WORKER.equals(url.getAddress().toFullAddress())), any(ServerScheduleJobReq.class));

        // 后续派发：排除已消费，原 Worker 自动恢复候选资格（不永久屏蔽）
        dispatchService.dispatch(job, 40L, Optional.of(instance), Optional.empty());
        verify(selector, times(2)).select(eq(job), eq(instance), captor.capture());
        assertEquals(Arrays.asList(OLD_WORKER, NEW_WORKER), captor.getValue().stream().map(WorkerInfo::getAddress).collect(Collectors.toList()));
        verify(transport).tell(eq("HTTP"), argThat(url -> OLD_WORKER.equals(url.getAddress().toFullAddress())), any(ServerScheduleJobReq.class));
    }

    @Test
    void exclusionWithoutOtherWorkerSkipsThisRoundAndRecoversNextRound() {
        // 集群只剩原 Worker：本轮放弃派发（实例保持等待派发），下轮恢复候选
        when(workers.geAvailableWorkers(job)).thenAnswer(inv -> new ArrayList<>(Arrays.asList(oldWorker)));
        dispatchService.redispatchAsyncExcludingWorker(40L, InstanceStatus.RUNNING.getV(), OLD_WORKER);

        dispatchService.dispatch(job, 40L, Optional.of(instance), Optional.empty());
        verify(selector, never()).select(any(), any(), anyList());
        verify(transport, never()).tell(any(), any(), any());

        // 下一轮：排除已失效，原 Worker 重新成为候选
        dispatchService.dispatch(job, 40L, Optional.of(instance), Optional.empty());
        verify(selector).select(eq(job), eq(instance), anyList());
        verify(transport).tell(eq("HTTP"), argThat(url -> OLD_WORKER.equals(url.getAddress().toFullAddress())), any(ServerScheduleJobReq.class));
    }

    @Test
    void plainRedispatchDoesNotExcludeAnything() {
        dispatchService.redispatchAsync(40L, InstanceStatus.RUNNING.getV());
        dispatchService.dispatch(job, 40L, Optional.of(instance), Optional.empty());
        ArgumentCaptor<List<WorkerInfo>> captor = workerListCaptor();
        verify(selector).select(eq(job), eq(instance), captor.capture());
        assertEquals(Arrays.asList(OLD_WORKER, NEW_WORKER), captor.getValue().stream().map(WorkerInfo::getAddress).collect(Collectors.toList()));
    }
}
