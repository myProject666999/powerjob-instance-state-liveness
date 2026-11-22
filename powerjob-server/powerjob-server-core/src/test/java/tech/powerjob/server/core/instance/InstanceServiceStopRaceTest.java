package tech.powerjob.server.core.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.SystemInstanceResult;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.request.ServerStopInstanceReq;
import tech.powerjob.server.common.module.WorkerInfo;
import tech.powerjob.server.core.DispatchService;
import tech.powerjob.server.core.uid.IdGenerateService;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;
import tech.powerjob.server.persistence.remote.repository.JobInfoRepository;
import tech.powerjob.server.remote.transporter.TransportService;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 手动停止与超时失败竞争的优先级测试：终态写入先到先得，后到者放弃，收尾只触发一次。
 */
class InstanceServiceStopRaceTest {

    private static final String TRACKER_ADDRESS = "worker1:27777";

    private final TransportService transport = mock(TransportService.class);
    private final InstanceManager instanceManager = mock(InstanceManager.class);
    private final InstanceInfoRepository instances = mock(InstanceInfoRepository.class);
    private final WorkerClusterQueryService workers = mock(WorkerClusterQueryService.class);

    private InstanceService instanceService;
    private InstanceInfoDO instance;

    @BeforeEach
    void setUp() {
        instanceService = new InstanceService(transport, mock(DispatchService.class), mock(IdGenerateService.class),
                instanceManager, mock(JobInfoRepository.class), instances, workers, mock(InstanceLogService.class));
        instance = new InstanceInfoDO();
        instance.setId(20L); instance.setAppId(10L); instance.setJobId(30L); instance.setInstanceId(40L);
        instance.setStatus(InstanceStatus.RUNNING.getV());
        instance.setTaskTrackerAddress(TRACKER_ADDRESS);
        when(instances.findByInstanceId(40L)).thenReturn(instance);
    }

    @Test
    void manualStopWinsWhenConditionalUpdateSucceeds() {
        when(instances.update4StopByInstanceIdAndOriginStatusIn(eq(40L), anyList(),
                eq(InstanceStatus.STOPPED.getV()), eq(SystemInstanceResult.STOPPED_BY_USER), anyLong(), any())).thenReturn(1);
        WorkerInfo worker = new WorkerInfo();
        worker.setAddress(TRACKER_ADDRESS); worker.setProtocol("HTTP");
        when(workers.getWorkerInfoByAddress(10L, TRACKER_ADDRESS)).thenReturn(Optional.of(worker));

        instanceService.stopInstance(10L, 40L);

        assertEquals(InstanceStatus.STOPPED.getV(), instance.getStatus());
        verify(instanceManager, times(1)).processFinishedInstance(40L, null, InstanceStatus.STOPPED, SystemInstanceResult.STOPPED_BY_USER);
        verify(transport).tell(eq("HTTP"), argThat(url -> TRACKER_ADDRESS.equals(url.getAddress().toFullAddress())), any(ServerStopInstanceReq.class));
    }

    @Test
    void manualStopYieldsWhenTerminalStateAlreadyWritten() {
        // 超时失败等并发终态写入已先生效：手动停止放弃，不覆盖终态、不重复收尾
        when(instances.update4StopByInstanceIdAndOriginStatusIn(eq(40L), anyList(),
                eq(InstanceStatus.STOPPED.getV()), eq(SystemInstanceResult.STOPPED_BY_USER), anyLong(), any())).thenReturn(0);

        assertThrows(IllegalArgumentException.class, () -> instanceService.stopInstance(10L, 40L));

        assertEquals(InstanceStatus.RUNNING.getV(), instance.getStatus(), "entity must not be mutated when the conditional update loses the race");
        verify(instanceManager, never()).processFinishedInstance(anyLong(), any(), any(), any());
        verifyNoInteractions(transport);
    }
}
