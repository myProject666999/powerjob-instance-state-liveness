package tech.powerjob.server.core.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tech.powerjob.common.SystemInstanceResult;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.model.InstanceDetail;
import tech.powerjob.common.request.ServerQueryInstanceStatusReq;
import tech.powerjob.common.response.AskResponse;
import tech.powerjob.remote.framework.base.URL;
import tech.powerjob.server.common.Holder;
import tech.powerjob.common.enums.SwitchableStatus;
import tech.powerjob.server.common.module.WorkerInfo;
import tech.powerjob.server.core.DispatchService;
import tech.powerjob.server.core.instance.InstanceManager;
import tech.powerjob.server.core.workflow.WorkflowInstanceManager;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.JobInfoDO;
import tech.powerjob.server.persistence.remote.model.WorkflowInfoDO;
import tech.powerjob.server.persistence.remote.model.WorkflowInstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.brief.BriefInstanceInfo;
import tech.powerjob.server.persistence.remote.repository.*;
import tech.powerjob.server.remote.transporter.TransportService;
import tech.powerjob.server.remote.transporter.impl.ServerURLFactory;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 定时状态检查
 *
 * @author tjq
 * @since 2020/4/7
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InstanceStatusCheckService {

    private static final int MAX_BATCH_NUM_APP = 10;
    private static final int MAX_BATCH_NUM_INSTANCE = 3000;
    private static final int MAX_BATCH_UPDATE_NUM = 500;
    private static final long DISPATCH_TIMEOUT_MS = 30000;
    private static final long RECEIVE_TIMEOUT_MS = 60000;
    private static final long WORKFLOW_WAITING_TIMEOUT_MS = 60000;

    /**
     * 探测队列长度上限，超过后本周期放弃探测（不阻塞检查线程）
     */
    private static final int PROBE_QUEUE_CAPACITY = 1024;

    public static final long CHECK_INTERVAL = 10000;

    private final TransportService transportService;

    private final DispatchService dispatchService;

    private final InstanceManager instanceManager;

    private final WorkflowInstanceManager workflowInstanceManager;

    private final AppInfoRepository appInfoRepository;

    private final JobInfoRepository jobInfoRepository;

    private final InstanceInfoRepository instanceInfoRepository;

    private final WorkflowInfoRepository workflowInfoRepository;

    private final WorkflowInstanceInfoRepository workflowInstanceInfoRepository;

    private final WorkerClusterQueryService workerClusterQueryService;

    private final InstanceLivenessProperties livenessProperties;

    /**
     * 注入时钟，测试可替换为固定时钟（默认系统时钟）
     */
    private Clock clock = Clock.systemDefaultZone();

    /**
     * 探测线程池（限制并发，懒加载；测试可替换为同步执行器）
     */
    private volatile Executor probeExecutor;

    public void checkWorkflowInstance() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // 查询 DB 获取该 Server 需要负责的 AppGroup
        List<Long> allAppIds = appInfoRepository.listAppIdByCurrentServer(transportService.defaultProtocol().getAddress());
        if (CollectionUtils.isEmpty(allAppIds)) {
            log.info("[InstanceStatusChecker] current server has no app's job to check");
            return;
        }
        try {
            checkWorkflowInstance(allAppIds);
        } catch (Exception e) {
            log.error("[InstanceStatusChecker] WorkflowInstance status check failed.", e);
        }
        log.info("[InstanceStatusChecker] WorkflowInstance status check using {}.", stopwatch.stop());
    }

    /**
     * 检查等待派发的实例
     * WAITING_DISPATCH 超时：写入时间轮但未调度前 server down
     */
    public void checkWaitingDispatchInstance() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // 查询 DB 获取该 Server 需要负责的 AppGroup
        List<Long> allAppIds = appInfoRepository.listAppIdByCurrentServer(transportService.defaultProtocol().getAddress());
        if (CollectionUtils.isEmpty(allAppIds)) {
            log.info("[InstanceStatusChecker] current server has no app's job to check");
            return;
        }
        try {
            // 检查等待 WAITING_DISPATCH 状态的任务
            Lists.partition(allAppIds, MAX_BATCH_NUM_APP).forEach(this::handleWaitingDispatchInstance);
        } catch (Exception e) {
            log.error("[InstanceStatusChecker] WaitingDispatchInstance status check failed.", e);
        }
        log.info("[InstanceStatusChecker] WaitingDispatchInstance status check using {}.", stopwatch.stop());
    }

    /**
     * 检查等待 worker 接收的实例
     * WAITING_WORKER_RECEIVE 超时：由于网络错误导致 worker 未接受成功
     */
    public void checkWaitingWorkerReceiveInstance() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // 查询 DB 获取该 Server 需要负责的 AppGroup
        List<Long> allAppIds = appInfoRepository.listAppIdByCurrentServer(transportService.defaultProtocol().getAddress());
        if (CollectionUtils.isEmpty(allAppIds)) {
            log.info("[InstanceStatusChecker] current server has no app's job to check");
            return;
        }
        try {
            // 检查 WAITING_WORKER_RECEIVE 状态的任务
            Lists.partition(allAppIds, MAX_BATCH_NUM_APP).forEach(this::handleWaitingWorkerReceiveInstance);
        } catch (Exception e) {
            log.error("[InstanceStatusChecker] WaitingWorkerReceiveInstance status check failed.", e);
        }
        log.info("[InstanceStatusChecker] WaitingWorkerReceiveInstance status check using {}.", stopwatch.stop());
    }

    /**
     * 检查运行中的实例
     * RUNNING 超时：TaskTracker down，断开与 server 的心跳连接
     */
    public void checkRunningInstance() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // 查询 DB 获取该 Server 需要负责的 AppGroup
        List<Long> allAppIds = appInfoRepository.listAppIdByCurrentServer(transportService.defaultProtocol().getAddress());
        if (CollectionUtils.isEmpty(allAppIds)) {
            log.info("[InstanceStatusChecker] current server has no app's job to check");
            return;
        }
        try {
            // 检查 RUNNING 状态的任务（一定时间没收到 TaskTracker 的状态报告，视为失败）
            Lists.partition(allAppIds, MAX_BATCH_NUM_APP).forEach(this::handleRunningInstance);
        } catch (Exception e) {
            log.error("[InstanceStatusChecker] RunningInstance status check failed.", e);
        }
        log.info("[InstanceStatusChecker] RunningInstance status check using {}.", stopwatch.stop());
    }

    private void handleWaitingDispatchInstance(List<Long> appIds) {

        // 存在移除操作，需要重新创建集合，否则会导致外层抛出 NoSuchElementException: null
        List<Long> partAppIds = Lists.newArrayList(appIds);

        // 1. 检查等待 WAITING_DISPATCH 状态的任务
        long threshold = System.currentTimeMillis() - DISPATCH_TIMEOUT_MS;
        List<InstanceInfoDO> waitingDispatchInstances = instanceInfoRepository.findAllByAppIdInAndStatusAndExpectedTriggerTimeLessThan(partAppIds, InstanceStatus.WAITING_DISPATCH.getV(), threshold, PageRequest.of(0, MAX_BATCH_NUM_INSTANCE));
        while (!waitingDispatchInstances.isEmpty()) {
            List<Long> overloadAppIdList = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            // 按照 appId 分组处理，方便处理超载的逻辑
            Map<Long, List<InstanceInfoDO>> waitingDispatchInstancesMap = waitingDispatchInstances.stream().collect(Collectors.groupingBy(InstanceInfoDO::getAppId));
            for (Map.Entry<Long, List<InstanceInfoDO>> entry : waitingDispatchInstancesMap.entrySet()) {
                final Long currentAppId = entry.getKey();
                final List<InstanceInfoDO> currentAppWaitingDispatchInstances = entry.getValue();
                // collect job id
                Set<Long> jobIds = currentAppWaitingDispatchInstances.stream().map(InstanceInfoDO::getJobId).collect(Collectors.toSet());
                // query job info and map
                Map<Long, JobInfoDO> jobInfoMap = jobInfoRepository.findByIdIn(jobIds).stream().collect(Collectors.toMap(JobInfoDO::getId, e -> e));
                log.warn("[InstanceStatusChecker] find some instance in app({}) which is not triggered as expected: {}", currentAppId, currentAppWaitingDispatchInstances.stream().map(InstanceInfoDO::getInstanceId).collect(Collectors.toList()));
                final Holder<Boolean> overloadFlag = new Holder<>(false);
                // 先这么简单处理没问题，毕竟只有这一个地方用了 parallelStream
                currentAppWaitingDispatchInstances.parallelStream().forEach(instance -> {
                    if (overloadFlag.get()) {
                        // 直接忽略
                        return;
                    }
                    Optional<JobInfoDO> jobInfoOpt = Optional.ofNullable(jobInfoMap.get(instance.getJobId()));
                    if (jobInfoOpt.isPresent()) {
                        // 处理等待派发的任务没有必要再重置一次状态，减少 io 次数
                        dispatchService.dispatch(jobInfoOpt.get(), instance.getInstanceId(), Optional.of(instance), Optional.of(overloadFlag));
                    } else {
                        log.warn("[InstanceStatusChecker] can't find job by jobId[{}], so redispatch failed, failed instance: {}", instance.getJobId(), instance);
                        final Optional<InstanceInfoDO> opt = instanceInfoRepository.findById(instance.getId());
                        opt.ifPresent(instanceInfoDO -> updateFailedInstance(instanceInfoDO, SystemInstanceResult.CAN_NOT_FIND_JOB_INFO, InstanceStatus.WAITING_DISPATCH.getV()));
                    }
                });
                threshold = System.currentTimeMillis() - DISPATCH_TIMEOUT_MS;
                if (overloadFlag.get()) {
                    overloadAppIdList.add(currentAppId);
                }
            }
            log.info("[InstanceStatusChecker] process {} task,use {} ms", waitingDispatchInstances.size(), System.currentTimeMillis() - startTime);
            if (!overloadAppIdList.isEmpty()) {
                log.warn("[InstanceStatusChecker] app[{}] is overload, so skip check waiting dispatch instance", overloadAppIdList);
                partAppIds.removeAll(overloadAppIdList);
            }
            if (partAppIds.isEmpty()) {
                break;
            }
            waitingDispatchInstances = instanceInfoRepository.findAllByAppIdInAndStatusAndExpectedTriggerTimeLessThan(partAppIds, InstanceStatus.WAITING_DISPATCH.getV(), threshold, PageRequest.of(0, MAX_BATCH_NUM_INSTANCE));
        }

    }

    private void handleWaitingWorkerReceiveInstance(List<Long> partAppIds) {
        // 2. 检查 WAITING_WORKER_RECEIVE 状态的任务
        long threshold = System.currentTimeMillis() - RECEIVE_TIMEOUT_MS;
        List<BriefInstanceInfo> waitingWorkerReceiveInstances = instanceInfoRepository.selectBriefInfoByAppIdInAndStatusAndActualTriggerTimeLessThan(partAppIds, InstanceStatus.WAITING_WORKER_RECEIVE.getV(), threshold, PageRequest.of(0, MAX_BATCH_NUM_INSTANCE));
        while (!waitingWorkerReceiveInstances.isEmpty()) {
            log.warn("[InstanceStatusChecker] find some instance didn't receive any reply from worker, try to redispatch: {}", waitingWorkerReceiveInstances.stream().map(BriefInstanceInfo::getInstanceId).collect(Collectors.toList()));
            final List<List<BriefInstanceInfo>> partitions = Lists.partition(waitingWorkerReceiveInstances, MAX_BATCH_UPDATE_NUM);
            for (List<BriefInstanceInfo> partition : partitions) {
                dispatchService.redispatchBatchAsyncLockFree(partition.stream().map(BriefInstanceInfo::getInstanceId).collect(Collectors.toList()), InstanceStatus.WAITING_WORKER_RECEIVE.getV());
            }
            // 重新查询
            threshold = System.currentTimeMillis() - RECEIVE_TIMEOUT_MS;
            waitingWorkerReceiveInstances = instanceInfoRepository.selectBriefInfoByAppIdInAndStatusAndActualTriggerTimeLessThan(partAppIds, InstanceStatus.WAITING_WORKER_RECEIVE.getV(), threshold, PageRequest.of(0, MAX_BATCH_NUM_INSTANCE));
        }
    }

    /**
     * 检查 RUNNING 状态的实例。
     * 普通任务区分两种失联：
     * <ul>
     *   <li>Worker 心跳超时（查不到 WorkerInfo 或心跳超时）：跳过探测，直接进入原重试/失败路径，
     *       失败文案使用 {@link SystemInstanceResult#WORKER_LOST} 以区别于上报停滞。</li>
     *   <li>心跳正常但上报停滞：不直接重派，先用 ServerQueryInstanceStatusReq 探测原 TaskTracker。
     *       仍运行则刷新上报时间；不存在或无答复才进入原重试/失败路径（{@link SystemInstanceResult#REPORT_TIMEOUT}）。
     *       探测每实例每检查周期最多一次，异步执行不阻塞检查线程，并发受限。</li>
     * </ul>
     * 注意：探测失败不能证明旧任务已停，重派后旧 TaskTracker 可能仍在运行，存在重复执行风险，
     * 本机制不是严格单次执行保证。
     */
    private void handleRunningInstance(List<Long> partAppIds) {
        // 3. 检查 RUNNING 状态的任务（一定时间没收到 TaskTracker 的状态报告，视为失败）
        long threshold = clock.millis() - livenessProperties.getReportTimeoutMs();
        List<BriefInstanceInfo> failedInstances = instanceInfoRepository.selectBriefInfoByAppIdInAndStatusAndGmtModifiedBefore(partAppIds, InstanceStatus.RUNNING.getV(), new Date(threshold), PageRequest.of(0, MAX_BATCH_NUM_INSTANCE));
        // 每个检查周期内，每个实例最多探测一次
        Set<Long> probedInstanceIds = new HashSet<>();
        while (!failedInstances.isEmpty()) {
            // collect job id
            Set<Long> jobIds = failedInstances.stream().map(BriefInstanceInfo::getJobId).collect(Collectors.toSet());
            // query job info and map
            Map<Long, JobInfoDO> jobInfoMap = jobInfoRepository.findByIdIn(jobIds).stream().collect(Collectors.toMap(JobInfoDO::getId, e -> e));
            log.warn("[InstanceStatusCheckService] find some instances have not received status report for a long time : {}", failedInstances.stream().map(BriefInstanceInfo::getInstanceId).collect(Collectors.toList()));
            for (BriefInstanceInfo instance : failedInstances) {
                // 本周期已探测过的实例直接跳过（探测结果异步生效，下周期再评估）
                if (probedInstanceIds.contains(instance.getInstanceId())) {
                    continue;
                }
                Optional<JobInfoDO> jobInfoOpt = Optional.ofNullable(jobInfoMap.get(instance.getJobId()));
                if (!jobInfoOpt.isPresent()) {
                    final Optional<InstanceInfoDO> opt = instanceInfoRepository.findById(instance.getId());
                    opt.ifPresent(e -> updateFailedInstance(e, SystemInstanceResult.REPORT_TIMEOUT, InstanceStatus.RUNNING.getV()));
                    continue;
                }
                JobInfoDO jobInfo = jobInfoOpt.get();
                TimeExpressionType timeExpressionType = TimeExpressionType.of(jobInfo.getTimeExpressionType());
                SwitchableStatus switchableStatus = SwitchableStatus.of(jobInfo.getStatus());
                // 如果任务已关闭，则不进行重试，将任务置为失败即可；秒级任务也直接置为失败，由派发器重新调度
                if (switchableStatus != SwitchableStatus.ENABLE || TimeExpressionType.FREQUENT_TYPES.contains(timeExpressionType.getV())) {
                    final Optional<InstanceInfoDO> opt = instanceInfoRepository.findById(instance.getId());
                    opt.ifPresent(e -> updateFailedInstance(e, SystemInstanceResult.REPORT_TIMEOUT, InstanceStatus.RUNNING.getV()));
                    continue;
                }
                // 普通任务：通过 WorkerInfo.timeout() 区分 Worker 心跳超时 与 心跳正常但上报停滞
                Optional<WorkerInfo> workerInfoOpt = workerClusterQueryService.getWorkerInfoByAddress(instance.getAppId(), instance.getTaskTrackerAddress());
                boolean heartbeatTimeout = !workerInfoOpt.isPresent() || workerInfoOpt.get().timeout(livenessProperties.getHeartbeatTimeoutMs());
                if (heartbeatTimeout) {
                    // Worker 失联：跳过探测，直接进入原重试/失败路径
                    retryOrFailRunningInstance(instance, jobInfo, SystemInstanceResult.WORKER_LOST);
                    continue;
                }
                // 心跳正常但上报停滞：先探测，不直接重派
                probedInstanceIds.add(instance.getInstanceId());
                submitProbe(instance, jobInfo, workerInfoOpt.get());
            }
            threshold = clock.millis() - livenessProperties.getReportTimeoutMs();
            failedInstances = instanceInfoRepository.selectBriefInfoByAppIdInAndStatusAndGmtModifiedBefore(partAppIds, InstanceStatus.RUNNING.getV(), new Date(threshold), PageRequest.of(0, MAX_BATCH_NUM_INSTANCE));
            // 剩余的实例本周期都已探测过（结果异步生效），退出避免空转
            boolean allProbed = failedInstances.stream().allMatch(i -> probedInstanceIds.contains(i.getInstanceId()));
            if (allProbed) {
                break;
            }
        }

    }

    /**
     * 提交一次异步探测（不阻塞检查线程；线程池饱和时放弃，下个检查周期重试）
     */
    private void submitProbe(BriefInstanceInfo instance, JobInfoDO jobInfo, WorkerInfo workerInfo) {
        try {
            probeExecutor().execute(() -> probeAndHandle(instance, jobInfo, workerInfo));
        } catch (RejectedExecutionException e) {
            log.warn("[InstanceStatusChecker] probe executor is saturated, skip probing instance[{}] in this check cycle", instance.getInstanceId());
        }
    }

    /**
     * 探测原 TaskTracker 并按结果处理：
     * 仍运行 -> 刷新上报时间，本周期不再处理；
     * 不存在/无答复/探测异常 -> 进入原重试、失败路径。
     * 探测失败不能证明旧任务已停，重派后存在重复执行风险。
     */
    private void probeAndHandle(BriefInstanceInfo instance, JobInfoDO jobInfo, WorkerInfo workerInfo) {
        Long instanceId = instance.getInstanceId();
        try {
            ServerQueryInstanceStatusReq req = new ServerQueryInstanceStatusReq(instanceId, null);
            URL url = ServerURLFactory.queryInstance2Worker(workerInfo.getAddress());
            AskResponse askResponse = transportService.ask(workerInfo.getProtocol(), url, req, AskResponse.class)
                    .toCompletableFuture()
                    .get(livenessProperties.getProbeTimeoutMs(), TimeUnit.MILLISECONDS);
            if (askResponse != null && askResponse.isSuccess()) {
                InstanceDetail detail = askResponse.getData(InstanceDetail.class);
                if (detail != null && detail.getStatus() != null && InstanceStatus.GENERALIZED_RUNNING_STATUS.contains(detail.getStatus())) {
                    // 旧 TaskTracker 确认仍在运行：条件刷新上报时间（实例可能已被并发处理，仅在仍为 RUNNING 时刷新）
                    long now = clock.millis();
                    int updated = instanceInfoRepository.refreshReportTimeByInstanceIdAndStatus(now, new Date(now), instanceId, InstanceStatus.RUNNING.getV());
                    log.info("[InstanceStatusChecker] probe instance[{}] on worker[{}]: still running, refresh report time (updated={})", instanceId, workerInfo.getAddress(), updated);
                    return;
                }
            }
            log.warn("[InstanceStatusChecker] probe instance[{}] on worker[{}]: TaskTracker not found or not running, enter retry/fail path", instanceId, workerInfo.getAddress());
        } catch (Exception e) {
            log.warn("[InstanceStatusChecker] probe instance[{}] on worker[{}] failed: {}, enter retry/fail path", instanceId, workerInfo.getAddress(), e.toString());
        }
        retryOrFailRunningInstance(instance, jobInfo, SystemInstanceResult.REPORT_TIMEOUT);
    }

    /**
     * 原重试/失败路径：重试预算内重派（本次派发排除原地址，后续派发自动恢复候选），否则置为失败
     */
    private void retryOrFailRunningInstance(BriefInstanceInfo instance, JobInfoDO jobInfo, String result) {
        // runningTimes includes the initial attempt; allow the configured number of retries.
        if (instance.getRunningTimes() <= jobInfo.getInstanceRetryNum()) {
            dispatchService.redispatchAsyncExcludingWorker(instance.getInstanceId(), InstanceStatus.RUNNING.getV(), instance.getTaskTrackerAddress());
        } else {
            final Optional<InstanceInfoDO> opt = instanceInfoRepository.findById(instance.getId());
            opt.ifPresent(e -> updateFailedInstance(e, result, InstanceStatus.RUNNING.getV()));
        }
    }

    private Executor probeExecutor() {
        if (probeExecutor == null) {
            synchronized (this) {
                if (probeExecutor == null) {
                    ThreadPoolExecutor executor = new ThreadPoolExecutor(
                            livenessProperties.getProbeConcurrency(), livenessProperties.getProbeConcurrency(),
                            60L, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>(PROBE_QUEUE_CAPACITY),
                            new ThreadFactoryBuilder().setNameFormat("instance-status-prober-%d").setDaemon(true).build(),
                            new ThreadPoolExecutor.AbortPolicy());
                    executor.allowCoreThreadTimeOut(true);
                    probeExecutor = executor;
                }
            }
        }
        return probeExecutor;
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }

    @VisibleForTesting
    void setProbeExecutor(Executor probeExecutor) {
        this.probeExecutor = probeExecutor;
    }

    /**
     * 定期检查工作流实例状态
     * 此处仅检查并重试长时间处于 WAITING 状态的工作流实例，工作流的其他可靠性由 Instance 支撑，即子任务失败会反馈会 WorkflowInstance
     *
     * @param allAppIds 本系统所承担的所有 appIds
     */
    private void checkWorkflowInstance(List<Long> allAppIds) {

        // 重试长时间处于 WAITING 状态的工作流实例
        long threshold = System.currentTimeMillis() - WORKFLOW_WAITING_TIMEOUT_MS;
        Lists.partition(allAppIds, MAX_BATCH_NUM_APP).forEach(partAppIds -> {
            List<WorkflowInstanceInfoDO> waitingWfInstanceList = workflowInstanceInfoRepository.findByAppIdInAndStatusAndExpectedTriggerTimeLessThan(partAppIds, WorkflowInstanceStatus.WAITING.getV(), threshold);
            if (!CollectionUtils.isEmpty(waitingWfInstanceList)) {

                List<Long> wfInstanceIds = waitingWfInstanceList.stream().map(WorkflowInstanceInfoDO::getWfInstanceId).collect(Collectors.toList());
                log.warn("[WorkflowInstanceChecker] wfInstance({}) is not started as expected, oms try to restart these workflowInstance.", wfInstanceIds);

                waitingWfInstanceList.forEach(wfInstance -> {
                    Optional<WorkflowInfoDO> workflowOpt = workflowInfoRepository.findById(wfInstance.getWorkflowId());
                    workflowOpt.ifPresent(workflowInfo -> {
                        workflowInstanceManager.start(workflowInfo, wfInstance.getWfInstanceId());
                        log.info("[Workflow-{}|{}] restart workflowInstance successfully~", workflowInfo.getId(), wfInstance.getWfInstanceId());
                    });
                });
            }
        });
    }

    /**
     * 处理失败的任务实例。
     * 终态写入使用条件更新：与手动停止等并发终态写入竞争时先到先得（如手动停止已生效则本次放弃），
     * 保证终态不被改写、收尾（日志/告警/工作流推进）只触发一次。
     */
    private void updateFailedInstance(InstanceInfoDO instance, String result, int originStatus) {

        log.warn("[InstanceStatusChecker] instance[{}] failed due to {}, instanceInfo: {}", instance.getInstanceId(), result, instance);

        long now = clock.millis();
        int updated = instanceInfoRepository.update4FailByInstanceIdAndOriginStatus(instance.getInstanceId(), originStatus, InstanceStatus.FAILED.getV(), result, now, new Date(now));
        if (updated == 0) {
            log.warn("[InstanceStatusChecker] instance[{}] status has been changed by other thread (maybe stopped manually), discard this failure handling.", instance.getInstanceId());
            return;
        }
        // 同步内存中的实体状态，便于调用方断言与后续日志输出
        instance.setStatus(InstanceStatus.FAILED.getV());
        instance.setFinishedTime(now);
        instance.setGmtModified(new Date(now));
        instance.setResult(result);

        instanceManager.processFinishedInstance(instance.getInstanceId(), instance.getWfInstanceId(), InstanceStatus.FAILED, result);
    }
}
