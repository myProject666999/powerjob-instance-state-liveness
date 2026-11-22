package tech.powerjob.server.core.instance;

import tech.powerjob.common.enums.InstanceStatus;

/**
 * 普通任务实例状态迁移表（纯函数，无副作用，不依赖任何外部状态）。
 *
 * <p>仅适用于普通任务（非秒级/固定频率任务）。工作流推进由
 * {@link InstanceManager#processFinishedInstance} 在终态首次写入成功后触发，
 * 秒级任务（FREQUENT）维持原有的独立处理分支，均不在本表管辖范围内。
 *
 * <p>迁移表（行=数据库当前状态，列=TaskTracker 上报状态）：
 * <pre>
 * current \ reported   RUNNING                 SUCCEED   FAILED            其他
 * WAITING_DISPATCH     APPLY                   FINISH    RETRY_OR_FINISH   IGNORE
 * WAITING_WORKER_RECV  APPLY(计次)             FINISH(计次) RETRY_OR_FINISH(计次) IGNORE
 * RUNNING              APPLY                   FINISH    RETRY_OR_FINISH   IGNORE
 * FAILED/SUCCEED/
 * CANCELED/STOPPED     STOP_STRAY_TRACKER      IGNORE_ON_TERMINAL（晚到回报只记录）
 * </pre>
 *
 * <p>核心约束：
 * <ul>
 *   <li>四个终态（失败/成功/取消/手动停止）不可被上报改写；晚到回报只记录日志，
 *       不重复触发完成、告警或工作流推进。</li>
 *   <li>递增运行次数只发生在「等待Worker接收」的首次有效上报（RUNNING/SUCCEED/FAILED）：
 *       其中 WAITING_WORKER_RECEIVE -&gt; RUNNING 是唯一的非终态递增迁移；
 *       首次上报即终态（任务极速结束）同样计一次，否则失败重试次数永不增长会导致无限重试。</li>
 *   <li>RUNNING -&gt; WAITING_DISPATCH 只能由服务端失联判定触发（见 InstanceStatusCheckService），
 *       上报路径永远不会把运行中的实例改回等待派发。</li>
 *   <li>终态下收到旧 TaskTracker 仍运行（RUNNING）的上报时，通知其停止（STOP_STRAY_TRACKER）。
 *       调用方需先核对上报来源确为当前记录的 TaskTracker 地址，且停止请求只发往该地址。</li>
 * </ul>
 *
 * @author tjq
 * @since 2026/11/23
 */
public final class InstanceStatusTransition {

    private InstanceStatusTransition() {
    }

    /**
     * 迁移决策
     */
    public enum Action {
        /**
         * 接受上报，按上报状态推进（含 RUNNING 存活刷新）
         */
        APPLY,
        /**
         * 接受终态上报（成功/失败），写入终态并进入收尾
         */
        FINISH,
        /**
         * 失败上报：重试预算内转 WAITING_DISPATCH，否则写入 FAILED 终态并收尾
         */
        RETRY_OR_FINISH,
        /**
         * 无意义上报（如 TaskTracker 上报了非运行侧状态），直接忽略
         */
        IGNORE,
        /**
         * 当前已是终态：晚到回报只记录日志，不改写状态、不重复收尾
         */
        IGNORE_ON_TERMINAL,
        /**
         * 当前已是终态，但旧 TaskTracker 仍上报运行中：通知该 TaskTracker 停止
         */
        STOP_STRAY_TRACKER
    }

    /**
     * 决策迁移动作
     *
     * @param current  数据库中的当前状态
     * @param reported TaskTracker 上报的状态
     * @return 迁移动作
     */
    public static Action decide(InstanceStatus current, InstanceStatus reported) {
        if (current == null || reported == null) {
            return Action.IGNORE;
        }
        switch (current) {
            case FAILED:
            case SUCCEED:
            case CANCELED:
            case STOPPED:
                // 终态不可改写；旧 TaskTracker 仍上报运行中则需要通知其停止
                return reported == InstanceStatus.RUNNING ? Action.STOP_STRAY_TRACKER : Action.IGNORE_ON_TERMINAL;
            case WAITING_DISPATCH:
            case WAITING_WORKER_RECEIVE:
            case RUNNING:
                switch (reported) {
                    case RUNNING:
                        return Action.APPLY;
                    case SUCCEED:
                        return Action.FINISH;
                    case FAILED:
                        return Action.RETRY_OR_FINISH;
                    default:
                        return Action.IGNORE;
                }
            default:
                return Action.IGNORE;
        }
    }

    /**
     * 该次上报是否需要递增运行次数。
     * 递增只发生在「等待Worker接收」状态的首次有效上报（RUNNING/SUCCEED/FAILED），
     * 与历史行为保持一致；首次上报即 FAILED 也必须计次，否则重试预算永不消耗，会导致无限重试。
     *
     * @param current  数据库中的当前状态
     * @param reported TaskTracker 上报的状态
     * @return true 表示 runningTimes 需要 +1
     */
    public static boolean countsAttempt(InstanceStatus current, InstanceStatus reported) {
        return current == InstanceStatus.WAITING_WORKER_RECEIVE
                && (reported == InstanceStatus.RUNNING || reported == InstanceStatus.SUCCEED || reported == InstanceStatus.FAILED);
    }
}
