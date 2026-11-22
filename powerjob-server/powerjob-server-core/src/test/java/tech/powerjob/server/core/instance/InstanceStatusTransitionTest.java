package tech.powerjob.server.core.instance;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.server.core.instance.InstanceStatusTransition.Action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 普通任务实例状态迁移表的逐格参数化测试（7 种当前状态 x 7 种上报状态）
 */
class InstanceStatusTransitionTest {

    @ParameterizedTest(name = "{0} + {1} -> {2}")
    @CsvSource({
            // WAITING_DISPATCH：接受运行侧上报，不递增运行次数
            "WAITING_DISPATCH,RUNNING,APPLY",
            "WAITING_DISPATCH,SUCCEED,FINISH",
            "WAITING_DISPATCH,FAILED,RETRY_OR_FINISH",
            "WAITING_DISPATCH,WAITING_DISPATCH,IGNORE",
            "WAITING_DISPATCH,WAITING_WORKER_RECEIVE,IGNORE",
            "WAITING_DISPATCH,CANCELED,IGNORE",
            "WAITING_DISPATCH,STOPPED,IGNORE",
            // WAITING_WORKER_RECEIVE：-> RUNNING 是唯一的非终态递增迁移
            "WAITING_WORKER_RECEIVE,RUNNING,APPLY",
            "WAITING_WORKER_RECEIVE,SUCCEED,FINISH",
            "WAITING_WORKER_RECEIVE,FAILED,RETRY_OR_FINISH",
            "WAITING_WORKER_RECEIVE,WAITING_DISPATCH,IGNORE",
            "WAITING_WORKER_RECEIVE,WAITING_WORKER_RECEIVE,IGNORE",
            "WAITING_WORKER_RECEIVE,CANCELED,IGNORE",
            "WAITING_WORKER_RECEIVE,STOPPED,IGNORE",
            // RUNNING：运行中只能被服务端失联判定改回等待派发，上报路径不存在该迁移
            "RUNNING,RUNNING,APPLY",
            "RUNNING,SUCCEED,FINISH",
            "RUNNING,FAILED,RETRY_OR_FINISH",
            "RUNNING,WAITING_DISPATCH,IGNORE",
            "RUNNING,WAITING_WORKER_RECEIVE,IGNORE",
            "RUNNING,CANCELED,IGNORE",
            "RUNNING,STOPPED,IGNORE",
            // FAILED（终态）：不可改写；旧 TaskTracker 仍上报运行中则通知停止
            "FAILED,RUNNING,STOP_STRAY_TRACKER",
            "FAILED,SUCCEED,IGNORE_ON_TERMINAL",
            "FAILED,FAILED,IGNORE_ON_TERMINAL",
            "FAILED,WAITING_DISPATCH,IGNORE_ON_TERMINAL",
            "FAILED,WAITING_WORKER_RECEIVE,IGNORE_ON_TERMINAL",
            "FAILED,CANCELED,IGNORE_ON_TERMINAL",
            "FAILED,STOPPED,IGNORE_ON_TERMINAL",
            // SUCCEED（终态）
            "SUCCEED,RUNNING,STOP_STRAY_TRACKER",
            "SUCCEED,SUCCEED,IGNORE_ON_TERMINAL",
            "SUCCEED,FAILED,IGNORE_ON_TERMINAL",
            "SUCCEED,WAITING_DISPATCH,IGNORE_ON_TERMINAL",
            "SUCCEED,WAITING_WORKER_RECEIVE,IGNORE_ON_TERMINAL",
            "SUCCEED,CANCELED,IGNORE_ON_TERMINAL",
            "SUCCEED,STOPPED,IGNORE_ON_TERMINAL",
            // CANCELED（终态）
            "CANCELED,RUNNING,STOP_STRAY_TRACKER",
            "CANCELED,SUCCEED,IGNORE_ON_TERMINAL",
            "CANCELED,FAILED,IGNORE_ON_TERMINAL",
            "CANCELED,WAITING_DISPATCH,IGNORE_ON_TERMINAL",
            "CANCELED,WAITING_WORKER_RECEIVE,IGNORE_ON_TERMINAL",
            "CANCELED,CANCELED,IGNORE_ON_TERMINAL",
            "CANCELED,STOPPED,IGNORE_ON_TERMINAL",
            // STOPPED（终态）
            "STOPPED,RUNNING,STOP_STRAY_TRACKER",
            "STOPPED,SUCCEED,IGNORE_ON_TERMINAL",
            "STOPPED,FAILED,IGNORE_ON_TERMINAL",
            "STOPPED,WAITING_DISPATCH,IGNORE_ON_TERMINAL",
            "STOPPED,WAITING_WORKER_RECEIVE,IGNORE_ON_TERMINAL",
            "STOPPED,CANCELED,IGNORE_ON_TERMINAL",
            "STOPPED,STOPPED,IGNORE_ON_TERMINAL",
    })
    void decideCoversEveryCell(InstanceStatus current, InstanceStatus reported, Action expected) {
        assertEquals(expected, InstanceStatusTransition.decide(current, reported));
    }

    @ParameterizedTest(name = "countsAttempt({0},{1}) = {2}")
    @CsvSource({
            // 仅「等待Worker接收」的首次有效上报计一次运行次数
            "WAITING_WORKER_RECEIVE,RUNNING,true",
            "WAITING_WORKER_RECEIVE,SUCCEED,true",
            "WAITING_WORKER_RECEIVE,FAILED,true",
            "WAITING_WORKER_RECEIVE,WAITING_DISPATCH,false",
            "WAITING_WORKER_RECEIVE,CANCELED,false",
            "WAITING_DISPATCH,RUNNING,false",
            "WAITING_DISPATCH,SUCCEED,false",
            "WAITING_DISPATCH,FAILED,false",
            "RUNNING,RUNNING,false",
            "RUNNING,SUCCEED,false",
            "RUNNING,FAILED,false",
            "FAILED,RUNNING,false",
            "SUCCEED,RUNNING,false",
            "CANCELED,RUNNING,false",
            "STOPPED,RUNNING,false",
    })
    void countsAttemptOnlyOnLeavingWaitingWorkerReceive(InstanceStatus current, InstanceStatus reported, boolean expected) {
        if (expected) {
            assertTrue(InstanceStatusTransition.countsAttempt(current, reported));
        } else {
            assertFalse(InstanceStatusTransition.countsAttempt(current, reported));
        }
    }
}
