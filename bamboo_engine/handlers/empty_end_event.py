# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云PaaS平台社区版 (BlueKing PaaS Community
Edition) available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import logging
from typing import Optional

from bamboo_engine import metrics, states
from bamboo_engine.config import Settings
from bamboo_engine.context import Context
from bamboo_engine.eri import ExecuteInterruptPoint, NodeType, ProcessInfo
from bamboo_engine.handler import ExecuteResult, NodeHandler, register_handler
from bamboo_engine.template.template import Template

logger = logging.getLogger("bamboo_engine")


@register_handler(NodeType.EmptyEndEvent)
class EmptyEndEventHandler(NodeHandler):
    def execute(
        self,
        process_info: ProcessInfo,
        loop: int,
        inner_loop: int,
        version: str,
        recover_point: Optional[ExecuteInterruptPoint] = None,
    ) -> ExecuteResult:
        """
        节点的 execute 处理逻辑

        :param runtime: 引擎运行时实例
        :type runtime: EngineRuntimeInterface
        :param process_info: 进程信息
        :type process_id: ProcessInfo
        :return: 执行结果
        :rtype: ExecuteResult
        """
        with metrics.observe(
            metrics.ENGINE_NODE_EXECUTE_PRE_PROCESS_DURATION, type=self.node.type.value, hostname=self._hostname
        ):
            root_pipeline_id = process_info.root_pipeline_id
            pipeline_id = process_info.pipeline_stack.pop()
            root_pipeline_finished = len(process_info.pipeline_stack) == 0

            root_pipeline_inputs = self._get_plain_inputs(process_info.root_pipeline_id)
            if not root_pipeline_finished:
                subproc_state = self.runtime.get_state(pipeline_id)

            # write pipeline data
            context_outputs = self.runtime.get_context_outputs(pipeline_id)
            logger.info(
                "root_pipeline[%s] pipeline(%s) context outputs: %s",
                root_pipeline_id,
                pipeline_id,
                context_outputs,
            )

            context_values = self.runtime.get_context_values(pipeline_id=pipeline_id, keys=context_outputs)
            logger.info(
                "root_pipeline[%s] pipeline(%s) context values: %s",
                root_pipeline_id,
                pipeline_id,
                context_values,
            )

            # caculate outputs values references
            output_value_refs = set(Template([cv.value for cv in context_values]).get_reference())
            logger.info(
                "root_pipeline[%s] node(%s) outputs values refs: %s",
                root_pipeline_id,
                self.node.id,
                output_value_refs,
            )

            additional_refs = self.runtime.get_context_key_references(pipeline_id=pipeline_id, keys=output_value_refs)
            output_value_refs = output_value_refs.union(additional_refs)
            logger.info(
                "root_pipeline[%s] pipeline(%s) outputs values final refs: %s",
                root_pipeline_id,
                pipeline_id,
                output_value_refs,
            )
            context_values.extend(self.runtime.get_context_values(pipeline_id=pipeline_id, keys=output_value_refs))

            context = Context(self.runtime, context_values, root_pipeline_inputs)
            try:
                hydrated_context = context.hydrate(deformat=False)
            except Exception:
                logger.exception(
                    "root_pipeline[%s] node(%s) context hydrate error",
                    root_pipeline_id,
                    self.node.id,
                )
                self.runtime.set_state(
                    node_id=self.node.id,
                    version=version,
                    to_state=states.FAILED,
                    set_archive_time=True,
                    ignore_boring_set=recover_point is not None,
                )

                return ExecuteResult(
                    should_sleep=True,
                    schedule_ready=False,
                    schedule_type=None,
                    schedule_after=-1,
                    dispatch_processes=[],
                    next_node_id=None,
                )

            logger.info(
                "root_pipeline[%s] pipeline(%s) hydrated context: %s",
                root_pipeline_id,
                pipeline_id,
                hydrated_context,
            )

        outputs = {}
        for key in context_outputs:
            outputs[key] = hydrated_context.get(key, key)
        if not root_pipeline_finished:
            outputs[self.LOOP_KEY] = subproc_state.loop + Settings.RERUN_INDEX_OFFSET
            outputs[self.INNER_LOOP_KEY] = subproc_state.inner_loop + Settings.RERUN_INDEX_OFFSET

            # 为循环场景打包当前循环的输出，供 extract_outputs 使用
            # 支持 SubProcess 和 SubCanvas 节点的循环
            node = self.runtime.get_node(pipeline_id)
            if getattr(node, "loop_enabled", False) or getattr(node, "loop_times", None) is not None:
                loop_outputs = {}
                for origin_key, target_key in self.runtime.get_data_outputs(pipeline_id).items():
                    if origin_key in outputs:
                        loop_outputs[target_key] = outputs[origin_key]
                outputs[Settings.LOOP_OUTPUTS_INNER_KEY] = loop_outputs

        with metrics.observe(
            metrics.ENGINE_NODE_EXECUTE_POST_PROCESS_DURATION, type=self.node.type.value, hostname=self._hostname
        ):
            self.runtime.set_execution_data_outputs(node_id=pipeline_id, outputs=outputs)

            self.runtime.set_state(
                node_id=self.node.id,
                version=version,
                to_state=states.FINISHED,
                set_archive_time=True,
                ignore_boring_set=recover_point is not None,
            )

            pipeline_state = self.runtime.get_state_or_none(node_id=pipeline_id)
            top_pipeline_state_version = pipeline_state.version if pipeline_state else None
            self.runtime.set_state(
                node_id=pipeline_id,
                version=top_pipeline_state_version,
                to_state=states.FINISHED,
                set_archive_time=True,
                ignore_boring_set=recover_point is not None,
            )

            # root pipeline finish
            if root_pipeline_finished:
                self.runtime.pipeline_finish(pipeline_id)
                return ExecuteResult(
                    should_sleep=False,
                    schedule_ready=False,
                    schedule_type=None,
                    schedule_after=-1,
                    dispatch_processes=[],
                    next_node_id=None,
                    should_die=True,
                )

            # subprocess finish
            subprocess = self.runtime.get_node(pipeline_id)

            # 检查是否为子画布且需要继续循环
            # 如果子画布节点开启了循环且当前 inner_loop 未达到循环次数上限
            # 则返回子画布节点 ID，触发下一次循环
            if subprocess.type == NodeType.SubCanvas:
                # 获取当前子画布节点的状态和配置
                subcanvas_state = self.runtime.get_state(pipeline_id)
                current_inner_loop = subcanvas_state.inner_loop if subcanvas_state else inner_loop

                # 判断是否需要继续循环
                if getattr(subprocess, "loop_enabled", False) and subprocess.should_continue_loop(current_inner_loop):
                    logger.info(
                        "root_pipeline[%s] subcanvas(%s) loop continue, inner_loop: %s, loop_times: %s",
                        root_pipeline_id,
                        pipeline_id,
                        current_inner_loop,
                        subprocess.loop_times,
                    )
                    # 返回子画布节点自身，触发下一次循环
                    # 需要先将 pipeline_id 压回 stack，因为前面已经 pop 了
                    process_info.pipeline_stack.append(pipeline_id)
                    self.runtime.set_pipeline_stack(process_info.process_id, process_info.pipeline_stack)

                    return ExecuteResult(
                        should_sleep=False,
                        schedule_ready=False,
                        schedule_type=None,
                        schedule_after=-1,
                        dispatch_processes=[],
                        next_node_id=pipeline_id,  # 返回子画布节点自身，重新执行
                    )

            # extract subprocess outputs to parent context
            subprocess_outputs = self.runtime.get_data_outputs(pipeline_id)
            context.extract_outputs(
                pipeline_id=process_info.pipeline_stack[-1],
                data_outputs=subprocess_outputs,
                execution_data_outputs=outputs,
            )

            self.runtime.set_pipeline_stack(process_info.process_id, process_info.pipeline_stack)

            return ExecuteResult(
                should_sleep=False,
                schedule_ready=False,
                schedule_type=None,
                schedule_after=-1,
                dispatch_processes=[],
                next_node_id=subprocess.target_nodes[0],
            )
