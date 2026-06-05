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

from bamboo_engine import metrics
from bamboo_engine.config import Settings
from bamboo_engine.context import Context
from bamboo_engine.eri import (
    ContextValue,
    ContextValueType,
    ExecuteInterruptPoint,
    NodeType,
    ProcessInfo,
)
from bamboo_engine.handler import ExecuteResult, NodeHandler, register_handler
from bamboo_engine.interrupt import ExecuteKeyPoint
from bamboo_engine.template import Template

logger = logging.getLogger("bamboo_engine")


@register_handler(NodeType.SubCanvas)
class SubCanvasHandler(NodeHandler):
    """
    子画布处理器

    子画布与子流程的区别：
    1. 子画布共享父流程的上下文，不创建独立的上下文
    2. 子画布的 pipeline_stack 行为可能不同（取决于具体需求）
    3. 子画布可以视为一种轻量级的子流程，用于逻辑分组或前端展示
    """

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

        :param process_info: 进程信息
        :type process_info: ProcessInfo
        :param loop: 循环次数
        :type loop: int
        :param inner_loop: 内层循环次数
        :type inner_loop: int
        :param version: 版本号
        :type version: str
        :param recover_point: 恢复点
        :type recover_point: Optional[ExecuteInterruptPoint]
        :return: 执行结果
        :rtype: ExecuteResult
        """

        with metrics.observe(
            metrics.ENGINE_NODE_EXECUTE_PRE_PROCESS_DURATION, type=self.node.type.value, hostname=self._hostname
        ):
            data = self.runtime.get_data(self.node.id)
            root_pipeline_id = process_info.root_pipeline_id
            top_pipeline_id = process_info.top_pipeline_id

            # 解析输入中的上下文引用
            need_render_inputs = data.need_render_inputs()
            render_escape_inputs = data.render_escape_inputs()
            inputs_refs = Template(need_render_inputs).get_reference()

            # 准备上下文：子画布共享父流程的上下文
            context_values = self.runtime.get_context_values(pipeline_id=top_pipeline_id, keys=inputs_refs)

            # 处理循环参数：将循环参数作为变量加入到上下文中，供子画布使用
            node = self.runtime.get_node(self.node.id)
            if hasattr(node, "loop_enabled") and node.loop_enabled:
                loop_params = node.loop_config.loop_params if node.loop_config else {}
                min_loop_times = None

                for param_key, param_value in loop_params.items():
                    param_refs = Template(param_value).get_reference()
                    if param_refs:
                        # 参数包含引用，需要渲染
                        param_context_values = self.runtime.get_context_values(
                            pipeline_id=top_pipeline_id, keys=param_refs
                        )
                        hydrated_param_context = Context(self.runtime, param_context_values, {}).hydrate(deformat=True)
                        inputs = Template(param_value).render(hydrated_param_context)

                        # 判断渲染后的值是否为可迭代对象（列表/元组/字典），若不是则抛出异常
                        if not isinstance(inputs, (list, tuple, dict)):
                            return self._execute_fail(
                                ex_data="循环参数 %s 的值必须是可迭代对象，当前值类型为 %s，值为：%s"
                                % (param_key, type(inputs).__name__, inputs),
                                version=version,
                                ignore_boring_set=recover_point is not None,
                            )

                        # 检查是否超过最大循环次数
                        max_loop_times = getattr(Settings, "MAX_LOOP_TIMES", 1000)
                        if len(inputs) > max_loop_times:
                            return self._execute_fail(
                                ex_data="循环参数 %s 的值超过最大循环次数 %s" % (param_key, max_loop_times),
                                version=version,
                                ignore_boring_set=recover_point is not None,
                            )

                        current_len = len(inputs)
                        # 获取当前循环次数对应的值（inner_loop 从 1 开始）
                        loop_item_value = list(inputs)[inner_loop - 1]
                    else:
                        # 参数不包含引用，按逗号分隔解析
                        items = [item.strip() for item in param_value.split(",") if item.strip()]
                        current_len = len(items)
                        loop_item_value = items[inner_loop - 1]

                    min_loop_times = current_len if min_loop_times is None else min(min_loop_times, current_len)

                    # 将循环参数作为上下文值添加，供子画布使用
                    context_values.append(
                        ContextValue(
                            key=param_key,
                            type=ContextValueType.PLAIN,
                            value=loop_item_value,
                            code=None,
                        )
                    )

                # 如果节点没有设置 loop_times，则更新为最小循环次数
                if not getattr(node, "loop_times", None):
                    self.runtime.update_node_loop_times(node_id=self.node.id, loop_times=min_loop_times)

                logger.info(
                    "root_pipeline[%s] node(%s) subcanvas loop params processed, min_loop_times: %s, inner_loop: %s",
                    root_pipeline_id,
                    self.node.id,
                    min_loop_times,
                    inner_loop,
                )

            # 上下文填充（子画布共享父流程上下文，root_pipeline_inputs 设为空字典）
            root_pipeline_inputs = {}
            context = Context(self.runtime, context_values, root_pipeline_inputs)
            try:
                hydrated_context = context.hydrate(deformat=True)
            except Exception as e:
                logger.exception(
                    "root_pipeline[%s] node(%s) context hydrate error",
                    root_pipeline_id,
                    self.node.id,
                )
                return self._execute_fail(
                    ex_data="context hydrate failed(%s), check node log for details." % e,
                    version=version,
                    ignore_boring_set=recover_point is not None,
                )

            logger.info(
                "root_pipeline[%s] node(%s) subcanvas hydrated context: %s",
                root_pipeline_id,
                self.node.id,
                hydrated_context,
            )

        # 解析输入
        subcanvas_inputs = Template(need_render_inputs).render(hydrated_context)
        subcanvas_inputs.update(render_escape_inputs)

        # 将子画布输入和循环参数作为上下文值写入（供子画布内部节点使用）
        sub_context_values = {
            key: ContextValue(key=key, type=ContextValueType.PLAIN, value=value)
            for key, value in subcanvas_inputs.items()
        }

        # 将循环参数也加入到上下文，供子画布使用
        for cv in context_values:
            if cv.key not in sub_context_values:
                sub_context_values[cv.key] = cv

        logger.info(
            "root_pipeline[%s] node(%s) subcanvas inject context keys: %s",
            root_pipeline_id,
            self.node.id,
            list(sub_context_values.keys()),
        )

        with metrics.observe(
            metrics.ENGINE_NODE_EXECUTE_POST_PROCESS_DURATION, type=self.node.type.value, hostname=self._hostname
        ):
            # 更新子画布执行数据输入
            self.runtime.set_execution_data_inputs(self.node.id, subcanvas_inputs)

            # 将子画布输入写入到子画布的上下文中
            self.runtime.upsert_plain_context_values(self.node.id, sub_context_values)

            # 将子画布 ID 压入 pipeline_stack，以便结束后能正确返回
            if not recover_point or not recover_point.handler_data.pipeline_stack_setted:
                process_info.pipeline_stack.append(self.node.id)
                self.runtime.set_pipeline_stack(process_info.process_id, process_info.pipeline_stack)

            self.interrupter.check_and_set(
                ExecuteKeyPoint.SP_SET_PIPELINE_STACK_DONE, pipeline_stack_setted=True, from_handler=True
            )

            # 返回子画布的开始节点 ID，开始执行子画布内部节点
            # 假设 SubCanvas 节点也有 start_event_id 属性
            start_event_id = self.node.start_event_id

            return ExecuteResult(
                should_sleep=False,
                schedule_ready=False,
                schedule_type=None,
                schedule_after=-1,
                dispatch_processes=[],
                next_node_id=start_event_id,
            )
