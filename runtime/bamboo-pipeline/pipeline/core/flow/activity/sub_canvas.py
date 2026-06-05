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

from copy import deepcopy

from pipeline.core.flow.activity.base import Activity


class SubCanvas(Activity):
    """
    子画布节点，用于在主流程中嵌入一个完整的子画布
    包含子画布的 pipeline_tree，执行时会在子进程中执行
    """

    result_bit = "_result"
    loop = "_loop"
    ON_RETRY = "_on_retry"

    def __init__(
        self,
        id,
        pipeline=None,
        name=None,
        data=None,
        error_ignorable=False,
        failure_handler=None,
        skippable=True,
        retryable=True,
        timeout=None,
    ):
        super(SubCanvas, self).__init__(id, name, data, failure_handler)
        self.pipeline = pipeline or {}
        self.error_ignorable = error_ignorable
        self.skippable = skippable
        self.retryable = retryable
        self.timeout = timeout

        if data:
            self._prepared_inputs = self.data.inputs_copy()
            self._prepared_outputs = self.data.outputs_copy()

    def __setstate__(self, state):
        for attr, obj in list(state.items()):
            setattr(self, attr, obj)

        if "timeout" not in state:
            self.timeout = None
        if "pipeline" not in state:
            self.pipeline = {}

    def execute(self, parent_data):
        """
        执行子画布节点
        实际执行逻辑由 engine 的 plugin_execute 处理
        """
        return True

    def skip(self):
        self._set_result_bit(True)
        return True

    def ignore_error(self):
        self._set_result_bit(False)
        return True

    def clear_outputs(self):
        self.data.reset_outputs({})

    def _set_result_bit(self, result):
        if result is False:
            self.data.set_outputs(self.result_bit, False)
        else:
            self.data.set_outputs(self.result_bit, True)

    def get_result_bit(self):
        return self.data.get_one_of_outputs(self.result_bit, False)

    def shell(self):
        shell = SubCanvas(
            id=self.id,
            pipeline=self.pipeline,
            name=self.name,
            data=self.data,
            error_ignorable=self.error_ignorable,
            timeout=self.timeout,
        )
        return shell

    def prepare_rerun_data(self):
        self.data.override_inputs(deepcopy(self._prepared_inputs))
        self.data.override_outputs(deepcopy(self._prepared_outputs))
