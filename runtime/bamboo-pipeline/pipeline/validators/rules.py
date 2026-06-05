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

from pipeline.core.flow import FlowNodeClsFactory

MAX_IN = 1000
MAX_OUT = 1000


def get_flow_nodes_without_start_event():
    """动态获取不包含开始事件的节点类型列表"""
    return FlowNodeClsFactory.node_types_without_start_event()


def get_flow_nodes_without_start_end_event():
    """动态获取不包含开始和结束事件的节点类型列表"""
    return FlowNodeClsFactory.node_types_without_start_end_event()


SOURCE_RULE = {
    "min_in": 0,
    "max_in": 0,
    "min_out": 1,
    "max_out": 1,
    "allowed_out": get_flow_nodes_without_start_end_event(),
}

SINK_RULE = {"min_in": 1, "max_in": MAX_IN, "min_out": 0, "max_out": 0, "allowed_out": []}

ACTIVITY_RULE = {
    "min_in": 1,
    "max_in": MAX_IN,
    "min_out": 1,
    "max_out": 1,
    "allowed_out": get_flow_nodes_without_start_event(),
}

EMIT_RULE = {
    "min_in": 1,
    "max_in": MAX_IN,
    "min_out": 1,
    "max_out": MAX_OUT,
    "allowed_out": get_flow_nodes_without_start_event(),
}

CONVERGE_RULE = {
    "min_in": 1,
    "max_in": MAX_IN,
    "min_out": 1,
    "max_out": 1,
    "allowed_out": get_flow_nodes_without_start_event(),
}

# rules of activity graph
NODE_RULES = {
    "EmptyStartEvent": SOURCE_RULE,
    "EmptyEndEvent": SINK_RULE,
    "ServiceActivity": ACTIVITY_RULE,
    "ExclusiveGateway": EMIT_RULE,
    "ParallelGateway": EMIT_RULE,
    "ConditionalParallelGateway": EMIT_RULE,
    "ConvergeGateway": CONVERGE_RULE,
    "SubProcess": ACTIVITY_RULE,
    "SubCanvas": ACTIVITY_RULE,
}
