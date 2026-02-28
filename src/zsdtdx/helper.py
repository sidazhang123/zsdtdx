"""
模块：`helper.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 0 个，函数 5 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import struct

import six


#### XXX: 分析了一下，貌似是类似utf-8的编码方式保存有符号数字
def get_price(data, pos):
    """
    输入：
    1. data: 输入参数，约束以协议定义与函数实现为准。
    2. pos: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_price` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    pos_byte = 6
    bdata = indexbytes(data, pos)
    intdata = bdata & 0x3f
    if bdata & 0x40:
        sign = True
    else:
        sign = False

    if bdata & 0x80:
        while True:
            pos += 1
            bdata = indexbytes(data, pos)
            intdata += (bdata & 0x7f) << pos_byte
            pos_byte += 7

            if bdata & 0x80:
                pass
            else:
                break

    pos += 1

    if sign:
        intdata = -intdata

    return intdata, pos


def get_volume(ivol):
    """
    输入：
    1. ivol: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_volume` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    logpoint = ivol >> (8 * 3)
    hheax = ivol >> (8 * 3);  # [3]
    hleax = (ivol >> (8 * 2)) & 0xff;  # [2]
    lheax = (ivol >> 8) & 0xff;  # [1]
    lleax = ivol & 0xff;  # [0]

    dbl_1 = 1.0
    dbl_2 = 2.0
    dbl_128 = 128.0

    dwEcx = logpoint * 2 - 0x7f;
    dwEdx = logpoint * 2 - 0x86;
    dwEsi = logpoint * 2 - 0x8e;
    dwEax = logpoint * 2 - 0x96;
    if dwEcx < 0:
        tmpEax = - dwEcx
    else:
        tmpEax = dwEcx

    dbl_xmm6 = 0.0
    dbl_xmm6 = pow(2.0, tmpEax)
    if dwEcx < 0:
        dbl_xmm6 = 1.0 / dbl_xmm6

    dbl_xmm4 = 0
    if hleax > 0x80:
        tmpdbl_xmm3 = 0.0
        tmpdbl_xmm1 = 0.0
        dwtmpeax = dwEdx + 1
        tmpdbl_xmm3 = pow(2.0, dwtmpeax)
        dbl_xmm0 = pow(2.0, dwEdx) * 128.0
        dbl_xmm0 += (hleax & 0x7f) * tmpdbl_xmm3
        dbl_xmm4 = dbl_xmm0

    else:
        dbl_xmm0 = 0.0
        if dwEdx >= 0:
            dbl_xmm0 = pow(2.0, dwEdx) * hleax
        else:
            dbl_xmm0 = (1 / pow(2.0, dwEdx)) * hleax
        dbl_xmm4 = dbl_xmm0

    dbl_xmm3 = pow(2.0, dwEsi) * lheax
    dbl_xmm1 = pow(2.0, dwEax) * lleax
    if hleax & 0x80:
        dbl_xmm3 *= 2.0
        dbl_xmm1 *= 2.0

    dbl_ret = dbl_xmm6 + dbl_xmm4 + dbl_xmm3 + dbl_xmm1
    return dbl_ret


def get_datetime(category, buffer, pos):
    """
    输入：
    1. category: 输入参数，约束以协议定义与函数实现为准。
    2. buffer: 输入参数，约束以协议定义与函数实现为准。
    3. pos: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_datetime` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    year = 0
    month = 0
    day = 0
    hour = 15
    minute = 0
    if category < 4 or category == 7 or category == 8:
        (zipday, tminutes) = struct.unpack("<HH", buffer[pos: pos + 4])
        year = (zipday >> 11) + 2004
        month = int((zipday % 2048) / 100)
        day = (zipday % 2048) % 100

        hour = int(tminutes / 60)
        minute = tminutes % 60
    else:
        (zipday,) = struct.unpack("<I", buffer[pos: pos + 4])

        year = int(zipday / 10000);
        month = int((zipday % 10000) / 100)
        day = zipday % 100

    pos += 4

    return year, month, day, hour, minute, pos


def get_time(buffer, pos):
    """
    输入：
    1. buffer: 输入参数，约束以协议定义与函数实现为准。
    2. pos: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `get_time` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    (tminutes, ) = struct.unpack("<H", buffer[pos: pos + 2])
    hour = int(tminutes / 60)
    minute = tminutes % 60
    pos += 2

    return hour, minute, pos

def indexbytes(data, pos):

    """
    输入：
    1. data: 输入参数，约束以协议定义与函数实现为准。
    2. pos: 输入参数，约束以协议定义与函数实现为准。
    输出：
    1. 返回值语义由函数实现定义；无返回时为 `None`。
    用途：
    1. 执行 `indexbytes` 对应的协议处理、数据解析或调用适配逻辑。
    边界条件：
    1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
    """
    if six.PY2:
        if type(data) is bytearray:
            return data[pos]
        else:
            return six.indexbytes(data, pos)
    else:
        return data[pos]
