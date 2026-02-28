"""
模块：`parser/get_finance_info.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 3 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

import struct
from collections import OrderedDict

import six

from zsdtdx.parser.base import BaseParser

class GetFinanceInfo(BaseParser):

    def setParams(self, market, code):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if type(code) is six.text_type:
            code = code.encode("utf-8")
        pkg = bytearray.fromhex(u'0c 1f 18 76 00 01 0b 00 0b 00 10 00 01 00')
        pkg.extend(struct.pack(u"<B6s", market, code))
        self.send_pkg = pkg

    def parseResponse(self, body_buf):
        """
        输入：
        1. body_buf: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `parseResponse` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pos = 0
        pos += 2 #skip num ,we only query 1 in this case
        market, code = struct.unpack(u"<B6s",body_buf[pos: pos+7])
        pos += 7

        (
            liutongguben,
            province,
            industry,
            updated_date,
            ipo_date,
            zongguben,
            guojiagu,
            faqirenfarengu,
            farengu,
            bgu,
            hgu,
            zhigonggu,
            zongzichan,
            liudongzichan,
            gudingzichan,
            wuxingzichan,
            gudongrenshu,
            liudongfuzhai,
            changqifuzhai,
            zibengongjijin,
            jingzichan,
            zhuyingshouru,
            zhuyinglirun,
            yingshouzhangkuan,
            yingyelirun,
            touzishouyu,
            jingyingxianjinliu,
            zongxianjinliu,
            cunhuo,
            lirunzonghe,
            shuihoulirun,
            jinglirun,
            weifenlirun,
            baoliu1,
            baoliu2
        ) = struct.unpack("<fHHIIffffffffffffffffffffffffffffff", body_buf[pos:])

        def _get_v(v):
            """
            输入：
            1. v: 输入参数，约束以协议定义与函数实现为准。
            输出：
            1. 返回值语义由函数实现定义；无返回时为 `None`。
            用途：
            1. 执行 `_get_v` 对应的协议处理、数据解析或调用适配逻辑。
            边界条件：
            1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
            """
            return v

        return OrderedDict(
            [
                ("market", market),
                ("code", code.decode("utf-8")),
                ("liutongguben", _get_v(liutongguben)*10000),
                ('province', province),
                ('industry', industry),
                ('updated_date', updated_date),
                ('ipo_date', ipo_date),
                ("zongguben", _get_v(zongguben)*10000),
                ("guojiagu", _get_v(guojiagu)*10000),
                ("faqirenfarengu", _get_v(faqirenfarengu)*10000),
                ("farengu", _get_v(farengu)*10000),
                ("bgu", _get_v(bgu)*10000),
                ("hgu", _get_v(hgu)*10000),
                ("zhigonggu", _get_v(zhigonggu)*10000),
                ("zongzichan", _get_v(zongzichan)*10000),
                ("liudongzichan", _get_v(liudongzichan)*10000),
                ("gudingzichan", _get_v(gudingzichan)*10000),
                ("wuxingzichan", _get_v(wuxingzichan)*10000),
                ("gudongrenshu", _get_v(gudongrenshu)),
                ("liudongfuzhai", _get_v(liudongfuzhai)*10000),
                ("changqifuzhai", _get_v(changqifuzhai)*10000),
                ("zibengongjijin", _get_v(zibengongjijin)*10000),
                ("jingzichan", _get_v(jingzichan)*10000),
                ("zhuyingshouru", _get_v(zhuyingshouru)*10000),
                ("zhuyinglirun", _get_v(zhuyinglirun)*10000),
                ("yingshouzhangkuan", _get_v(yingshouzhangkuan)*10000),
                ("yingyelirun", _get_v(yingyelirun)*10000),
                ("touzishouyu", _get_v(touzishouyu)*10000),
                ("jingyingxianjinliu", _get_v(jingyingxianjinliu)*10000),
                ("zongxianjinliu", _get_v(zongxianjinliu)*10000),
                ("cunhuo", _get_v(cunhuo)*10000),
                ("lirunzonghe", _get_v(lirunzonghe)*10000),
                ("shuihoulirun", _get_v(shuihoulirun)*10000),
                ("jinglirun", _get_v(jinglirun)*10000),
                ("weifenpeilirun", _get_v(weifenlirun)*10000),
                ("meigujingzichan", _get_v(baoliu1)),
                ("baoliu2", _get_v(baoliu2))
            ]
        )
