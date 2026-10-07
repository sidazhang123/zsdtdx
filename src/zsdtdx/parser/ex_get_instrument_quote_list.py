"""
模块：`parser/ex_get_instrument_quote_list.py`。

职责：
1. 构造并解析扩展行情期货/港股批量报价页。
2. 将协议二进制字段转换为上层可消费的数据结构。

边界：
1. 只负责单次请求组包与回包解析，不管理连接池、重试或 host 切换。
2. 网络错误由上层客户端处理；本模块只定义协议数据边界。
"""

# coding=utf-8

import struct
from collections import OrderedDict

from zsdtdx.parser.base import BaseParser


class GetInstrumentQuoteList(BaseParser):
    def setParams(self, market, category, start, count):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. category: 输入参数，约束以协议定义与函数实现为准。
        3. start: 输入参数，约束以协议定义与函数实现为准。
        4. count: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        pkg = bytearray.fromhex("01 c1 06 0b 00 02 0b 00 0b 00 00 24")
        pkg.extend(struct.pack("<BHHHH", market, 0, start, count, 1))
        self.category = category
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
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        pos = 0
        (num,) = struct.unpack("<H", body_buf[pos : pos + 2])
        pos += 2

        if num == 0:
            return []

        datalist = []
        if self.category not in [2, 3]:
            return []

        for i in range(num):
            """
            每个块一共300bytes
            """
            market, code = struct.unpack("<B9s", body_buf[pos : pos + 10])
            code = code.strip(b"\0").decode("gbk")  # to unicode
            pos += 10
            if self.category == 3:
                try:
                    pos = self.extract_futures(market, code, body_buf, datalist, pos)
                except Exception:
                    return []
            elif self.category == 2:
                """
                   market  category   name short_name
                0      31         2   香港主板         KH
                1      48         2  香港创业板         KG
                2      49         2   香港基金         KT
                3      71         2    沪港通         GH
                """
                try:
                    pos = self.extract_hongkong_stocks(
                        market, code, body_buf, datalist, pos
                    )
                except Exception:
                    return []

        return datalist

    def extract_hongkong_stocks(self, market, code, body_buf, datalist, pos):
        """
        :param body_buf[out]:
        :param datalist[out]:
        :param pos[out]:
        :return:
        """
        data_pack_format = "<IfffffIfIIfIIIIfffffIIIIIfffffIIIII"
        (
            HuoYueDu,
            ZuoShou,
            JinKai,
            ZuiGao,
            ZuiDi,
            XianJia,
            _,  # 0
            MaiRuJia,  # ?
            ZongLiang,
            XianLiang,  # ?
            ZongJinE,
            _,  # ?
            _,  # ?
            Nei,  # 0
            Wai,  # 0 Nei/Wai = 内外比？
            MaiRuJia1,
            MaiRuJia2,
            MaiRuJia3,
            MaiRuJia4,
            MaiRuJia5,
            MaiRuLiang1,
            MaiRuLiang2,
            MaiRuLiang3,
            MaiRuLiang4,
            MaiRuLiang5,
            MaiChuJia1,
            MaiChuJia2,
            MaiChuJia3,
            MaiChuJia4,
            MaiChuJia5,
            MaiChuLiang1,
            MaiChuLiang2,
            MaiChuLiang3,
            MaiChuLiang4,
            MaiChuLiang5,
        ) = struct.unpack(data_pack_format, body_buf[pos : pos + 140])
        pos += 290
        one = OrderedDict(
            [
                ("market", market),
                ("code", code),
                ("HuoYueDu", HuoYueDu),
                ("ZuoShou", ZuoShou),
                ("JinKai", JinKai),
                ("ZuiGao", ZuiGao),
                ("ZuiDi", ZuiDi),
                ("XianJia", XianJia),
                ("MaiRuJia", MaiRuJia),
                ("ZongLiang", ZongLiang),
                ("XianLiang", XianLiang),
                ("ZongJinE", ZongJinE),
                ("Nei", Nei),
                ("Wai", Wai),  # 0 Nei/Wai = 内外比？
                ("MaiRuJia1", MaiRuJia1),
                ("MaiRuJia2", MaiRuJia2),
                ("MaiRuJia3", MaiRuJia3),
                ("MaiRuJia4", MaiRuJia4),
                ("MaiRuJia5", MaiRuJia5),
                ("MaiRuLiang1", MaiRuLiang1),
                ("MaiRuLiang2", MaiRuLiang2),
                ("MaiRuLiang3", MaiRuLiang3),
                ("MaiRuLiang4", MaiRuLiang4),
                ("MaiRuLiang5", MaiRuLiang5),
                ("MaiChuJia1", MaiChuJia1),
                ("MaiChuJia2", MaiChuJia2),
                ("MaiChuJia3", MaiChuJia3),
                ("MaiChuJia4", MaiChuJia4),
                ("MaiChuJia5", MaiChuJia5),
                ("MaiChuLiang1", MaiChuLiang1),
                ("MaiChuLiang2", MaiChuLiang2),
                ("MaiChuLiang3", MaiChuLiang3),
                ("MaiChuLiang4", MaiChuLiang4),
                ("MaiChuLiang5", MaiChuLiang5),
            ]
        )
        datalist.append(one)
        return pos

    def extract_futures(self, market, code, body_buf, datalist, pos):
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. body_buf: 输入参数，约束以协议定义与函数实现为准。
        4. datalist: 输入参数，约束以协议定义与函数实现为准。
        5. pos: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `extract_futures` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 连接与重试由上层客户端负责；本函数只处理当前协议数据。
        """
        data_pack_format = "<IfffffIIIIfIIfIfIIIIIIIIIfIIIIIIIII"

        (
            BiShu,
            ZuoJie,
            JinKai,
            ZuiGao,
            ZuiDi,
            MaiChu,
            KaiCang,
            _,
            ZongLiang,
            XianLiang,
            ZongJinE,
            NeiPan,
            WaiPan,
            _,
            ChiCangLiang,
            MaiRuJia,
            _,
            _,
            _,
            _,
            MaiRuLiang,
            _,
            _,
            _,
            _,
            MaiChuJia,
            _,
            _,
            _,
            _,
            MaiChuLiang,
            _,
            _,
            _,
            _,
        ) = struct.unpack(data_pack_format, body_buf[pos : pos + 140])
        pos += 290
        one = OrderedDict(
            [
                ("market", market),
                ("code", code),
                ("BiShu", BiShu),
                ("ZuoJie", ZuoJie),
                ("JinKai", JinKai),
                ("ZuiGao", ZuiGao),
                ("ZuiDi", ZuiDi),
                ("MaiChu", MaiChu),
                ("KaiCang", KaiCang),
                ("ZongLiang", ZongLiang),
                ("XianLiang", XianLiang),
                ("ZongJinE", ZongJinE),
                ("NeiPan", NeiPan),
                ("WaiPan", WaiPan),
                ("ChiCangLiang", ChiCangLiang),
                ("MaiRuJia", MaiRuJia),
                ("MaiRuLiang", MaiRuLiang),
                ("MaiChuJia", MaiChuJia),
                ("MaiChuLiang", MaiChuLiang),
            ]
        )
        datalist.append(one)
        return pos
