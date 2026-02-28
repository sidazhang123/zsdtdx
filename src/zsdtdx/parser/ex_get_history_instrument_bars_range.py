"""
模块：`parser/ex_get_history_instrument_bars_range.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 1 个，函数 5 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8

    
import struct
from collections import OrderedDict

#import hexdump
from zsdtdx.parser.base import BaseParser


class GetHistoryInstrumentBarsRange(BaseParser):
    def __init__(self, *args, **kvargs):
        """
        输入：
        1. *args: 输入参数，约束以协议定义与函数实现为准。
        2. **kvargs: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__init__` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        self.seqid = 1
        BaseParser.__init__(self, *args, **kvargs)
        
        
    def setParams(self, market, code, date,date2):
        
        
        """
        输入：
        1. market: 输入参数，约束以协议定义与函数实现为准。
        2. code: 输入参数，约束以协议定义与函数实现为准。
        3. date: 输入参数，约束以协议定义与函数实现为准。
        4. date2: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `setParams` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        pkg = bytearray.fromhex('01')
        pkg.extend(struct.pack("<B", self.seqid))
        self.seqid = self.seqid+1
        pkg.extend(bytearray.fromhex('38 92 00 01 16 00 16 00 0D 24'))
        code = code.encode("utf-8")
        #x =struct.pack("<B9s",  market, code)
        pkg.extend(struct.pack("<B9s",  market, code))
        pkg.extend(bytearray.fromhex('07 00'))
        pkg.extend(struct.pack("<LL", date,date2))
        #print(hexdump.hexdump(pkg))
        self.send_pkg = pkg
#      
        
        
    def _parse_date(self, num):
        """
        输入：
        1. num: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_parse_date` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        year = num // 2048 + 2004
        month = (num % 2048) // 100
        day = (num % 2048) % 100

        return year, month, day

    def _parse_time(self, num):
        """
        输入：
        1. num: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `_parse_time` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        return (num // 60) , (num % 60)

    def parseResponse(self, body_buf):
#        print('测试', body_buf)
#        fileobj = open("a.bin", 'wb')  # make partfile
#        fileobj.write(body_buf)  # write data into partfile
#        fileobj.close()
        #print(hexdump.hexdump(body_buf[0:1024]))
#        import zlib
#        d=zlib.decompress(body_buf[16:])        
#        print(hexdump.hexdump(d))
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
        klines=[]
        pos = 12

        # 算了，前面不解析了，没太大用
        # (market, code) = struct.unpack("<B9s", body_buf[0: 10]

        (ret_count,) = struct.unpack("H", body_buf[pos: pos+2])
        pos = pos+2
        #print(hexdump.hexdump(body_buf[20:52]))
       # print(hexdump.hexdump(body_buf[20: 20+ret_count*32]))
        #global raw_li
        print(ret_count)
        
        for i in range(ret_count):
            (d1,d2,open_price, high, low, close, position, trade, settlementprice) = struct.unpack("<HHffffIIf", body_buf[pos:pos+32])  
            #print(raw_li[0])
            pos = pos+ 32
            #print(i)
            #pass
            #print(raw_li[i][0])
            year, month, day = self._parse_date(d1)
            hour, minute     = self._parse_time(d2)
#            print('%02d%02d%02d %02d %02d'%(year,month,day,hour,minute))
#            print('%5.2f %5.2f %5.2f %5.2f %7d %7d %5.2f'%(open_price, high, low, close, position, trade, settlementprice))
#            (open_price, high, low, close, position, trade, price) = struct.unpack("<ffffIIf", body_buf[pos: pos+28])
#            pos += 28
            kline = OrderedDict([
                ("datetime", "%d-%02d-%02d %02d:%02d" % (year, month, day, hour, minute)),
                ("year", year),
                ("month", month),
                ("day", day),
                ("hour", hour),
                ("minute", minute),
                ("open", open_price),
                ("high", high),
                ("low", low),
                ("close", close),
                ("position", position),
                ("trade", trade),
                ("settlementprice", settlementprice)
               
                
                
            ])
            klines.append(kline)

        return klines
        
    
#00000000  01 01 08 6A 01 01 16 00  16 00 FF 23 2F 49 46 4C   ...j.... ...#/IFL 
#00000010  30 00 F0 F4 94 13 07 00  01 00 00 00 00 00 F0 00   0....... ........ 
    
#00000000: 01 01 08 6A 01 01 16 00  16 00 FF 23 4A 4E 56 44  ...j.......#JNVD
#00000010: 41 00 C0 EC A3 13 07 00  01 00 00 00 00 00 C0 03  A...............    

#00000000  01 01 08 6A 01 01 16 00  16 00 FF 23 2F 49 46 31   ...j.... ...#/IF1
#00000010  37 30 39 00 94 13 07 00  01 00 00 00 00 00 F0 00   709..... ........
