"""离线验收标准行情握手组包：银河首包常量、随机尾、「银河证券」身份。"""

import struct

from zsdtdx.parser.setup_commands import SetupCmd1, SetupCmd2, SetupCmd3

_SETUP1_PREFIX = bytes.fromhex(
    "0c01187b00011a011a010b0047d7b5ff89a775cb6f9a279b25c23750749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae277003571ca34012266752b310928f2dbc306c776b5bc87e"
    "b3376a05588b8447c7bbb2528608c62da95a2b69749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357f7e5f2e0"
    "de0975886f9a279b25c23750749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357"
)


def test_setup_cmd1_prefix_and_random_tail():
    """输入：两次构造 SetupCmd1；输出：长度 292、前 284 相同、尾 8 字节不同。"""
    a = SetupCmd1(None)
    a.setup()
    b = SetupCmd1(None)
    b.setup()
    assert len(a.send_pkg) == 292
    assert len(b.send_pkg) == 292
    assert bytes(a.send_pkg[:284]) == _SETUP1_PREFIX
    assert bytes(b.send_pkg[:284]) == _SETUP1_PREFIX
    assert bytes(a.send_pkg[284:]) != bytes(b.send_pkg[284:])


def test_setup_cmd2_yinhe_merged_packet():
    """输入：SetupCmd2.setup；输出：银河 13 字节合并握手包（末字节 02）。"""
    cmd = SetupCmd2(None)
    cmd.setup()
    assert bytes(cmd.send_pkg) == bytes.fromhex("0c0218940001030003000d0002")


def test_setup_cmd3_yinhe_identity():
    """输入：SetupCmd3.setup；输出：含 GBK「银河证券」的 42 字节身份包。"""
    cmd = SetupCmd3(None)
    cmd.setup()
    raw = bytes(cmd.send_pkg)
    assert len(raw) == 42
    assert raw[12:20] == "银河证券".encode("gbk")
    assert abs(struct.unpack_from("<f", raw, 23)[0] - 11.63) < 1e-5
    assert struct.unpack_from("<I", raw, 27)[0] == 14
    assert raw[-1] == 5
    assert raw == bytes.fromhex(
        "0c031899000120002000db0fd2f8bad3d6a4c8af0000007b143a410e000000"
        "0000000000000000000005"
    )
