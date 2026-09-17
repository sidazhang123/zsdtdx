"""离线验收标准行情握手组包：官方首包常量、随机尾、tdxlevel 身份。"""

from zsdtdx.parser.setup_commands import SetupCmd1, SetupCmd2, SetupCmd3

_SETUP1_PREFIX = bytes.fromhex(
    "0c01187b00011a011a010b00e53878ee8bd8dbb8749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357749933ae"
    "27700357749933ae27700357749933ae27700357749933ae27700357b979179e"
    "dcfc5a4c6810db2bdf3e50a19e93269128ddf91f749933ae27700357749933ae"
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


def test_setup_cmd2_official_merged_packet():
    """输入：SetupCmd2.setup；输出：官方 13 字节合并握手包。"""
    cmd = SetupCmd2(None)
    cmd.setup()
    assert bytes(cmd.send_pkg) == bytes.fromhex("0c0218940001030003000d0001")


def test_setup_cmd3_tdxlevel_identity():
    """输入：SetupCmd3.setup；输出：含 ASCII tdxlevel 的 42 字节身份包。"""
    cmd = SetupCmd3(None)
    cmd.setup()
    raw = bytes(cmd.send_pkg)
    assert len(raw) == 42
    assert raw[12:20] == b"tdxlevel"
    assert raw == bytes.fromhex(
        "0c031899000120002000db0f7464786c6576656c000000295cf74011000000"
        "0000000000000000000005"
    )
