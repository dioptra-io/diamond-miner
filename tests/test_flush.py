from diamond_miner_core.flush import flush_format


def test_flush_format():
    assert flush_format(1, 1, 1, 1) == ["0000000001", "00001", "00001", "001"]
    assert flush_format(12345, 240, 3343, 27) == ["0000012345", "00240", "03343", "027"]
    assert flush_format(1234567890, 24000, 33434, 255) == [
        "1234567890",
        "24000",
        "33434",
        "255",
    ]
