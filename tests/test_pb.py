import os
import struct
import unittest
import gzip

import pb

MAGIC = 0xFFFFFFFF
DEVICE_APPS_TYPE = 1
TEST_FILE = "test.pb.gz"


class TestPB(unittest.TestCase):
    deviceapps = [
        {
            "device": {"type": "idfa", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7c"},
            "lat": 67.7835424444,
            "lon": -22.8044005471,
            "apps": [1, 2, 3, 4],
        },
        {
            "device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"},
            "lat": 42,
            "lon": -42,
            "apps": [1, 2],
        },
        {
            "device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"},
            "lat": 42,
            "lon": -42,
            "apps": [],
        },
        {
            "device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"},
            "apps": [1],
        },
    ]

    magic_str = struct.pack("I", MAGIC)

    def tearDown(self):
        os.remove(TEST_FILE)

    def test_write(self):
        bytes_written = pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        self.assertTrue(bytes_written > 0)

    def test_file_write_has_magic(self):
        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        with gzip.open(TEST_FILE, "rb") as f:
            file_magic = f.read(4)
            self.assertEqual(file_magic, self.magic_str)

    def test_write_right_pb_id(self):
        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        with gzip.open(TEST_FILE, "rb") as f:
            file_magic = f.read(4)
            pb_type, pb_len = struct.unpack("HH", f.read(4))
            message = f.read(pb_len)

            first_device_app = self.deviceapps[0]
            id_device = first_device_app["device"]["id"]
            _, id_from_file = struct.unpack(
                f"I{len(id_device)}s", message[: 4 + len(id_device)]
            )
            self.assertEqual(id_device, id_from_file.decode())

    def test_message_not_a_dict(self):
        with self.assertRaises(TypeError):
            pb.deviceapps_xwrite_pb(["No dict"], TEST_FILE)

    def test_message_device_not_a_dict(self):
        with self.assertRaises(TypeError):
            pb.deviceapps_xwrite_pb(
                [{"device": "error", "lat": 42, "lon": -42, "apps": [1, 2]}], TEST_FILE
            )

    @unittest.skip("Optional problem")
    def test_read(self):
        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        for i, d in enumerate(pb.deviceapps_xread_pb(TEST_FILE)):
            self.assertEqual(d, self.deviceapps[i])
