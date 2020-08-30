C-extension-pb
=====================
C extension for writing a protobuf in compression file.

Requirements
-------
* python3-devel
* gcc
* make
* protobuf
* protobuf-c
* protobuf-c-compiler
* protobuf-c-devel
* zlib-devel

Install
----------
For Centos7
```bash
yum install -y  gcc make protobuf protobuf-c protobuf-c-compiler protobuf-c-devel python3-devel zlib-devel python3-setuptools

python3 setup.py test && python3 setup.py install

```


How to use
----------
The `pb` library has the `deviceapps_xwrite_pb` function. The function accepts a protobuf message  and string Path to the file.
```
deviceapps_xwrite_pb(messages: List[pb], path: string)
```

Example
--------
python3
```python3
import pb

file_to_save = 'test.pb.gz'
deviceapps =  [{
            "device": {"type": "idfa", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7c"},
            "lat": 67.7835424444,
            "lon": -22.8044005471,
            "apps": [1, 2, 3, 4],
        }]

pb.deviceapps_xwrite_pb(deviceapps, file_to_save)

```
