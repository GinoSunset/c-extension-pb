#include <zlib.h>
#include <python3.6m/Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "deviceapps.pb-c.h"

#define MAGIC 0xFFFFFFFF
#define DEVICE_APPS_TYPE 1

typedef struct pbheader_s
{
    uint32_t magic;
    uint16_t type;
    uint16_t length;
} pbheader_t;
#define PBHEADER_INIT \
    {                 \
        MAGIC, 0, 0   \
    }

static PyObject *py_deviceapps_xwrite_pb(PyObject *self, PyObject *args)
{
    const char *path;
    static PyObject *error;
    PyObject *protobuf = NULL;
    long unsigned int len;
    long unsigned int write_len = 0;
    long unsigned int total_length = 0;
    long unsigned int write_header_len = 0;
    void *buf;
    gzFile gzip_file;

    PyObject *o;
    pbheader_t header = PBHEADER_INIT;

    if (!PyArg_ParseTuple(args, "Os", &o, &path))
        return NULL;

    printf("Write to: %s\n", path);
    gzip_file = gzopen(path, "wb");
    if (gzip_file == NULL)
    {
        PyErr_SetString(error, "Can't open file");
        return NULL;
    }
    if (!PyList_Check(o))
    {
        PyErr_SetString(PyExc_TypeError, "Objects must be a list");
        return NULL;
    }

    long lenght = PyList_Size(o);

    long int i;
    for (i = 0; i < lenght; i++)
    {
        DeviceApps msg = DEVICE_APPS__INIT;
        DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;

        PyObject *protobuf = PyList_GetItem(o, i);

        if (protobuf == NULL || !PyDict_Check(protobuf))
        {
            PyErr_SetString(PyExc_TypeError, "Protobuf message must be a dictionary");
            return NULL;
        }

        PyObject_Print(protobuf, stdout, 0);

        PyObject *device_ = PyDict_GetItemString(protobuf, "device");
        if (!PyDict_Check(device_))
        {
            PyErr_SetString(PyExc_TypeError, "Device message must be a dictionary");
            return NULL;
        }

        PyObject *dev_test = PyDict_GetItemString(device_, "type");
        if (!PyUnicode_Check(dev_test))
        {
            PyErr_SetString(PyExc_TypeError, "Device type message must be a string");
            return NULL;
        }
        const char *device_type = PyUnicode_AsUTF8(dev_test);
        if (device_type == NULL)
        {
            device.has_type = 0;
        }
        else
        {
            device.has_type = 1;
            device.type.data = (uint8_t *)device_type;
            device.type.len = strlen(device_type);
        }

        PyObject *dev_id = PyDict_GetItemString(device_, "id");
        if (!PyUnicode_Check(dev_id))
        {
            PyErr_SetString(PyExc_TypeError, "Device id message must be a string");
            return NULL;
        }

        const char *device_id = PyUnicode_AsUTF8(dev_id);
        if (device_id == NULL)
        {
            device.has_id = 0;
        }
        else
        {
            device.has_id = 1;
            device.id.data = (uint8_t *)device_id;
            device.id.len = strlen(device_id);
        }

        msg.device = &device;

        PyObject *lat = PyDict_GetItemString(protobuf, "lat");
        PyObject *lon = PyDict_GetItemString(protobuf, "lon");

        if (lat != NULL)
        {
            msg.lat = PyFloat_AsDouble(lat);
            msg.has_lat = 1;
        }
        else
        {
            msg.has_lat = 0;
        }

        if (lon != NULL)
        {
            msg.lon = PyFloat_AsDouble(lon);
            msg.has_lon = 1;
        }
        else
        {
            msg.has_lon = 0;
        }
        PyObject *apps_arr = PyDict_GetItemString(protobuf, "apps");

        msg.n_apps = PyList_Size(apps_arr);

        msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);
        if (msg.apps == NULL && msg.n_apps > 0)
        {
            PyErr_SetString(error, "Can't allocate apps");
            return NULL;
        }
        long unsigned int i;
        for (i = 0; i < msg.n_apps; i++)
        {
            msg.apps[i] = PyLong_AsUnsignedLongLong(PyList_GetItem(apps_arr, i));
        }

        len = device_apps__get_packed_size(&msg);
        buf = malloc(len);

        device_apps__pack(&msg, buf);
        header.type = DEVICE_APPS_TYPE;
        header.length = len;

        write_header_len = gzwrite(gzip_file, &header, sizeof(pbheader_t));
        write_len = gzwrite(gzip_file, buf, len);
        total_length += write_len;
        total_length += write_header_len;

        free(msg.apps);
        free(buf);
    }
    fprintf(stderr, "===Total write %ld serialized bytes\n", total_length);
    gzclose(gzip_file);
    printf("\n===========================\n");

    return Py_BuildValue("i", total_length);
}

// Unpack only messages with type == DEVICE_APPS_TYPE
// Return iterator of Python dicts
static PyObject *py_deviceapps_xread_pb(PyObject *self, PyObject *args)
{
    const char *path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    printf("Read from: %s\n", path);
    Py_RETURN_NONE;
}

static PyMethodDef PBMethods[] = {
    {"deviceapps_xwrite_pb", py_deviceapps_xwrite_pb, METH_VARARGS, "Write serialized protobuf to file fro iterator"},
    {"deviceapps_xread_pb", py_deviceapps_xread_pb, METH_VARARGS, "Deserialize protobuf from file, return iterator"},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef pbmodule = {
    PyModuleDef_HEAD_INIT,
    "pb",
    "This module to write protobuff to file",
    -1,
    PBMethods};

PyMODINIT_FUNC PyInit_pb(void)
{
    return PyModule_Create(&pbmodule);
}