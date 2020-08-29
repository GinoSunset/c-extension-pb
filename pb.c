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
    long unsigned int len;
    long unsigned int write_len = 0;
    void *buf;
    gzFile gzip_file;

    PyObject *o;

    if (!PyArg_ParseTuple(args, "Os", &o, &path))
        return NULL;

    printf("Write to: %s\n", path);
    gzip_file = gzopen(path, "wb");
    printf("gzip: %p\n ", gzip_file);
    if (gzip_file == NULL)
    {
        PyErr_SetString(error, "Can't open file");
        return NULL;
    }

    long lenght = PyList_Size(o);
    printf("Length: %li\n", lenght);

    long int i;
    for (i = 0; i < lenght; i++)
    {
        DeviceApps msg = DEVICE_APPS__INIT;
        DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;

        PyObject *protobuf = PyList_GetItem(o, i);
        PyObject_Print(protobuf, stdout, 0);

        PyObject *device_ = PyDict_GetItemString(protobuf, "device");

        const char *device_type = PyUnicode_AsUTF8(PyDict_GetItemString(device_, "type"));
        if (device_type == NULL)
        {
            printf("oooops");
            // Py_DECREF
        }
        device.has_type = 1;
        device.type.data = (uint8_t *)device_type;
        device.type.len = strlen(device_type);
        printf("\n type: %s", device_type);

        const char *device_id = PyUnicode_AsUTF8(PyDict_GetItemString(device_, "id"));
        if (device_id == NULL)
        {
            printf("oooops");
        };
        device.has_id = 1;
        device.id.data = (uint8_t *)device_id;
        device.id.len = strlen(device_id);
        printf(" id: %s\n", device_id);

        msg.device = &device;

        PyObject *lat = PyDict_GetItemString(protobuf, "lat");
        PyObject *lon = PyDict_GetItemString(protobuf, "lon");

        if (lat != NULL)
        {
            msg.lat = PyFloat_AsDouble(lat);
            printf(" lat: %lf", msg.lat);
            msg.has_lat = 1;
        }
        else
        {
            msg.has_lat = 0;
        }

        if (lon != NULL)
        {
            msg.lon = PyFloat_AsDouble(lon);
            printf(" lon: %lf\n", msg.lon);
            msg.has_lon = 1;
        }
        else
        {
            msg.has_lon = 0;
        }
        PyObject *apps_arr = PyDict_GetItemString(protobuf, "apps");

        msg.n_apps = PyList_Size(apps_arr);
        printf("size of apps: %ld \n", msg.n_apps);

        msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);
        printf("alloc: %p \n", msg.apps);
        if (msg.apps == NULL && msg.n_apps > 0)
        {
            PyErr_SetString(error, "Can't allocate apps");
            return NULL;
        }
        long unsigned int i;
        for (i = 0; i < msg.n_apps; i++)
        {
            msg.apps[i] = PyLong_AsUnsignedLongLong(PyList_GetItem(apps_arr, i));
            printf(" apps: %d ", msg.apps[i]);
        }

        len = device_apps__get_packed_size(&msg);
        buf = malloc(len);

        device_apps__pack(&msg, buf);

        write_len = gzwrite(gzip_file, buf, len);
        fprintf(stderr, "Writed %ld serialized bytes\n", write_len);

        free(msg.apps);
        free(buf);
        printf("\n===========================\n");
    }

    gzclose(gzip_file);
    printf("\n===========================\n");

    return Py_BuildValue("i", write_len);
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