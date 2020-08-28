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

// https://github.com/protobuf-c/protobuf-c/wiki/Examples
// void example() {
// DeviceApps msg = DEVICE_APPS__INIT;
// DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
// void *buf;
// unsigned len;

// char *device_id = "e7e1a50c0ec2747ca56cd9e1558c0d7c";
// char *device_type = "idfa";
// device.has_id = 1;
// device.id.data = (uint8_t*)device_id;
// device.id.len = strlen(device_id);
// device.has_type = 1;
// device.type.data = (uint8_t*)device_type;
// device.type.len = strlen(device_type);
// msg.device = &device;

// msg.has_lat = 1;
// msg.lat = 67.7835424444;
// msg.has_lon = 1;
// msg.lon = -22.8044005471;

// msg.n_apps = 3;
// msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);
// msg.apps[0] = 42;
// msg.apps[1] = 43;
// msg.apps[2] = 44;
// len = device_apps__get_packed_size(&msg);

// buf = malloc(len);
// device_apps__pack(&msg, buf);

// fprintf(stderr,"Writing %d serialized bytes\n",len); // See the length of message
// fwrite(buf, len, 1, stdout); // Write to stdout to allow direct command line piping

// free(msg.apps);
// free(buf);
// }

// Read iterator of Python dicts
// Pack them to DeviceApps protobuf and write to file with appropriate header
// Return number of written bytes as Python integer
static PyObject *py_deviceapps_xwrite_pb(PyObject *self, PyObject *args)
{
    const char *path;
    static PyObject *error;
    long unsigned int len, write_len;
    void *buf;

    PyObject *o;

    if (!PyArg_ParseTuple(args, "Os", &o, &path))
        return NULL;

    printf("Write to: %s\n", path);

    long lenght = PyList_Size(o);
    printf("Length: %li\n", lenght);

    for (long int i = 0; i < lenght; i++)
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

        msg.lat = PyFloat_AsDouble(lat);
        printf(" lat: %lf", msg.lat);
        msg.has_lat = 1;
        msg.lon = PyFloat_AsDouble(lon);
        printf(" lon: %lf\n", msg.lon);
        msg.has_lon = 1;

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
        for (long unsigned int i = 0; i < msg.n_apps; i++)
        {
            msg.apps[i] = PyLong_AsUnsignedLongLong(PyList_GetItem(apps_arr, i));
            printf(" apps: %d ", msg.apps[i]);
        }

        len = device_apps__get_packed_size(&msg);
        buf = malloc(len);

        device_apps__pack(&msg, buf);

        fprintf(stderr, "Writing %ld serialized bytes\n", len); // See the length of message
        fwrite(buf, len, 1, stdout);                            // Write to stdout to allow direct command line piping

        FILE *f = fopen(path, "a");

        if (f == NULL)
        {
            PyErr_SetString(error, "Can't open file");
            return NULL;
        }

        write_len = fwrite(buf, len, 1, f);
        fprintf(stderr, "Writed %ld serialized bytes\n", write_len); // See the length of message

        fclose(f);

        free(msg.apps);
        free(buf);
        printf("\n===========================\n");
    }

    Py_RETURN_NONE;
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
    (void)PyModule_Create(&pbmodule);
}