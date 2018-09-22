from __future__ import print_function, division, absolute_import, unicode_literals
import os
from past.builtins import basestring

import numpy as np
import pandas as pd
import h5py
import six
from bistiming import SimpleTimer
from tqdm import trange

from .data_definition import DataDefinition


def get_data_definitions_from_list_in_structure(structure):
    data_definitions = []
    for raw_data_def in structure:
        if isinstance(raw_data_def, dict):
            data_definitions.append(
                DataDefinition(raw_data_def['key'], raw_data_def['args']))
        elif isinstance(raw_data_def, basestring):
            data_definitions.append(DataDefinition(raw_data_def))
        else:
            raise TypeError("The bundle structure in list only support "
                            "dict and str, but got {}.".format(structure))
    return data_definitions


def get_data_definitions_from_dict_in_structure(structure):
    if 'key' in structure:
        data_definitions = [DataDefinition(structure['key'], structure['args'])]
    else:
        data_definitions = []
        for _, val in six.viewitems(structure):
            data_definitions.extend(get_data_definitions_from_structure(val))
    return data_definitions


def get_data_definitions_from_structure(structure):
    if isinstance(structure, basestring):
        data_definitions = [DataDefinition(structure)]
    elif isinstance(structure, list):
        data_definitions = get_data_definitions_from_list_in_structure(structure)
    elif isinstance(structure, dict):
        data_definitions = get_data_definitions_from_dict_in_structure(structure)
    else:
        raise TypeError("The bundle structure only support "
                        "dict, list and str, but got {}.".format(structure))
    return data_definitions


class DataBundlerMixin(object):

    def fill_concat_data(self, data_bundle_hdf_path, dset_name, data_definitions,
                         buffer_size=int(1e+9)):
        data_shapes = []
        for data_definition in data_definitions:
            data_shape = self.get(data_definition).shape
            if len(data_shape) == 1:
                data_shape += (1,)
            data_shapes.append(data_shape)
        max_shape_length = max(map(len, data_shapes))
        if max_shape_length > 2:
            raise NotImplementedError("tensor data is not supported yet")

        n_rows = data_shapes[0][0]
        for data_shape in data_shapes:
            if data_shape[0] != n_rows:
                raise ValueError("different number of instances: {} and {}."
                                 .format(data_shapes[0], data_shape))
        n_cols = sum(shape[1] for shape in data_shapes)
        concat_shape = (n_rows, n_cols)

        h5f = h5py.File(data_bundle_hdf_path)
        dset = h5f.create_dataset(dset_name, shape=concat_shape,
                                  dtype=np.float32)

        data_d = 0
        for data_i, (data_definition, data_shape) in enumerate(zip(data_definitions, data_shapes)):
            data = self.get(data_definition)
            if isinstance(data, pd.DataFrame):
                data = data.values
            batch_size = buffer_size // (data.dtype.itemsize * data_shape[1])
            if batch_size == 0:
                print("Warning! buffer_size not enough to fitted by an "
                      "instance. Trying to use more memory.")
                batch_size = 1
            desc = "({}/{}) Filling {}".format(data_i + 1, len(data_definitions), data_definition)
            for batch_start in trange(0, data_shape[0], batch_size, desc=desc):
                batch_end = min(data_shape[0], batch_start + batch_size)
                data_buffer = data[batch_start: batch_end]
                if isinstance(data_buffer, pd.DataFrame):
                    data_buffer.values
                if len(data_buffer.shape) == 1:
                    data_buffer = data_buffer[:, np.newaxis]
                dset[batch_start: batch_end,
                     data_d: data_d + data_shape[1]] = data_buffer

            data_d += data_shape[1]

        h5f.close()

    def _bundle_list_in_structure(
            self, structure, data_bundle_hdf_path, buffer_size, structure_config, dset_name):
        data_definitions = get_data_definitions_from_list_in_structure(structure)
        if structure_config.get('concat', False):
            # write into single dataset
            self.fill_concat_data(data_bundle_hdf_path, dset_name, data_definitions, buffer_size)
        else:
            key_set = set()
            for data_definition in data_definitions:
                if data_definition.key in key_set:
                    raise ValueError("Duplicated key {} in a list structure."
                                     "Use dict structure to distinguish them instead."
                                     .format(data_definition.key))
                key_set.add(data_definition.key)
                self.get_handler(data_definition.key).bundle(
                    data_definition, data_bundle_hdf_path, dset_name + "/" + data_definition.key)

    def _bundle_dict_in_structure(
            self, structure, data_bundle_hdf_path, buffer_size, structure_config, dset_name):
        if 'key' in structure:
            data_definition = DataDefinition(structure['key'], structure['args'])
            self.get_handler(data_definition.key).bundle(
                data_definition, data_bundle_hdf_path, dset_name)
        else:
            for key, val in six.viewitems(structure):
                self._bundle(
                    val, data_bundle_hdf_path, buffer_size,
                    structure_config=structure_config.get(key, {}),
                    dset_name=dset_name + "/" + key)

    def _bundle(
            self, structure, data_bundle_hdf_path, buffer_size, structure_config, dset_name=""):
        if isinstance(structure, basestring) and dset_name != "":
            self.get_handler(structure).bundle(
                DataDefinition(structure), data_bundle_hdf_path, dset_name)
        elif isinstance(structure, list):
            self._bundle_list_in_structure(
                structure, data_bundle_hdf_path, buffer_size, structure_config, dset_name)
        elif isinstance(structure, dict):
            self._bundle_dict_in_structure(
                structure, data_bundle_hdf_path, buffer_size, structure_config, dset_name)
        else:
            raise TypeError("The bundle structure only support "
                            "dict, list and str (except the first layer).")

    def bundle(self, structure, data_bundle_hdf_path, buffer_size=int(1e+9),
               structure_config=None):
        if structure_config is None:
            structure_config = {}

        data_bundle_hdf_path = str(data_bundle_hdf_path)
        if os.path.isfile(data_bundle_hdf_path):
            os.remove(data_bundle_hdf_path)
        with SimpleTimer("Bundling data"):
            self._bundle(structure, data_bundle_hdf_path, buffer_size, structure_config)
        self.close()
