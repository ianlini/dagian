from __future__ import print_function, division, absolute_import, unicode_literals
import os.path
from abc import ABCMeta, abstractmethod
from functools import partial
import warnings

from bistiming import SimpleTimer
import h5py
import h5sparse
from mkdir_p import mkdir_p
import numpy as np
import pandas as pd
import scipy.sparse as ss
import six
from six.moves import cPickle
from tables import NaturalNameWarning
from pathlib2 import Path

from .data_wrappers import PandasHDFDataset
from .data_definition import DataDefinition


SPARSE_FORMAT_SET = set(['csr', 'csc'])


class DataHandler(six.with_metaclass(ABCMeta, object)):

    @abstractmethod
    def can_skip(self, data_definition):
        pass

    @abstractmethod
    def get(self, data_definition):
        pass

    def get_function_kwargs(self, will_generate_keys, data):
        # pylint: disable=unused-argument
        kwargs = {}
        if len(data) > 0:
            kwargs['data'] = data
        return kwargs

    @abstractmethod
    def write_data(self, data_definition, data, **kwargs):
        pass

    def bundle(self, data_definition, path, new_key):
        """Copy the data to another HDF5 file with new key."""
        data = self.get(data_definition)
        with h5sparse.File(path) as h5f:
            h5f.create_dataset(new_key, data=data)
        self.close()

    def is_return_data_expected(self, **kwargs):
        return True

    def close(self):
        pass


class H5pyDataHandler(DataHandler):

    def __init__(self, hdf_path):
        hdf_dir = os.path.dirname(hdf_path)
        if hdf_dir != '':
            mkdir_p(hdf_dir)
        # create an empty file if the file doesn't exists
        h5py.File(hdf_path, 'a').close()
        self.hdf_path = hdf_path
        self.h5f = None

    def can_skip(self, data_definition):
        with h5py.File(self.hdf_path, 'r') as h5f:
            if data_definition.to_json() in h5f:
                return True
        return False

    def get_read_only_h5py_file(self):
        if self.h5f is None:
            self.h5f = h5py.File(self.hdf_path, 'r')
        return self.h5f

    def get(self, data_definition):
        h5f = self.get_read_only_h5py_file()
        if isinstance(data_definition, DataDefinition):
            return h5sparse.Group(h5f)[data_definition.to_json()]
        return {k: h5sparse.Group(h5f)[k.to_json()] for k in data_definition}

    def get_function_kwargs(self, will_generate_keys, data,
                            manually_create_dataset=False):
        kwargs = {}
        if len(data) > 0:
            kwargs['data'] = data
        if manually_create_dataset is True:
            kwargs['create_dataset_functions'] = {
                k: partial(self.h5f.create_dataset, k)
                for k in will_generate_keys
            }
        elif manually_create_dataset in SPARSE_FORMAT_SET:
            kwargs['create_dataset_functions'] = {
                k: partial(h5sparse.Group(self.h5f).create_dataset, k)
                for k in will_generate_keys
            }
        return kwargs

    def write_data(self, data_definition, data, allow_nan=False):
        with h5py.File(self.hdf_path, 'a') as h5f:
            if not allow_nan:
                # check nan
                if ss.isspmatrix(data):
                    if np.isnan(data.data).any():
                        raise ValueError("data {} have nan".format(data_definition))
                elif np.isnan(data).any():
                    raise ValueError("data {} have nan".format(data_definition))

            # write data
            with SimpleTimer("[{}] Writing generated data {} to hdf5 file"
                             .format(type(self).__name__, data_definition),
                             end_in_new_line=False):
                if data_definition.to_json() in h5f:
                    # h5f[key][...] = data
                    raise NotImplementedError(
                        "Overwriting not supported. Please report an issue.")
                else:
                    h5sparse.Group(h5f).create_dataset(data_definition.to_json(), data=data)

    def close(self):
        if self.h5f is not None:
            self.h5f.close()
            self.h5f = None


class PandasHDFDataHandler(DataHandler):

    def __init__(self, hdf_path):
        hdf_dir = os.path.dirname(hdf_path)
        if hdf_dir != '':
            mkdir_p(hdf_dir)
        # create an empty file if the file doesn't exists
        pd.HDFStore(hdf_path, 'a').close()
        self.hdf_path = hdf_path
        self.hdf_store = None

    def can_skip(self, data_definition):
        with pd.HDFStore(self.hdf_path, 'r') as hdf_store:
            if data_definition.to_json() in hdf_store:
                return True
        return False

    def get_read_only_hdf_store(self):
        if self.hdf_store is None:
            self.hdf_store = pd.HDFStore(self.hdf_path, 'r')
        return self.hdf_store

    def get(self, data_definition):
        hdf_store = self.get_read_only_hdf_store()
        if isinstance(data_definition, DataDefinition):
            return PandasHDFDataset(hdf_store, data_definition.to_json())
        return {k: PandasHDFDataset(hdf_store, k.to_json()) for k in data_definition}

    def get_function_kwargs(self, will_generate_keys, data,
                            manually_append=False):
        kwargs = {}
        if len(data) > 0:
            kwargs['data'] = data
        if manually_append is True:
            kwargs['append_functions'] = {
                k: partial(self.hdf_store.append, k)
                for k in will_generate_keys
            }
        return kwargs

    def write_data(self, data_definition, data, allow_nan=False):
        with pd.HDFStore(self.hdf_path, 'a') as hdf_store:
            if not allow_nan:
                # check nan
                is_null = False
                if isinstance(data, pd.DataFrame):
                    if data.isnull().any().any():
                        is_null = True
                elif isinstance(data, pd.Series):
                    if data.isnull().any():
                        is_null = True
                else:
                    raise ValueError("PandasHDFDataHandler doesn't support type {} (in key {})"
                                     .format(type(data), data_definition))
                if is_null:
                    raise ValueError("data {} have nan".format(data_definition))

            # write data
            with SimpleTimer("[{}] Writing generated data {} to hdf5 file"
                             .format(type(self).__name__, data_definition),
                             end_in_new_line=False), \
                    warnings.catch_warnings():
                warnings.simplefilter('ignore', NaturalNameWarning)
                if (isinstance(data, pd.DataFrame)
                        and isinstance(data.index, pd.MultiIndex)
                        and isinstance(data.columns, pd.MultiIndex)):
                    hdf_store.put(data_definition.to_json(), data)
                else:
                    hdf_store.put(data_definition.to_json(), data, format='table')

    def bundle(self, data_definition, path, new_key):
        """Copy the data to another HDF5 file with new key."""
        data = self.get(data_definition).value
        data.to_hdf(path, new_key)
        self.close()

    def close(self):
        if self.hdf_store is not None:
            self.hdf_store.close()
            self.hdf_store = None


class MemoryDataHandler(DataHandler):

    def __init__(self):
        self.data = {}

    def can_skip(self, data_definition):
        if data_definition in self.data:
            return True
        return False

    def get(self, data_definition):
        if isinstance(data_definition, DataDefinition):
            return self.data[data_definition]
        return {k: self.data[k] for k in data_definition}

    def write_data(self, data_definition, data):
        self.data[data_definition] = data


class PickleDataHandler(DataHandler):

    def __init__(self, pickle_dir):
        self.pickle_dir = Path(pickle_dir)
        self.pickle_dir.mkdir(parents=True, exist_ok=True)

    def can_skip(self, data_definition):
        data_path = self.pickle_dir / (data_definition.to_json() + ".pkl")
        if data_path.exists():
            return True
        return False

    def get(self, data_definition):
        if isinstance(data_definition, DataDefinition):
            with (self.pickle_dir / (data_definition.to_json() + ".pkl")).open('rb') as fp:
                return cPickle.load(fp)
        data = {}
        for data_def in data_definition:
            with (self.pickle_dir / (data_def.to_json() + ".pkl")).open('rb') as fp:
                data[data_def] = cPickle.load(fp)
        return data

    def write_data(self, data_definition, data):
        pickle_path = self.pickle_dir / (data_definition.to_json() + ".pkl")
        with SimpleTimer("Writing generated data %s to pickle file" % data_definition,
                         end_in_new_line=False), \
                pickle_path.open('wb') as fp:
            cPickle.dump(data, fp, protocol=cPickle.HIGHEST_PROTOCOL)
