from __future__ import print_function, division, absolute_import, unicode_literals
from abc import ABCMeta, abstractmethod
from functools import partial
import warnings
from collections import namedtuple

from bistiming import SimpleTimer
import h5sparse
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

    @abstractmethod
    def write_data(self, data_definition, data, **kwargs):
        pass

    def bundle(self, data, path, new_key):
        """write the data to another HDF5 file with new key."""
        with h5sparse.File(path, 'a') as h5f:
            h5f.create_dataset(new_key, data=data)
        self.close()

    def update_context(self, context, data_definition, **kwargs):
        pass

    def is_return_data_expected(self, **kwargs):
        return True

    def close(self):
        pass


class H5pyDataHandlerArgs(
        namedtuple('H5pyDataHandlerArgs', ['allow_nan',
                                           'create_dataset_context'])):
    def __new__(
            cls, allow_nan=False, create_dataset_context=None):
        return super(H5pyDataHandlerArgs, cls).__new__(
            cls, allow_nan, create_dataset_context)


class H5pyDataHandler(DataHandler):

    def __init__(self, hdf_dir):
        self.hdf_dir = Path(hdf_dir)
        self.hdf_dir.mkdir(parents=True, exist_ok=True)
        self.h5f_dict = {}

    def _get_hdf_path(self, data_definition):
        return self.hdf_dir / (data_definition.to_json() + ".h5")

    def can_skip(self, data_definition):
        hdf_path = self._get_hdf_path(data_definition)
        if hdf_path.exists():
            return True
        return False

    def _get_read_only_h5py_file(self, data_definition):
        if data_definition in self.h5f_dict:
            return self.h5f_dict[data_definition]
        h5f = h5sparse.File(self._get_hdf_path(data_definition), 'r')
        self.h5f_dict[data_definition] = h5f
        return h5f

    def get(self, data_definition):
        if isinstance(data_definition, DataDefinition):
            return self._get_read_only_h5py_file(data_definition)['data']
        return {data_def: self._get_read_only_h5py_file(data_def)['data']
                for data_def in data_definition}

    def update_context(self, context, data_definition, **kwargs):
        args = H5pyDataHandlerArgs(**kwargs)
        if args.create_dataset_context is None:
            return
        functions = context.setdefault(args.create_dataset_context, {})
        assert data_definition.key not in functions

        # open h5
        assert data_definition not in self.h5f_dict
        hdf_path = self._get_hdf_path(data_definition)
        assert not hdf_path.exists()
        h5f = h5sparse.File(hdf_path, 'w')
        self.h5f_dict[data_definition] = h5f

        functions[data_definition.key] = partial(h5f.create_dataset, 'data')

    def write_data(self, data_definition, data, **kwargs):
        args = H5pyDataHandlerArgs(**kwargs)
        hdf_path = self._get_hdf_path(data_definition)
        if hdf_path.exists():
            raise NotImplementedError(
                "Overwriting not supported. Please report an issue.")
        if not args.allow_nan:
            # check nan
            if ss.isspmatrix(data):
                if np.isnan(data.data).any():
                    raise ValueError("data {} have nan".format(data_definition))
            elif np.isnan(data).any():
                raise ValueError("data {} have nan".format(data_definition))

        # write data
        with h5sparse.File(hdf_path, 'w') as h5f, \
                SimpleTimer("[{}] Writing generated data {} to hdf5 file"
                            .format(type(self).__name__, data_definition),
                            end_in_new_line=False):
            h5f.create_dataset('data', data=data)

    def is_return_data_expected(self, **kwargs):
        args = H5pyDataHandlerArgs(**kwargs)
        return args.create_dataset_context is None

    def close(self):
        if self.h5f_dict:
            for data_definition, h5f in six.viewitems(self.h5f_dict):
                h5f.close()
            self.h5f_dict = {}


class PandasHDFDataHandlerArgs(
        namedtuple('PandasHDFDataHandlerArgs', ['allow_nan',
                                                'append_context',
                                                'data_columns'])):
    def __new__(cls, allow_nan=False, append_context=None, data_columns=None):
        return super(PandasHDFDataHandlerArgs, cls).__new__(
            cls, allow_nan, append_context, data_columns)


class PandasHDFDataHandler(DataHandler):

    def __init__(self, hdf_dir):
        self.hdf_dir = Path(hdf_dir)
        self.hdf_dir.mkdir(parents=True, exist_ok=True)
        self.hdf_store_dict = {}

    def _get_hdf_path(self, data_definition):
        return self.hdf_dir / (data_definition.to_json() + ".h5")

    def can_skip(self, data_definition):
        hdf_path = self._get_hdf_path(data_definition)
        if hdf_path.exists():
            return True
        return False

    def _get_read_only_hdf_store(self, data_definition):
        if data_definition in self.hdf_store_dict:
            return self.hdf_store_dict[data_definition]
        hdf_store = pd.HDFStore(self._get_hdf_path(data_definition), 'r')
        self.hdf_store_dict[data_definition] = hdf_store
        return hdf_store

    def get(self, data_definition):
        if isinstance(data_definition, DataDefinition):
            return PandasHDFDataset(self._get_read_only_hdf_store(data_definition), 'data')
        return {data_def: PandasHDFDataset(self._get_read_only_hdf_store(data_def), 'data')
                for data_def in data_definition}

    def update_context(self, context, data_definition, **kwargs):
        args = PandasHDFDataHandlerArgs(**kwargs)
        if args.append_context is None:
            return
        functions = context.setdefault(args.append_context, {})
        assert data_definition.key not in functions

        # open hdf store
        assert data_definition not in self.hdf_store_dict
        hdf_path = self._get_hdf_path(data_definition)
        assert not hdf_path.exists()
        hdf_store = pd.HDFStore(hdf_path, 'w')
        self.hdf_store_dict[data_definition] = hdf_store

        functions[data_definition.key] = partial(hdf_store.append, 'data')

    def write_data(self, data_definition, data, **kwargs):
        args = PandasHDFDataHandlerArgs(**kwargs)
        hdf_path = self._get_hdf_path(data_definition)
        if hdf_path.exists():
            raise NotImplementedError(
                "Overwriting not supported. Please report an issue.")
        if not args.allow_nan:
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
        with pd.HDFStore(hdf_path, 'w') as hdf_store, \
                SimpleTimer("[{}] Writing generated data {} to hdf5 file"
                            .format(type(self).__name__, data_definition),
                            end_in_new_line=False), \
                warnings.catch_warnings():
            warnings.simplefilter('ignore', NaturalNameWarning)
            if (isinstance(data, pd.DataFrame)
                    and isinstance(data.index, pd.MultiIndex)
                    and isinstance(data.columns, pd.MultiIndex)):
                hdf_store.put('data', data)
            else:
                hdf_store.put('data', data, format='table', data_columns=args.data_columns)

    def bundle(self, data, path, new_key):
        """Write the data to another HDF5 file with new key."""
        data.value.to_hdf(path, new_key)
        self.close()

    def is_return_data_expected(self, **kwargs):
        args = PandasHDFDataHandlerArgs(**kwargs)
        return args.append_context is None

    def close(self):
        if self.hdf_store_dict:
            for data_definition, hdf_store in six.viewitems(self.hdf_store_dict):
                hdf_store.close()
            self.hdf_store_dict = {}


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
