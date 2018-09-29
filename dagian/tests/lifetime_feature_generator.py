from __future__ import print_function, division, absolute_import, unicode_literals
from io import StringIO

import dagian
from dagian import Argument as A
from dagian.decorators import (
    require,
    will_generate,
)
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.model_selection import train_test_split


class LifetimeFeatureGenerator(dagian.FeatureGenerator):

    @will_generate('memory', 'data_df')
    def gen_data_df(self):
        csv = StringIO("""\
id,lifetime,tested_age,weight,height,gender,income
0, 68, 50, 60.1, 170.5, f, 22000
1, 59, 41, 90.4, 168.9, m, 19000
2, 52, 39, 46.2, 173.6, m, 70000
3, 68, 25, 93.9, 180.0, m, 1000000
4, 99, 68, 65.7, 157.6, f, 46000
5, 90, 81, 56.3, 170.2, f, 17000
""")
        return {'data_df': pd.read_csv(csv, index_col='id')}

    @require('data_df')
    @will_generate('h5py', 'lifetime')
    def gen_lifetime(self, upstream_data):
        data_df = upstream_data['data_df']
        return {'lifetime': data_df['lifetime']}

    @require('data_df')
    @will_generate('h5py', ['weight', 'height'])
    def gen_raw_data_features(self, upstream_data):
        data_df = upstream_data['data_df']
        return data_df[['weight', 'height']]

    @require('data_df')
    @will_generate('memory', 'mem_raw_data')
    def gen_mem_raw_data(self, upstream_data):
        data_df = upstream_data['data_df']
        return {'mem_raw_data': data_df[['weight', 'height']].values}

    # @require('data_df')
    # @will_generate('h5py', 'man_raw_data', manually_create_dataset=True)
    # def gen_man_raw_data(self, upstream_data, create_dataset_functions):
    #     data_df = upstream_data['data_df']
    #     dset = create_dataset_functions['man_raw_data'](shape=(data_df.shape[0], 2))
    #     dset[...] = data_df[['weight', 'height']].values

    @require('data_df')
    @will_generate('pandas_hdf', ['pd_weight', 'pd_height'])
    def gen_raw_data_table(self, upstream_data):
        data_df = upstream_data['data_df']
        result_df = data_df.loc[:, ['weight', 'height']]
        result_df.rename(columns={'weight': 'pd_weight', 'height': 'pd_height'},
                         inplace=True)
        return result_df

    @require('data_df')
    @will_generate('pandas_hdf', 'pd_raw_data')
    def gen_raw_data_df(self, upstream_data):
        data_df = upstream_data['data_df']
        return {'pd_raw_data': data_df[['weight', 'height']]}

    # @require('pd_raw_data')
    # @will_generate('pandas_hdf', 'pd_raw_data_append', manually_append=True)
    # def gen_raw_data_append_df(self, upstream_data, append_functions):
    #     df = upstream_data['pd_raw_data'].value
    #     append_functions['pd_raw_data_append'](df.iloc[:3])
    #     append_functions['pd_raw_data_append'](df.iloc[3:])

    @require('data_df')
    @will_generate('h5py', 'BMI')
    def gen_bmi(self, upstream_data):
        data_df = upstream_data['data_df']
        bmi = data_df['weight'] / ((data_df['height'] / 100) ** 2)
        return {'BMI': bmi}

    @require('{dividend}')
    @require('{divisor}')
    @will_generate('h5py', 'division')
    def gen_division(self, upstream_data, dividend, divisor='height'):
        division_result = upstream_data['{dividend}'].value / upstream_data['{divisor}'].value
        return {'division': division_result}

    @require('division', 'partial_division', dividend=A('dividend'), divisor=A('divisor1'))
    @require('{divisor2}', 'divisor2')
    @will_generate('h5py', 'division_2_divisor')
    def gen_division_2_divisor(self, upstream_data, dividend, divisor1, divisor2):
        division_result = upstream_data['partial_division'].value / upstream_data['divisor2'].value
        return {'division_2_divisor': division_result}

    @require('division', dividend=A('dividend', lambda x: 'pd_' + x), divisor=A('divisor1'))
    @require(A('divisor2'))
    @will_generate('h5py', 'division_pd_2_divisor')
    def gen_division_pd_2_divisor(self, upstream_data, dividend, divisor1, divisor2):
        division_result = upstream_data['division'].value / upstream_data['divisor2'].value
        return {'division_pd_2_divisor': division_result}

    @require(A('dividend'))
    @require(A('divisor'))
    @will_generate('h5py', 'recursive_division')
    def gen_recursive_division(self, upstream_data, dividend, divisor):
        division_result = upstream_data['dividend'].value / upstream_data['divisor'].value
        return {'recursive_division': division_result}

    @require(A('sequence'))
    @will_generate('h5py', 'sequential_division')
    def gen_sequential_division(self, upstream_data, sequence):
        assert sequence
        division_result = upstream_data['sequence'][0].value
        for data in upstream_data['sequence'][1:]:
            division_result /= data.value
        return {'sequential_division': division_result}

    @require('data_df')
    @will_generate('pickle', 'train_test_split')
    def gen_train_test_split(self, upstream_data):
        data_df = upstream_data['data_df']
        train_id, test_id = train_test_split(
            data_df.index, test_size=0.5, random_state=0)
        return {'train_test_split': (train_id, test_id)}

    @require('data_df')
    @require('train_test_split')
    @will_generate('h5py', 'is_in_test_set')
    def gen_is_in_test_set(self, upstream_data):
        data_df = upstream_data['data_df']
        _, test_id = upstream_data['train_test_split']
        is_in_test_set = data_df.index.isin(test_id)
        sparse_is_in_test_set = csr_matrix(is_in_test_set[:, np.newaxis])
        return {'is_in_test_set': sparse_is_in_test_set}

    @require('data_df')
    @will_generate('h5py', 'nan', allow_nan=True)
    def gen_nan(self, upstream_data):
        nan = np.full(upstream_data['data_df'].shape[0], fill_value=np.nan, dtype=np.float32)
        return {'nan': nan}
