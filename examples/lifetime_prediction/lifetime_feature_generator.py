import pandas as pd
from sklearn.model_selection import train_test_split
import dagian as fg
from dagian.decorators import (
    will_generate,
    require,
)


class LifetimeFeatureGenerator(fg.FeatureGenerator):
    def __init__(self, h5py_hdf_path, data_csv_path):
        super(LifetimeFeatureGenerator, self).__init__(
            h5py_hdf_path=h5py_hdf_path)
        self.data_csv_path = data_csv_path

    @will_generate('memory', 'data_df')
    def gen_data_df(self):
        return {'data_df': pd.read_csv(self.data_csv_path, index_col='id')}

    @require('data_df')
    @will_generate('h5py', 'label')
    def gen_label(self, upstream_data):
        data_df = upstream_data['data_df']
        return {'label': data_df['lifetime']}

    @require('data_df')
    @will_generate('h5py', ['weight', 'height'])
    def gen_raw_data_features(self, upstream_data):
        data_df = upstream_data['data_df']
        return data_df[['weight', 'height']]

    @require('data_df')
    @will_generate('h5py', 'BMI')
    def gen_bmi(self, upstream_data):
        data_df = upstream_data['data_df']
        bmi = data_df['weight'] / ((data_df['height'] / 100) ** 2)
        return {'BMI': bmi}

    # @require('{dividend}')
    # @require('{divisor}')
    # @will_generate('h5py', 'division')
    # @params('dividend', 'divisor')
    # def gen_division(self, upstream_data, args):
    #     division_result = upstream_data['{dividend}'].value / upstream_data['{divisor}'].value
    #     return {'division': division_result}

    @require('data_df')
    @will_generate('h5py', 'is_in_test_set')
    def gen_is_in_test_set(self, upstream_data):
        data_df = upstream_data['data_df']
        _, test_id = train_test_split(
            data_df.index, test_size=0.5, random_state=0)
        is_in_test_set = data_df.index.isin(test_id)
        return {'is_in_test_set': is_in_test_set}
