from __future__ import print_function, division, absolute_import, unicode_literals
import unittest
from os.path import abspath, dirname, join
from shutil import rmtree
from tempfile import mkdtemp

from dagian.bundling import get_data_definitions_from_structure
from dagian.tools.dagian_runner import dagian_run_with_configs
import h5py
import yaml


class LifetimeFeaturesGeneratorTest(unittest.TestCase):
    def test_generate_lifetime_features(self):
        config_dir = join(dirname(abspath(__file__)), '.dagianrc')
        with open(join(config_dir, 'config.yml')) as fp:
            global_config = yaml.safe_load(fp)
        with open(join(config_dir, 'bundle_config.yml')) as fp:
            bundle_config = yaml.safe_load(fp)

        test_output_dir = mkdtemp(prefix="dagian_test_output_")
        h5py_hdf_path = join(test_output_dir, "h5py.h5")
        data_bundles_dir = join(test_output_dir, "data_bundles")

        global_config['data_bundles_dir'] = data_bundles_dir
        global_config['generator_class'] = ("examples.lifetime_prediction."
                                            + global_config['generator_class'])
        csv_path = global_config['generator_kwargs']['data_csv_path']
        global_config['generator_kwargs']['data_csv_path'] = join(
            "examples", "lifetime_prediction", csv_path)
        global_config['generator_kwargs']['h5py_hdf_path'] = \
            h5py_hdf_path

        dagian_run_with_configs(global_config, bundle_config)

        data_bundle_hdf_path = join(data_bundles_dir, 'default.h5')
        data_definitions = get_data_definitions_from_structure(bundle_config['structure'])
        with h5py.File(h5py_hdf_path, "r") as global_data_h5f, \
                h5py.File(data_bundle_hdf_path, "r") as data_bundle_h5f:
            assert (set(global_data_h5f)
                    > set(data_definition.json() for data_definition in data_definitions))
            assert set(data_bundle_h5f) == {'features', 'test_filters', 'label'}
            assert set(data_bundle_h5f['test_filters']) == {'is_in_test_set', 'test_set_masks'}
            self.assertTupleEqual(data_bundle_h5f['features'].shape, (6, 4))

        rmtree(test_output_dir)
