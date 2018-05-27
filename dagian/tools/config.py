from os.path import join, exists
import sys
from importlib import import_module

from mkdir_p import mkdir_p


def init_config():
    mkdir_p(".dagianrc")
    default_global_config = """\
generator_class: feature_generator.FeatureGenerator
data_bundles_dir: data_bundles

# The additional arguments that will be given when initiating the data generator
# object.
generator_kwargs:
  h5py_hdf_path:
    h5py.h5
  pandas_hdf_path:
    pandas.h5
"""
    default_bundle_config = """\
# The name of this bundle. This will be the file name of the data bundle.
# Another suggested usage is to comment out this line, so the name will be
# obtained from the file name of this config, that is, the name will be the same
# as the config file name without the extension.
name: default

# The structure of the data bundle. All the involved data will be generated and
# put into the global data file first (if data not exist), and then be bundled
# according to this structure, and then write to the data bundle file.
structure:
  id: id
  label: label
  features:
  - feature_1
  - feature_2

# Special configuration for the structure. Here we set concat=True for
# 'features'. It means that the data list in 'features' will be concatenated
# into a dataset.
structure_config:
  features:
    concat: True
"""
    default_global_config_path = join(".dagianrc", "config.yml")
    if exists(default_global_config_path):
        print("Warning: %s exists so it's not generated."
              % default_global_config_path)
    else:
        with open(default_global_config_path, "w") as fp:
            fp.write(default_global_config)

    default_bundle_config_path = join(".dagianrc", "bundle_config.yml")
    if exists(default_bundle_config_path):
        print("Warning: %s exists so it's not generated."
              % default_bundle_config_path)
    else:
        with open(default_bundle_config_path, "w") as fp:
            fp.write(default_bundle_config)


def get_class_from_str(class_str):
    module_name, class_name = class_str.rsplit(".", 1)
    sys.path.insert(0, '')
    module = import_module(module_name)
    sys.path.pop(0)
    generator_class = getattr(module, class_name)
    return generator_class


def get_data_generator_class_from_config(global_config):
    # TODO: check the config
    generator_class = get_class_from_str(global_config['generator_class'])
    return generator_class


def get_data_generator_from_config(global_config):
    generator_class = get_data_generator_class_from_config(global_config)
    data_generator = generator_class(**global_config['generator_kwargs'])
    return data_generator
