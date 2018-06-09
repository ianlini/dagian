from __future__ import print_function, division, absolute_import, unicode_literals
from os.path import basename, splitext, join
import sys
import argparse
import collections

import yaml
from mkdir_p import mkdir_p

from .config import get_data_generator_from_config
from ..bundling import get_data_definitions_from_structure


def dagian_run_with_configs(global_config, bundle_config, dag_output_path=None,
                            no_bundle=False):
    """Generate feature with configurations.

    global_config (collections.Mapping): global configuration
        generator_class: string
        data_bundles_dir: string
        generator_kwargs: collections.Mapping

    bundle_config (collections.Mapping): bundle configuration
        name: string
        structure: collections.Mapping
    """
    if not isinstance(global_config, collections.Mapping):
        raise ValueError("global_config should be a "
                         "collections.Mapping object.")
    if not isinstance(bundle_config, collections.Mapping):
        raise ValueError("bundle_config should be a "
                         "collections.Mapping object.")
    data_generator = get_data_generator_from_config(global_config)
    data_definitions = get_data_definitions_from_structure(bundle_config['structure'])
    data_generator.generate(data_definitions, dag_output_path)

    if not no_bundle:
        mkdir_p(global_config['data_bundles_dir'])
        bundle_path = join(global_config['data_bundles_dir'],
                           bundle_config['name'] + '.h5')
        data_generator.bundle(
            bundle_config['structure'], data_bundle_hdf_path=bundle_path,
            structure_config=bundle_config['structure_config'])


def dagian_run(argv=sys.argv[1:]):

    parser = argparse.ArgumentParser(
        description="Generate global data and data bundle.")
    parser.add_argument('-g', '--global-config',
                        default=".dagianrc/config.yml",
                        help="the path of the path configuration YAML file "
                             "(default: .dagianrc/config.yml)")
    parser.add_argument('-b', '--bundle-config',
                        default=".dagianrc/bundle_config.yml",
                        help="the path of the bundle configuration YAML file "
                             "(default: .dagianrc/bundle_config.yml)")
    parser.add_argument('-d', '--dag-output-path', default=None,
                        help="draw the involved subDAG to the provided path "
                             "(default: None)")
    parser.add_argument('--no-bundle', action='store_true',
                        help="not generate the data bundle")
    args = parser.parse_args(argv)
    with open(args.global_config) as fp:
        global_config = yaml.safe_load(fp)
    with open(args.bundle_config) as fp:
        bundle_config = yaml.safe_load(fp)
    filename_without_extension = splitext(basename(args.bundle_config))[0]
    bundle_config.setdefault('name', filename_without_extension)
    dagian_run_with_configs(global_config, bundle_config, args.dag_output_path,
                            args.no_bundle)
