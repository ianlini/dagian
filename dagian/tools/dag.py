from __future__ import print_function, division, absolute_import, unicode_literals
import sys
import argparse

import yaml

from .config import (
    get_data_generator_class_from_config,
    get_data_generator_from_config,
)
from ..bundling import get_data_definitions_from_structure


def draw_dag(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description="Generate DAG.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-g', '--global-config',
                        default=".dagianrc/config.yml",
                        help="the path of the path configuration YAML file")
    parser.add_argument('-b', '--bundle-config',
                        default=".dagianrc/bundle_config.yml",
                        help="the path of the bundle configuration YAML file")
    parser.add_argument('-d', '--dag-output-path',
                        default="dag.svg",
                        help="output image path")
    parser.add_argument('-i', '--involved', action='store_true',
                        help="annotate the involved nodes and skipped nodes")
    args = parser.parse_args(argv)
    with open(args.global_config) as fp:
        global_config = yaml.safe_load(fp)
    with open(args.bundle_config) as fp:
        bundle_config = yaml.safe_load(fp)
    data_definitions = get_data_definitions_from_structure(bundle_config['structure'])
    if args.involved:
        data_generator = get_data_generator_from_config(global_config)
        data_generator.draw_involved_dag(args.dag_output_path, data_definitions)
    else:
        generator_class = get_data_generator_class_from_config(global_config)
        generator_class.draw_dag(args.dag_output_path, data_definitions)
