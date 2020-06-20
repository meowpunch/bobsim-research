import argparse
from functools import partial

#
# class PipelineConfig(argparse.Namespace):
#     # TODO: apply configuration globally
#     def __init__(self, name="Pipeline config"):
#         self.name = name
#         self.parser = self.build_parser()
#         self.args = self.parser.parse_args()
#         super().__init__(**vars(self.args))
#
#     def init_parser(self):
#         parser = argparse.ArgumentParser(self.name)
#         parser.add_argument = partial(parser.add_argument, help=' ')
#         return parser
#
#     def build_parser(self):
#         parser = self.init_parser()
#         parser.add_argument('--save_rds', default=False, type=bool)
#         return parser
#
#     def get_parser(self):
#         return self.parser
#
#     def get_args(self):
#         return self.args

