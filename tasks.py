#
# Tasks for the tutorial
#
import os

import luigi

from util import get_config

repo_path = get_config('paths', 'luigi_tutorial_path')
output_path = os.path.join(repo_path, 'output')

class SimplestTaskEver(luigi.Task):
    """
    An incredibly simple task that writes your name to a text file.
    """

    your_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(output_path, 'your_name.txt')
        )

    def run(self):
        with open(self.output().path, 'w') as out:
            out.write('Your name is %s' % self.your_name)