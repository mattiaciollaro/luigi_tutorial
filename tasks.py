#
# Tasks for the tutorial
#
import os

import luigi

from util import get_config

repo_path = get_config('paths', 'luigi_tutorial_path')
output_path = os.path.join(repo_path, 'output')

class TellMeMyName(luigi.Task):
    """
    An incredibly simple task that writes your name to a text file.
    """
    my_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(output_path, 'my_name.txt')
        )

    def run(self):
        with open(self.output().path, 'w') as out:
            out.write('Your name is %s' % self.my_name)

class TellMeMyCity(luigi.Task):
    """
    A second very simple task that writes your name and the city where you live.
    """
    my_name = luigi.Parameter()
    my_city = luigi.Parameter()

    def requires(self):
        return TellMeMyName(my_name=self.my_name)

    def output(self):
        return luigi.LocalTarget(
            os.path.join(output_path, 'my_name_and_city.txt')
        )

    def run(self):
        with open(self.input().path) as name:
            text = name.read().strip()

        text += '\nYour city is %s' % self.my_city

        with open(self.output().path, 'w') as out:
            out.write(text)