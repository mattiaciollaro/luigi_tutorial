#
# Tasks for the tutorial
#
import os

import luigi

from util import get_config
from jupyter_notebook import JupyterNotebookTask

repo_path = get_config('paths', 'luigi_tutorial_path')
notebooks_path = os.path.join(repo_path, 'notebooks')
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

class BrokenTask(luigi.Task):
    """
    A broken version of `TellMeMyCity` (missing `requires()`).
    """
    my_name = luigi.Parameter()
    my_city = luigi.Parameter()


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

class BrokenTellMeMyName(luigi.Task):
    """
    A broken version of `TellMeMyName`.
    """
    my_name = luigi.Parameter()

    def run(self):
        with open(self.output().path, 'w') as out:
            out.write('Your name is %s' % self.my_name)

class TellMeMyCityAgain(luigi.Task):
    """
    Same as `TellMeMyCity`.
    """
    my_name = luigi.Parameter()
    my_city = luigi.Parameter()

    def requires(self):
        return BrokenTellMeMyName(my_name=self.my_name)

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

class PrepareData(JupyterNotebookTask):
    """
    A notebook that produces synthetic classification data.
    """
    notebook_path = os.path.join(notebooks_path, 'Prepare Data.ipynb')
    kernel_name = 'python3'
    timeout = 60

    def output(self):
        return [
            luigi.LocalTarget(
                os.path.join(output_path, 'model_ready_data.csv')
            )
        ]

class FitModel(JupyterNotebookTask):
    """
    A notebook that fits a Random Forest classifier.
    """
    notebook_path = os.path.join(notebooks_path, 'Fit Model.ipynb')
    kernel_name = 'python3'
    
    parameters = {
        'n_estimators': 50,
        'criterion': 'entropy',
        'max_features': 20
    }

    def requires(self):
        return {
            'data': PrepareData()
        }

    def output(self):
        return {
            'model_fit': luigi.LocalTarget(
                os.path.join(output_path, 'model_fit.pkl')
            )
        }

class ProduceDiagnostics(JupyterNotebookTask):
    """
    A notebook that produces some visualizations about the Random Forest
    classifier fit.
    """
    notebook_path = os.path.join(notebooks_path, 'Produce Diagnostics.ipynb')
    kernel_name = 'r_ker'
    
    def requires(self):
        return {
            'model': FitModel()
        }

    def output(self):
        return {
            'plot_one': luigi.LocalTarget(
                os.path.join(output_path, plot_one.pdf)
            ),
            'plot_two': luigi.LocalTarget(
                os.path.join(output_path, plot_two.pdf)
            )
        }