#
# Tasks for the tutorial
#
import os

import luigi

from util import get_config
from jupyter_notebook import JupyterNotebookTask

repo_path = get_config('paths', 'luigi_tutorial')
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

    kernel_name = 'luigi_tutorial_py3'

    def output(self):
        return luigi.LocalTarget(os.path.join(
            output_path, 'model_ready_data.csv')
        )

class FitModel(JupyterNotebookTask):
    """
    A notebook that fits a Random Forest classifier.
    """
    notebook_path = os.path.join(notebooks_path, 'Fit Model.ipynb')

    kernel_name = 'luigi_tutorial_py3'

    n_estimators = luigi.Parameter(
        default=200
    )

    criterion = luigi.Parameter(
        default='gini'
    )

    max_features = luigi.Parameter(
        default=50
    )
    
    def requires(self):
        return PrepareData()

    def output(self):
        return luigi.LocalTarget(os.path.join(
            output_path, 
            'model_fit.pkl'
        ))

class ProducePlot(JupyterNotebookTask):
    """
    A notebook that produces a visualization about the Random Forest
    classifier fit.
    """
    notebook_path = luigi.Parameter(
        default=os.path.join(notebooks_path, 'Produce Plot.ipynb')
    )
    
    kernel_name = luigi.Parameter(
        default='luigi_tutorial_py3'
    )

    n_estimators = luigi.Parameter(
        default=50
    )

    criterion = luigi.Parameter(
        default='entropy'
    )

    max_features = luigi.Parameter(
        default=3
    )

    def requires(self):
        return {
            'data': PrepareData(),
            'model': FitModel(
                n_estimators=self.n_estimators,
                criterion=self.criterion,
                max_features=self.max_features
            )
        }

    def output(self):
        return luigi.LocalTarget(os.path.join(
            output_path, 
            'importances_plot.png'
        ))

@inherits(FitModel)
class ProducePlot(JupyterNotebookTask)

    def requires(self):
        return {
            'data': PrepareData(),
            'model': FitModel(
                n_estimators=self.n_estimators,
                criterion=self.criterion,
                max_features=self.max_features
            )
        }

    def output(self):
        return luigi.LocalTarget(os.path.join(
            output_path, 
            'importances_plot.png'
        ))