import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR
import numpy as np
import pandas as pd

from iotfunctions.base import BaseTransformer,BaseComplexAggregator
from iotfunctions import ui

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/jeffdare/test-jeff@starter_package'


class JeffCompAgg(BaseComplexAggregator):
    '''
    The docstring of the function will show as the function description in the UI.
    '''
    _abort_on_fail = True
    _allow_empty_df = False
    # _discard_prior_on_merge = True
    requires_input_items = True
    produces_output_items = True

    def __init__(self, baselineColumn, sum=None):
        # a function is expected to have at least one parameter that acts
        # as an input argument, e.g. "name" is an argument that represents the
        # name to be used in the greeting. It is an "input" as it is something
        # that the function needs to execute.

        # a function is expected to have at lease one parameter that describes
        # the output data items produced by the function, e.g. "greeting_col"
        # is the argument that asks what data item name should be used to
        # deliver the functions outputs

        # always create an instance variable with the same name as your arguments
        logging.debug("Entering init")
        logging.debug(baselineColumn)
        self.baselineColumn = baselineColumn
        self.sum = sum
        super().__init__()

    def execute(self, df=None, start_ts=None, end_ts=None, entities=None):
        logging.debug("Entering execute")
        # grouper = self.granularity.grouper
        # logger.debug('grouper=%s' % str(grouper))
        df[self.sum] = df[self.baselineColumn].sum()
        return df

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        logging.debug("build_ui")
        logging.debug(cls)
        inputs = []
        inputs.append(ui.UISingleItem(name='baselineColumn', datatype=float, required=True, description='The baseline column.'))
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(name='sum', datatype=float, description='The sum.'))
        return (inputs, outputs)
