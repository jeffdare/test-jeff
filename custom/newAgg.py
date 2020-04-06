import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR
import numpy as np
import pandas as pd

from iotfunctions.base import BaseTransformer,BaseSimpleAggregator
from iotfunctions import ui

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/jeffdare/test-jeff@starter_package'


class NewAgg(BaseSimpleAggregator):
    '''
    The docstring of the function will show as the function description in the UI.
    '''

    def __init__(self, source, name=None):
        # a function is expected to have at least one parameter that acts
        # as an input argument, e.g. "name" is an argument that represents the
        # name to be used in the greeting. It is an "input" as it is something
        # that the function needs to execute.

        # a function is expected to have at lease one parameter that describes
        # the output data items produced by the function, e.g. "greeting_col"
        # is the argument that asks what data item name should be used to
        # deliver the functions outputs

        # always create an instance variable with the same name as your arguments
        logging.debug("Entering init 1")
        logging.debug(source)
        self.source = source
        self.name = name
        super().__init__()

        # do not place any business logic in the __init__ method  # all business logic goes into the execute() method or methods called by the  # execute() method

    def get_aggregation_method(self):
        return self.aggregate

    def execute(self, df):
        logging.debug("Entering execute")
        df = df.copy()
        for i,input_item in enumerate(self.source):
            df[self.name[i]] = df[input_item].agg("mean")
        return df

    def aggregate(self, df):
        logging.debug("Entering aggregate")
        df = df.copy()
        for i,input_item in enumerate(self.source):
            df[self.name[i]] = df[input_item].agg("mean")
        return df

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        logging.debug("build_ui")
        logging.debug(cls)
        inputs = []
        inputs.append(ui.UISingleItem(
                name = 'source',
                datatype=float,
                description = "Mean")
                      )        
        outputs = []
        outputs.append(ui.UIFunctionOutSingle(name='name', datatype=float, description='The mean'))
        return (inputs,outputs)
