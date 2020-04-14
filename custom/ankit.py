import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR
import numpy as np
import pandas as pd

from iotfunctions.base import BaseSimpleAggregator
from iotfunctions.ui import (UIExpression,UIMultiItem)

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/jeffdare/test-jeff@starter_package'


class JeffSimpleAggregator(BaseSimpleAggregator):
    '''
    Create aggregation using expression. The calculation is evaluated for
    each data_item selected. The data item will be made available as a
    Pandas Series. Refer to the Pandas series using the local variable named
    "x". The expression must return a scalar value.

    Example:

    x.max() - x.min()

    '''

    def __init__(self, input_items, expression=None, output_items=None):
        super().__init__()

        self.input_items = input_items
        self.expression = expression
        self.output_items = output_items

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UIMultiItem(name='input_items', datatype=None, description=('Choose the data items'
                                                                                  ' that you would like to'
                                                                                  ' aggregate'),
                                  output_item='output_items', is_output_datatype_derived=True))

        inputs.append(UIExpression(name='expression', description='Paste in or type an AS expression'))

        return (inputs, [])

    def _calc(self, df):
        """
        If the function should be executed separately for each entity, describe the function logic in the _calc method
        """
        return df[self.input_items].apply(self.get_aggregation_method())

    def aggregate(self, x):
        return eval(self.expression)