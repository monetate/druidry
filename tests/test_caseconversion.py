import unittest
from .context import druidry


class TestCaseConversion(unittest.TestCase):

    def test_camel_case_kwargs(self):

        @druidry.caseconversion.camel_case_kwargs
        def fn(**kwargs):
            return kwargs.get('camelCase')

        result_true = fn(camel_case=True)
        self.assertTrue(result_true)

        result_false = fn(camel_case=False)
        self.assertFalse(result_false)
