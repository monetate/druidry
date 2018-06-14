from .context import druidry
import unittest

class TestTypedDict(unittest.TestCase):

    def test_invalid_type(self):
        class OneTypeDict(druidry.typeddict.TypedDict):
            required_fields = {'valid': {}}

        with self.assertRaises(druidry.errors.DruidQueryError):
            OneTypeDict('invalid')

    def test_valid_type(self):
        class OneTypeDict(druidry.typeddict.TypedDict):
            required_fields = {'valid': {}}

        result = OneTypeDict('valid')
        self.assertEqual(result, {})

    def test_one_required_field(self):
        class RequiredFieldsDict(druidry.typeddict.TypedDict):
            required_fields = {
                'dict_type': {
                    'required_field': bool
                }
            }

        result = RequiredFieldsDict('dict_type', required_field=True)
        self.assertEqual(result, {'required_field': True})

    def test_multiple_required_fields(self):
        class RequiredFieldsDict(druidry.typeddict.TypedDict):
            required_fields = {
                'dict_type': {
                    'required_field_1': bool,
                    'required_field_2': int
                }
            }

        result = RequiredFieldsDict('dict_type', required_field_1=True, required_field_2=42)
        self.assertEqual(result, {'required_field_2': 42, 'required_field_1': True})

    def test_required_field_no_type(self):
        class RequiredFieldsDict(druidry.typeddict.TypedDict):
            required_fields = {
                'dict_type': {
                    'required_field': None
                }
            }

        result_1 = RequiredFieldsDict('dict_type', required_field=42)
        self.assertEqual(result_1, {'required_field': 42})

        result_2 = RequiredFieldsDict('dict_type', required_field=[1, 2, True])
        self.assertEqual(result_2, {'required_field': [1, 2, True]})

    def test_optional_field(self):
        class OptionalFieldsDict(druidry.typeddict.TypedDict):
            required_fields = {
                'dict_type': {
                    'required_field': list
                }
            }

            optional_field = {
                'dict_type': {
                    'optional_field': dict
                }
            }

        result_1 = OptionalFieldsDict('dict_type', required_field=[1, 2, 3], optional_field={'a': 'z'})
        self.assertEqual(result_1, {'required_field': [1, 2, 3]})

        result_2 = OptionalFieldsDict('dict_type', required_field=[1, 2, 3])
        self.assertEqual(result_2, {'required_field': [1, 2, 3]})

    def test_multile_types_field(self):
        class MultipleTypesFieldsDict(druidry.typeddict.TypedDict):
            required_fields = {
                'dict_type': {
                    'required_field': [list, dict]
                }
            }

        result_1 = MultipleTypesFieldsDict('dict_type', required_field=[1, 2, 3])
        self.assertEqual(result_1, {'required_field': [1, 2, 3]})

        result_2 = MultipleTypesFieldsDict('dict_type', required_field={'a': 'z'})
        self.assertEqual(result_2, {'required_field': {'a': 'z'}})

    def test_extend(self):
        d = druidry.typeddict.ExtendableDict({'a': 1, 'b': 2})
        self.assertEqual(d.extend(c=3), {'a': 1, 'b': 2, 'c': 3})

    def test_extend_overwrite(self):
        d = druidry.typeddict.ExtendableDict({'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(d.extend(c=4), {'a': 1, 'b': 2, 'c': 4})

    def test_extend_by(self):
        d = druidry.typeddict.ExtendableDict({'a': 1, 'b': 2})
        self.assertEqual(d.extend_by({'c': 3}), {'a': 1, 'b': 2, 'c': 3})

    def test_extend_by_overwrite(self):
        d = druidry.typeddict.ExtendableDict({'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(d.extend_by({'c': 4}), {'a': 1, 'b': 2, 'c': 4})

    def test_extend_subclass(self):
        class MyDict(druidry.typeddict.ExtendableDict):
            pass

        d = MyDict({'a': 1, 'b': 2})
        self.assertEqual(type(d.extend(c=3)), MyDict)

    def test_extend_subclass_overwrite(self):
        class MyDict(druidry.typeddict.ExtendableDict):
            pass

        d = MyDict({'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(type(d.extend(c=4)), MyDict)

    def test_wrap_instance(self):
        class MyDict(druidry.typeddict.ExtendableDict):
            pass

        d = MyDict({'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(type(MyDict.wrap(d)), MyDict)

    def test_wrap_noninstance(self):
        class MyDict(druidry.typeddict.ExtendableDict):
            pass

        d = {'a': 1, 'b': 2, 'c': 3}
        self.assertEqual(type(MyDict.wrap(d)), MyDict)
