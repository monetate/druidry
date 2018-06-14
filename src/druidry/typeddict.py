from . import errors

import copy


class ExtendableDict(dict):

    @classmethod
    def wrap(cls, obj):
        return obj if isinstance(obj, cls) else cls(**obj)

    def copy(self):
        return copy.deepcopy(self)

    def extend(self, **kwargs):
        copy_ = copy.deepcopy(self)
        copy_.update(**kwargs)
        return copy_

    def extend_by(self, other):
        return self.extend(**other)


class TypedDict(ExtendableDict):

    def __init__(self, type_, **kwargs):
        self.type = type_
        lookup_type = self._get_lookup_type()

        if lookup_type not in self.required_fields:
            raise errors.DruidQueryError(self.type_error_message.format(type=lookup_type))

        self._set_required_fields(**kwargs)
        self._set_optional_fields(**kwargs)

    def _set_optional_fields(self, **kwargs):
        lookup_type = self._get_lookup_type()
        for field, field_type in self.optional_fields.get(lookup_type, {}).items():
            if field in kwargs:
                if field_type:
                    field_types = field_type if type(field_type) == list else [field_type]
                    if not any(isinstance(kwargs[field], field_type) for field_type in field_types):
                        raise errors.DruidQueryError(
                            ['Field {} has mismatched type (expecting {}, found {})'.format(
                                field, field_type, type(kwargs[field]))])

                self[field] = kwargs[field]

    def _set_required_fields(self, **kwargs):
        lookup_type = self._get_lookup_type()
        for field, field_type in self.required_fields[lookup_type].items():
            if field not in kwargs:
                raise errors.DruidQueryError(
                    ['Missing field: {} required for type: {}'.format(field, self.type)])

            if field_type:
                field_types = field_type if type(field_type) == list else [field_type]
                if not any(isinstance(kwargs[field], field_type) for field_type in field_types):
                    raise errors.DruidQueryError(
                        ['Field {} has mismatched type (expecting {}, found {})'.format(
                            field, field_type, type(kwargs[field]))])

            self[field] = kwargs[field]

    def _get_lookup_type(self):
        return self.type

    optional_fields = {}

    required_fields = {}

    type_error_message = ''
