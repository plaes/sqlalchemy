# orm/interfaces.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

Contains various base classes used throughout the ORM.

Defines the now deprecated ORM extension classes as well
as ORM internals.

Other than the deprecated extensions, this module and the
classes within should be considered mostly private.

"""

from __future__ import absolute_import

from .. import exc as sa_exc, util, inspect
from ..sql import operators
from collections import deque
from .base import _is_aliased_class, _class_to_mapper
from ..sql.base import _generative, Generative
from .base import ONETOMANY, MANYTOONE, MANYTOMANY, EXT_CONTINUE, EXT_STOP, NOT_EXTENSION
from .base import _InspectionAttr, _MappedAttribute
from .path_registry import PathRegistry, GenericRegistry
import collections


__all__ = (
    'AttributeExtension',
    'EXT_CONTINUE',
    'EXT_STOP',
    'ExtensionOption',
    'InstrumentationManager',
    'LoaderStrategy',
    'MapperExtension',
    'MapperOption',
    'MapperProperty',
    'PropComparator',
    'PropertyOption',
    'SessionExtension',
    'StrategizedOption',
    'StrategizedProperty',
    )



class MapperProperty(_MappedAttribute, _InspectionAttr):
    """Manage the relationship of a ``Mapper`` to a single class
    attribute, as well as that attribute as it appears on individual
    instances of the class, including attribute instrumentation,
    attribute access, loading behavior, and dependency calculations.

    The most common occurrences of :class:`.MapperProperty` are the
    mapped :class:`.Column`, which is represented in a mapping as
    an instance of :class:`.ColumnProperty`,
    and a reference to another class produced by :func:`.relationship`,
    represented in the mapping as an instance of
    :class:`.RelationshipProperty`.

    """

    cascade = frozenset()
    """The set of 'cascade' attribute names.

    This collection is checked before the 'cascade_iterator' method is called.

    """

    is_property = True

    def setup(self, context, entity, path, adapter, **kwargs):
        """Called by Query for the purposes of constructing a SQL statement.

        Each MapperProperty associated with the target mapper processes the
        statement referenced by the query context, adding columns and/or
        criterion as appropriate.
        """

        pass

    def create_row_processor(self, context, path,
                                            mapper, row, adapter):
        """Return a 3-tuple consisting of three row processing functions.

        """
        return None, None, None

    def cascade_iterator(self, type_, state, visited_instances=None,
                            halt_on=None):
        """Iterate through instances related to the given instance for
        a particular 'cascade', starting with this MapperProperty.

        Return an iterator3-tuples (instance, mapper, state).

        Note that the 'cascade' collection on this MapperProperty is
        checked first for the given type before cascade_iterator is called.

        See PropertyLoader for the related instance implementation.
        """

        return iter(())

    def set_parent(self, parent, init):
        self.parent = parent

    def instrument_class(self, mapper):  # pragma: no-coverage
        raise NotImplementedError()

    @util.memoized_property
    def info(self):
        """Info dictionary associated with the object, allowing user-defined
        data to be associated with this :class:`.MapperProperty`.

        The dictionary is generated when first accessed.  Alternatively,
        it can be specified as a constructor argument to the
        :func:`.column_property`, :func:`.relationship`, or :func:`.composite`
        functions.

        .. versionadded:: 0.8  Added support for .info to all
           :class:`.MapperProperty` subclasses.

        .. seealso::

            :attr:`.QueryableAttribute.info`

            :attr:`.SchemaItem.info`

        """
        return {}

    _configure_started = False
    _configure_finished = False

    def init(self):
        """Called after all mappers are created to assemble
        relationships between mappers and perform other post-mapper-creation
        initialization steps.

        """
        self._configure_started = True
        self.do_init()
        self._configure_finished = True

    @property
    def class_attribute(self):
        """Return the class-bound descriptor corresponding to this
        :class:`.MapperProperty`.

        This is basically a ``getattr()`` call::

            return getattr(self.parent.class_, self.key)

        I.e. if this :class:`.MapperProperty` were named ``addresses``,
        and the class to which it is mapped is ``User``, this sequence
        is possible::

            >>> from sqlalchemy import inspect
            >>> mapper = inspect(User)
            >>> addresses_property = mapper.attrs.addresses
            >>> addresses_property.class_attribute is User.addresses
            True
            >>> User.addresses.property is addresses_property
            True


        """

        return getattr(self.parent.class_, self.key)

    def do_init(self):
        """Perform subclass-specific initialization post-mapper-creation
        steps.

        This is a template method called by the ``MapperProperty``
        object's init() method.

        """

        pass

    def post_instrument_class(self, mapper):
        """Perform instrumentation adjustments that need to occur
        after init() has completed.

        """
        pass

    def is_primary(self):
        """Return True if this ``MapperProperty``'s mapper is the
        primary mapper for its class.

        This flag is used to indicate that the ``MapperProperty`` can
        define attribute instrumentation for the class at the class
        level (as opposed to the individual instance level).
        """

        return not self.parent.non_primary

    def merge(self, session, source_state, source_dict, dest_state,
                dest_dict, load, _recursive):
        """Merge the attribute represented by this ``MapperProperty``
        from source to destination object"""

        pass

    def compare(self, operator, value, **kw):
        """Return a compare operation for the columns represented by
        this ``MapperProperty`` to the given value, which may be a
        column value or an instance.  'operator' is an operator from
        the operators module, or from sql.Comparator.

        By default uses the PropComparator attached to this MapperProperty
        under the attribute name "comparator".
        """

        return operator(self.comparator, value)

    def __repr__(self):
        return '<%s at 0x%x; %s>' % (
            self.__class__.__name__,
            id(self), getattr(self, 'key', 'no key'))

class PropComparator(operators.ColumnOperators):
    """Defines boolean, comparison, and other operators for
    :class:`.MapperProperty` objects.

    SQLAlchemy allows for operators to
    be redefined at both the Core and ORM level.  :class:`.PropComparator`
    is the base class of operator redefinition for ORM-level operations,
    including those of :class:`.ColumnProperty`,
    :class:`.RelationshipProperty`, and :class:`.CompositeProperty`.

    .. note:: With the advent of Hybrid properties introduced in SQLAlchemy
       0.7, as well as Core-level operator redefinition in
       SQLAlchemy 0.8, the use case for user-defined :class:`.PropComparator`
       instances is extremely rare.  See :ref:`hybrids_toplevel` as well
       as :ref:`types_operators`.

    User-defined subclasses of :class:`.PropComparator` may be created. The
    built-in Python comparison and math operator methods, such as
    :meth:`.operators.ColumnOperators.__eq__`,
    :meth:`.operators.ColumnOperators.__lt__`, and
    :meth:`.operators.ColumnOperators.__add__`, can be overridden to provide
    new operator behavior. The custom :class:`.PropComparator` is passed to
    the :class:`.MapperProperty` instance via the ``comparator_factory``
    argument. In each case,
    the appropriate subclass of :class:`.PropComparator` should be used::

        # definition of custom PropComparator subclasses

        from sqlalchemy.orm.properties import \\
                                ColumnProperty,\\
                                CompositeProperty,\\
                                RelationshipProperty

        class MyColumnComparator(ColumnProperty.Comparator):
            def __eq__(self, other):
                return self.__clause_element__() == other

        class MyRelationshipComparator(RelationshipProperty.Comparator):
            def any(self, expression):
                "define the 'any' operation"
                # ...

        class MyCompositeComparator(CompositeProperty.Comparator):
            def __gt__(self, other):
                "redefine the 'greater than' operation"

                return sql.and_(*[a>b for a, b in
                                  zip(self.__clause_element__().clauses,
                                      other.__composite_values__())])


        # application of custom PropComparator subclasses

        from sqlalchemy.orm import column_property, relationship, composite
        from sqlalchemy import Column, String

        class SomeMappedClass(Base):
            some_column = column_property(Column("some_column", String),
                                comparator_factory=MyColumnComparator)

            some_relationship = relationship(SomeOtherClass,
                                comparator_factory=MyRelationshipComparator)

            some_composite = composite(
                    Column("a", String), Column("b", String),
                    comparator_factory=MyCompositeComparator
                )

    Note that for column-level operator redefinition, it's usually
    simpler to define the operators at the Core level, using the
    :attr:`.TypeEngine.comparator_factory` attribute.  See
    :ref:`types_operators` for more detail.

    See also:

    :class:`.ColumnProperty.Comparator`

    :class:`.RelationshipProperty.Comparator`

    :class:`.CompositeProperty.Comparator`

    :class:`.ColumnOperators`

    :ref:`types_operators`

    :attr:`.TypeEngine.comparator_factory`

    """

    def __init__(self, prop, parentmapper, adapt_to_entity=None):
        self.prop = self.property = prop
        self._parentmapper = parentmapper
        self._adapt_to_entity = adapt_to_entity

    def __clause_element__(self):
        raise NotImplementedError("%r" % self)

    def adapt_to_entity(self, adapt_to_entity):
        """Return a copy of this PropComparator which will use the given
        :class:`.AliasedInsp` to produce corresponding expressions.
        """
        return self.__class__(self.prop, self._parentmapper, adapt_to_entity)

    @property
    def adapter(self):
        """Produce a callable that adapts column expressions
        to suit an aliased version of this comparator.

        """
        if self._adapt_to_entity is None:
            return None
        else:
            return self._adapt_to_entity._adapt_element

    @util.memoized_property
    def info(self):
        return self.property.info

    @staticmethod
    def any_op(a, b, **kwargs):
        return a.any(b, **kwargs)

    @staticmethod
    def has_op(a, b, **kwargs):
        return a.has(b, **kwargs)

    @staticmethod
    def of_type_op(a, class_):
        return a.of_type(class_)

    def of_type(self, class_):
        """Redefine this object in terms of a polymorphic subclass.

        Returns a new PropComparator from which further criterion can be
        evaluated.

        e.g.::

            query.join(Company.employees.of_type(Engineer)).\\
               filter(Engineer.name=='foo')

        :param \class_: a class or mapper indicating that criterion will be
            against this specific subclass.


        """

        return self.operate(PropComparator.of_type_op, class_)

    def any(self, criterion=None, **kwargs):
        """Return true if this collection contains any member that meets the
        given criterion.

        The usual implementation of ``any()`` is
        :meth:`.RelationshipProperty.Comparator.any`.

        :param criterion: an optional ClauseElement formulated against the
          member class' table or attributes.

        :param \**kwargs: key/value pairs corresponding to member class
          attribute names which will be compared via equality to the
          corresponding values.

        """

        return self.operate(PropComparator.any_op, criterion, **kwargs)

    def has(self, criterion=None, **kwargs):
        """Return true if this element references a member which meets the
        given criterion.

        The usual implementation of ``has()`` is
        :meth:`.RelationshipProperty.Comparator.has`.

        :param criterion: an optional ClauseElement formulated against the
          member class' table or attributes.

        :param \**kwargs: key/value pairs corresponding to member class
          attribute names which will be compared via equality to the
          corresponding values.

        """

        return self.operate(PropComparator.has_op, criterion, **kwargs)


class StrategizedProperty(MapperProperty):
    """A MapperProperty which uses selectable strategies to affect
    loading behavior.

    There is a single strategy selected by default.  Alternate
    strategies can be selected at Query time through the usage of
    ``StrategizedOption`` objects via the Query.options() method.

    """

    strategy_wildcard_key = None

    @util.memoized_property
    def _wildcard_path(self):
        if self.strategy_wildcard_key:
            return ('loaderstrategy', (self.strategy_wildcard_key,))
        else:
            return None

    def _get_context_strategy(self, context, path):
        load = path._inlined_get_for(self, context, 'loader')

        #if not strategy_cls:
        #    wc_key = self._wildcard_path
        #   if wc_key and wc_key in context.attributes:
        #        strategy_cls = context.attributes[wc_key]

        if load:
            return load.strategy_impl
        else:
            return self.strategy

    def _get_strategy(self, key):
        try:
            return self._strategies[key]
        except KeyError:
            cls = self._strategy_lookup(*key)
            self._strategies[key] = strategy = cls(self)
            return strategy

    def setup(self, context, entity, path, adapter, **kwargs):
        self._get_context_strategy(context, path).\
                    setup_query(context, entity, path,
                                    adapter, **kwargs)

    def create_row_processor(self, context, path, mapper, row, adapter):
        return self._get_context_strategy(context, path).\
                    create_row_processor(context, path,
                                    mapper, row, adapter)

    def do_init(self):
        self._strategies = {}
        self.strategy = self._get_strategy(
                            self.strategy_class._strategy_keys[0])

    def post_instrument_class(self, mapper):
        if self.is_primary() and \
            not mapper.class_manager._attr_has_impl(self.key):
            self.strategy.init_class_attribute(mapper)


    _strategies = collections.defaultdict(dict)

    @classmethod
    def _strategy_for(cls, *keys):
        def decorate(dec_cls):
            dec_cls._strategy_keys = []
            for key in keys:
                key = tuple(sorted(key.items()))
                cls._strategies[cls][key] = dec_cls
                dec_cls._strategy_keys.append(key)
            return dec_cls
        return decorate

    @classmethod
    def _strategy_lookup(cls, *key):
        for prop_cls in cls.__mro__:
            if prop_cls in cls._strategies:
                strategies = cls._strategies[prop_cls]
                try:
                    return strategies[key]
                except KeyError:
                    pass
        raise Exception("can't locate strategy for %s %s" % (cls, key))


class MapperOption(object):
    """Describe a modification to a Query."""

    propagate_to_loaders = False
    """if True, indicate this option should be carried along
    Query object generated by scalar or object lazy loaders.
    """

    def process_query(self, query):
        pass

    def process_query_conditionally(self, query):
        """same as process_query(), except that this option may not
        apply to the given query.

        Used when secondary loaders resend existing options to a new
        Query."""

        self.process_query(query)

class Load(Generative, MapperOption):
    def __init__(self, entity):
        if entity is not None:
            insp = inspect(entity)
            self.path = insp._path_registry
            self.materialized = True
        else:
            self.path = GenericRegistry(None, None, None)
            self.materialized = False
            self._unmaterialized = set([self])
        self.context = {}

    strategy = None
    propagate_to_loaders = False

    def _materialize(self, query, raiseerr):
        if self.materialized:
            return

        start_path = self.path.path[1:]
        # _current_path implies we're in a
        # secondary load with an existing path
        current_path = list(query._current_path.path)
        if current_path:
            start_path = self._chop_path(start_path, current_path)

        self.materialized = True

        if not start_path:
            path = PathRegistry.root
        else:
            token = start_path[0]
            if isinstance(token, str):
                entity = self._find_entity_basestring(query, token, raiseerr)
            elif isinstance(token, PropComparator):
                prop = token.property
                entity = self._find_entity_prop_comparator(
                                        query,
                                        prop.key,
                                        token._parententity,
                                        raiseerr)

            else:
                raise sa_exc.ArgumentError(
                        "mapper option expects "
                        "string key or list of attributes")

            path_element = entity.entity_zero
            path = PathRegistry.root[path_element]
            for token in start_path:
                path = self._generate_real_path(path, token)

        self.path = path

        if self.strategy:
            if path.has_entity:
                self.path.parent.set(self.context, "loader", self)
            else:
                self.path.set(self.context, "loader", self)

    def _chop_path(to_chop, path):
        i = -1
        for i, (c_token, (p_mapper, p_prop)) in enumerate(zip(to_chop, path.pairs())):
            if c_token.property is not p_prop.property:
                break
        else:
            i += 1
        return to_chop[i:]

    def _find_entity_prop_comparator(self, query, token, mapper, conditional):
        if _is_aliased_class(mapper):
            searchfor = mapper
        else:
            searchfor = _class_to_mapper(mapper)
        for ent in query._mapper_entities:
            if ent.corresponds_to(searchfor):
                return ent
        else:
            if not conditional:
                if not list(query._mapper_entities):
                    raise sa_exc.ArgumentError(
                        "Query has only expression-based entities - "
                        "can't find property named '%s'."
                         % (token, )
                    )
                else:
                    raise sa_exc.ArgumentError(
                        "Can't find property '%s' on any entity "
                        "specified in this Query.  Note the full path "
                        "from root (%s) to target entity must be specified."
                        % (token, ",".join(str(x) for
                            x in query._mapper_entities))
                    )
            else:
                return None

    def _find_entity_basestring(self, query, token, raiseerr):
        for ent in query._mapper_entities:
            # return only the first _MapperEntity when searching
            # based on string prop name.   Ideally object
            # attributes are used to specify more exactly.
            return ent
        else:
            if raiseerr:
                raise sa_exc.ArgumentError(
                    "Query has only expression-based entities - "
                    "can't find property named '%s'."
                     % (token, )
                )
            else:
                return None

    def process_query(self, query):
        self._process(query, True)

    def process_query_conditionally(self, query):
        self._process(query, False)

    def _process(self, query, raiseerr):
        if not self.materialized:
            for val in self._unmaterialized:
                val._materialize(query, raiseerr)
            self._unmaterialized.clear()

        query._attributes.update(self.context)

    @util.dependencies("sqlalchemy.orm.util")
    def _generate_real_path(self, orm_util, path, attr):
        if isinstance(attr, util.string_types):
            attr = path.entity.attrs[attr]
            path = path[attr]
        else:
            prop = attr.property
            if getattr(attr, '_of_type', None):
                ac = attr._of_type
                ext_info = inspect(ac)

                path_element = ext_info.mapper
                if not ext_info.is_aliased_class:
                    if self.materialized:
                        ac = orm_util.with_polymorphic(
                                    ext_info.mapper.base_mapper,
                                    ext_info.mapper, aliased=True,
                                    _use_mapper_path=True)
                        path.entity_path[prop].set(self.context,
                                            "path_with_polymorphic", inspect(ac))
                path = path[prop][path_element]
            else:
                path = path[prop]

        if path.has_entity:
            path = path.entity_path
        return path

    def _generate_path(self, path, attr):
        if self.materialized:
            return self._generate_real_path(path, attr)
        else:
            path = path[attr]
        return path

    @_generative
    def _set_strategy(self, attr, strategy):
        self.path = self._generate_path(self.path, attr)
        self.strategy = strategy
        if self.materialized:
            self.path.parent.set(self.context, "loader", self)
        else:
            self._unmaterialized.add(self)

    @_generative
    def _set_column_strategy(self, attrs, strategy):
        for attr in attrs:
            path = self._generate_path(self.path, attr)
            cloned = self._generate()
            cloned.strategy = strategy
            cloned.path = path

            if self.materialized:
                path.set(self.context, "loader", cloned)
            else:
                self._unmaterialized.add(cloned)

    def defer(self, *attrs):
        return self._set_column_strategy(
                    attrs,
                    (("deferred", True), ("instrument", True))
                )

    def joined(self, attr):
        return self._set_strategy(
                    attr,
                    (("lazy", "joined"),)
                )

    @util.memoized_property
    def strategy_impl(self):
        assert self.materialized
        if self.path.has_entity:
            return self.path.parent.prop._get_strategy(self.strategy)
        else:
            return self.path.prop._get_strategy(self.strategy)

class _UnmaterializedLoad(Load):
    def __init__(self):
        self.path = GenericRegistry(None, None, None)
        self._unmaterialized = set([self])

    strategy = None
    propagate_to_loaders = False

    @_generative
    def _set_strategy(self, attr, strategy):
        self.path = self._generate_path(self.path, attr)
        self.strategy = strategy
        self._unmaterialized.add(self)

    @_generative
    def _set_column_strategy(self, attrs, strategy):
        for attr in attrs:
            path = self._generate_path(self.path, attr)
            cloned = self._generate()
            cloned.strategy = strategy
            cloned.path = path

            self._unmaterialized.add(cloned)

    def _materialize(self, query, context, raiseerr):
        start_path = self.path.path[1:]
        # _current_path implies we're in a
        # secondary load with an existing path
        current_path = list(query._current_path.path)
        if current_path:
            start_path = self._chop_path(start_path, current_path)


        loader = Load(None)
        loader.materialized = True
        loader.context = context

        if not start_path:
            path = PathRegistry.root
        else:
            token = start_path[0]
            if isinstance(token, str):
                entity = self._find_entity_basestring(query, token, raiseerr)
            elif isinstance(token, PropComparator):
                prop = token.property
                entity = self._find_entity_prop_comparator(
                                        query,
                                        prop.key,
                                        token._parententity,
                                        raiseerr)

            else:
                raise sa_exc.ArgumentError(
                        "mapper option expects "
                        "string key or list of attributes")

            path_element = entity.entity_zero
            path = PathRegistry.root[path_element]
            for token in start_path:
                path = loader._generate_real_path(path, token)

        #$import pdb
        #pdb.set_trace()
        loader.path = path
        loader.strategy = self.strategy
        if loader.strategy:
            if path.has_entity:
                path.parent.set(context, "loader", loader)
            else:
                path.set(context, "loader", loader)

    def _generate_path(self, path, attr):
        path = path[attr]
        return path

    def _chop_path(to_chop, path):
        i = -1
        for i, (c_token, (p_mapper, p_prop)) in enumerate(zip(to_chop, path.pairs())):
            if c_token.property is not p_prop.property:
                break
        else:
            i += 1
        return to_chop[i:]

    def _find_entity_prop_comparator(self, query, token, mapper, conditional):
        if _is_aliased_class(mapper):
            searchfor = mapper
        else:
            searchfor = _class_to_mapper(mapper)
        for ent in query._mapper_entities:
            if ent.corresponds_to(searchfor):
                return ent
        else:
            if not conditional:
                if not list(query._mapper_entities):
                    raise sa_exc.ArgumentError(
                        "Query has only expression-based entities - "
                        "can't find property named '%s'."
                         % (token, )
                    )
                else:
                    raise sa_exc.ArgumentError(
                        "Can't find property '%s' on any entity "
                        "specified in this Query.  Note the full path "
                        "from root (%s) to target entity must be specified."
                        % (token, ",".join(str(x) for
                            x in query._mapper_entities))
                    )
            else:
                return None

    def _find_entity_basestring(self, query, token, raiseerr):
        for ent in query._mapper_entities:
            # return only the first _MapperEntity when searching
            # based on string prop name.   Ideally object
            # attributes are used to specify more exactly.
            return ent
        else:
            if raiseerr:
                raise sa_exc.ArgumentError(
                    "Query has only expression-based entities - "
                    "can't find property named '%s'."
                     % (token, )
                )
            else:
                return None

    def _process(self, query, raiseerr):
        context = {}
        for val in self._unmaterialized:
            val._materialize(query, context, raiseerr)
        query._attributes.update(context)


class LoaderStrategy(object):
    """Describe the loading behavior of a StrategizedProperty object.

    The ``LoaderStrategy`` interacts with the querying process in three
    ways:

    * it controls the configuration of the ``InstrumentedAttribute``
      placed on a class to handle the behavior of the attribute.  this
      may involve setting up class-level callable functions to fire
      off a select operation when the attribute is first accessed
      (i.e. a lazy load)

    * it processes the ``QueryContext`` at statement construction time,
      where it can modify the SQL statement that is being produced.
      Simple column attributes may add their represented column to the
      list of selected columns, *eager loading* properties may add
      ``LEFT OUTER JOIN`` clauses to the statement.

    * It produces "row processor" functions at result fetching time.
      These "row processor" functions populate a particular attribute
      on a particular mapped instance.

    """
    def __init__(self, parent):
        self.parent_property = parent
        self.is_class_level = False
        self.parent = self.parent_property.parent
        self.key = self.parent_property.key

    def init_class_attribute(self, mapper):
        pass

    def setup_query(self, context, entity, path, adapter, **kwargs):
        pass

    def create_row_processor(self, context, path, mapper,
                                row, adapter):
        """Return row processing functions which fulfill the contract
        specified by MapperProperty.create_row_processor.

        StrategizedProperty delegates its create_row_processor method
        directly to this method. """

        return None, None, None

    def __str__(self):
        return str(self.parent_property)
