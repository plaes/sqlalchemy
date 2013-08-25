# orm/strategy_options.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Superseded loader strategy options.

The query options here were for many years the standard construct
passed to Query.options() to control loading.   The system as of 0.9
has been replaced by the much simpler ``load()`` construct; the
options here now reinterpret themselves in terms of the new system.

"""

from .interfaces import MapperOption
from .. import util

def joinedload(*keys, **kw):
    """Return a ``MapperOption`` that will convert the property of the given
    name or series of mapped attributes into an joined eager load.

    .. versionchanged:: 0.6beta3
        This function is known as :func:`eagerload` in all versions
        of SQLAlchemy prior to version 0.6beta3, including the 0.5 and 0.4
        series. :func:`eagerload` will remain available for the foreseeable
        future in order to enable cross-compatibility.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    examples::

        # joined-load the "orders" collection on "User"
        query(User).options(joinedload(User.orders))

        # joined-load the "keywords" collection on each "Item",
        # but not the "items" collection on "Order" - those
        # remain lazily loaded.
        query(Order).options(joinedload(Order.items, Item.keywords))

        # to joined-load across both, use joinedload_all()
        query(Order).options(joinedload_all(Order.items, Item.keywords))

        # set the default strategy to be 'joined'
        query(Order).options(joinedload('*'))

    :func:`joinedload` also accepts a keyword argument `innerjoin=True` which
    indicates using an inner join instead of an outer::

        query(Order).options(joinedload(Order.user, innerjoin=True))

    .. note::

       The join created by :func:`joinedload` is anonymously aliased such that
       it **does not affect the query results**.   An :meth:`.Query.order_by`
       or :meth:`.Query.filter` call **cannot** reference these aliased
       tables - so-called "user space" joins are constructed using
       :meth:`.Query.join`.   The rationale for this is that
       :func:`joinedload` is only applied in order to affect how related
       objects or collections are loaded as an optimizing detail - it can be
       added or removed with no impact on actual results.   See the section
       :ref:`zen_of_eager_loading` for a detailed description of how this is
       used, including how to use a single explicit JOIN for
       filtering/ordering and eager loading simultaneously.

    See also:  :func:`subqueryload`, :func:`lazyload`

    """
    innerjoin = kw.pop('innerjoin', None)
    if innerjoin is not None:
        return (
             _strategies.EagerLazyOption(keys, lazy='joined'),
             _strategies.EagerJoinOption(keys, innerjoin)
         )
    else:
        return _strategies.EagerLazyOption(keys, lazy='joined')


def joinedload_all(*keys, **kw):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path or series of mapped attributes
    into an joined eager load.

    .. versionchanged:: 0.6beta3
        This function is known as :func:`eagerload_all` in all versions
        of SQLAlchemy prior to version 0.6beta3, including the 0.5 and 0.4
        series. :func:`eagerload_all` will remain available for the
        foreseeable future in order to enable cross-compatibility.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    For example::

        query.options(joinedload_all('orders.items.keywords'))...

    will set all of ``orders``, ``orders.items``, and
    ``orders.items.keywords`` to load in one joined eager load.

    Individual descriptors are accepted as arguments as well::

        query.options(joinedload_all(User.orders, Order.items, Item.keywords))

    The keyword arguments accept a flag `innerjoin=True|False` which will
    override the value of the `innerjoin` flag specified on the
    relationship().

    See also:  :func:`subqueryload_all`, :func:`lazyload`

    """
    innerjoin = kw.pop('innerjoin', None)
    if innerjoin is not None:
        return (
            _strategies.EagerLazyOption(keys, lazy='joined', chained=True),
            _strategies.EagerJoinOption(keys, innerjoin, chained=True)
        )
    else:
        return _strategies.EagerLazyOption(keys, lazy='joined', chained=True)


def eagerload(*args, **kwargs):
    """A synonym for :func:`joinedload()`."""
    return joinedload(*args, **kwargs)


def eagerload_all(*args, **kwargs):
    """A synonym for :func:`joinedload_all()`"""
    return joinedload_all(*args, **kwargs)


def subqueryload(*keys):
    """Return a ``MapperOption`` that will convert the property
    of the given name or series of mapped attributes
    into an subquery eager load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    examples::

        # subquery-load the "orders" collection on "User"
        query(User).options(subqueryload(User.orders))

        # subquery-load the "keywords" collection on each "Item",
        # but not the "items" collection on "Order" - those
        # remain lazily loaded.
        query(Order).options(subqueryload(Order.items, Item.keywords))

        # to subquery-load across both, use subqueryload_all()
        query(Order).options(subqueryload_all(Order.items, Item.keywords))

        # set the default strategy to be 'subquery'
        query(Order).options(subqueryload('*'))

    See also:  :func:`joinedload`, :func:`lazyload`

    """
    return _strategies.EagerLazyOption(keys, lazy="subquery")


def subqueryload_all(*keys):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path or series of mapped attributes
    into a subquery eager load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    For example::

        query.options(subqueryload_all('orders.items.keywords'))...

    will set all of ``orders``, ``orders.items``, and
    ``orders.items.keywords`` to load in one subquery eager load.

    Individual descriptors are accepted as arguments as well::

        query.options(subqueryload_all(User.orders, Order.items,
        Item.keywords))

    See also:  :func:`joinedload_all`, :func:`lazyload`, :func:`immediateload`

    """
    return _strategies.EagerLazyOption(keys, lazy="subquery", chained=True)


def lazyload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given
    name or series of mapped attributes into a lazy load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`eagerload`, :func:`subqueryload`, :func:`immediateload`

    """
    return _strategies.EagerLazyOption(keys, lazy=True)


def lazyload_all(*keys):
    """Return a ``MapperOption`` that will convert all the properties
    along the given dot-separated path or series of mapped attributes
    into a lazy load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`eagerload`, :func:`subqueryload`, :func:`immediateload`

    """
    return _strategies.EagerLazyOption(keys, lazy=True, chained=True)


def noload(*keys):
    """Return a ``MapperOption`` that will convert the property of the
    given name or series of mapped attributes into a non-load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`lazyload`, :func:`eagerload`,
    :func:`subqueryload`, :func:`immediateload`

    """
    return _strategies.EagerLazyOption(keys, lazy=None)


def immediateload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given
    name or series of mapped attributes into an immediate load.

    The "immediate" load means the attribute will be fetched
    with a separate SELECT statement per parent in the
    same way as lazy loading - except the loader is guaranteed
    to be called at load time before the parent object
    is returned in the result.

    The normal behavior of lazy loading applies - if
    the relationship is a simple many-to-one, and the child
    object is already present in the :class:`.Session`,
    no SELECT statement will be emitted.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`lazyload`, :func:`eagerload`, :func:`subqueryload`

    .. versionadded:: 0.6.5

    """
    return _strategies.EagerLazyOption(keys, lazy='immediate')


def contains_eager(*keys, **kwargs):
    """Return a ``MapperOption`` that will indicate to the query that
    the given attribute should be eagerly loaded from columns currently
    in the query.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    The option is used in conjunction with an explicit join that loads
    the desired rows, i.e.::

        sess.query(Order).\\
                join(Order.user).\\
                options(contains_eager(Order.user))

    The above query would join from the ``Order`` entity to its related
    ``User`` entity, and the returned ``Order`` objects would have the
    ``Order.user`` attribute pre-populated.

    :func:`contains_eager` also accepts an `alias` argument, which is the
    string name of an alias, an :func:`~sqlalchemy.sql.expression.alias`
    construct, or an :func:`~sqlalchemy.orm.aliased` construct. Use this when
    the eagerly-loaded rows are to come from an aliased table::

        user_alias = aliased(User)
        sess.query(Order).\\
                join((user_alias, Order.user)).\\
                options(contains_eager(Order.user, alias=user_alias))

    See also :func:`eagerload` for the "automatic" version of this
    functionality.

    For additional examples of :func:`contains_eager` see
    :ref:`contains_eager`.

    """
    alias = kwargs.pop('alias', None)
    if kwargs:
        raise exc.ArgumentError(
                'Invalid kwargs for contains_eager: %r' % list(kwargs.keys()))
    return _strategies.EagerLazyOption(keys, lazy='joined',
            propagate_to_loaders=False, chained=True), \
        _strategies.LoadEagerFromAliasOption(keys, alias=alias, chained=True)


def defer(*key):
    """Return a :class:`.MapperOption` that will convert the column property
    of the given name into a deferred load.

    Used with :meth:`.Query.options`.

    e.g.::

        from sqlalchemy.orm import defer

        query(MyClass).options(defer("attribute_one"),
                            defer("attribute_two"))

    A class bound descriptor is also accepted::

        query(MyClass).options(
                            defer(MyClass.attribute_one),
                            defer(MyClass.attribute_two))

    A "path" can be specified onto a related or collection object using a
    dotted name. The :func:`.orm.defer` option will be applied to that object
    when loaded::

        query(MyClass).options(
                            defer("related.attribute_one"),
                            defer("related.attribute_two"))

    To specify a path via class, send multiple arguments::

        query(MyClass).options(
                            defer(MyClass.related, MyOtherClass.attribute_one),
                            defer(MyClass.related, MyOtherClass.attribute_two))

    See also:

    :ref:`deferred`

    :param \*key: A key representing an individual path.   Multiple entries
     are accepted to allow a multiple-token path for a single target, not
     multiple targets.

    """
    return _strategies.DeferredOption(key, defer=True)


def undefer(*key):
    """Return a :class:`.MapperOption` that will convert the column property
    of the given name into a non-deferred (regular column) load.

    Used with :meth:`.Query.options`.

    e.g.::

        from sqlalchemy.orm import undefer

        query(MyClass).options(
                    undefer("attribute_one"),
                    undefer("attribute_two"))

    A class bound descriptor is also accepted::

        query(MyClass).options(
                    undefer(MyClass.attribute_one),
                    undefer(MyClass.attribute_two))

    A "path" can be specified onto a related or collection object using a
    dotted name. The :func:`.orm.undefer` option will be applied to that
    object when loaded::

        query(MyClass).options(
                    undefer("related.attribute_one"),
                    undefer("related.attribute_two"))

    To specify a path via class, send multiple arguments::

        query(MyClass).options(
                    undefer(MyClass.related, MyOtherClass.attribute_one),
                    undefer(MyClass.related, MyOtherClass.attribute_two))

    See also:

    :func:`.orm.undefer_group` as a means to "undefer" a group
    of attributes at once.

    :ref:`deferred`

    :param \*key: A key representing an individual path.   Multiple entries
     are accepted to allow a multiple-token path for a single target, not
     multiple targets.

    """
    return _strategies.DeferredOption(key, defer=False)


def undefer_group(name):
    """Return a :class:`.MapperOption` that will convert the given group of
    deferred column properties into a non-deferred (regular column) load.

    Used with :meth:`.Query.options`.

    e.g.::

        query(MyClass).options(undefer("group_one"))

    See also:

    :ref:`deferred`

    :param name: String name of the deferred group.   This name is
     established using the "group" name to the :func:`.orm.deferred`
     configurational function.

    """
    return _strategies.UndeferGroupOption(name)


class PropertyOption(MapperOption):
    """A MapperOption that is applied to a property off the mapper or
    one of its child mappers, identified by a dot-separated key
    or list of class-bound attributes. """

    def __init__(self, key, mapper=None):
        self.key = key
        self.mapper = mapper

    def process_query(self, query):
        self._process(query, True)

    def process_query_conditionally(self, query):
        self._process(query, False)

    def _process(self, query, raiseerr):
        paths = self._process_paths(query, raiseerr)
        if paths:
            self.process_query_property(query, paths)

    def process_query_property(self, query, paths):
        pass

    def __getstate__(self):
        d = self.__dict__.copy()
        d['key'] = ret = []
        for token in util.to_list(self.key):
            if isinstance(token, PropComparator):
                ret.append((token._parentmapper.class_, token.key))
            else:
                ret.append(token)
        return d

    def __setstate__(self, state):
        ret = []
        for key in state['key']:
            if isinstance(key, tuple):
                cls, propkey = key
                ret.append(getattr(cls, propkey))
            else:
                ret.append(key)
        state['key'] = tuple(ret)
        self.__dict__ = state

    def _find_entity_prop_comparator(self, query, token, mapper, raiseerr):
        if _is_aliased_class(mapper):
            searchfor = mapper
        else:
            searchfor = _class_to_mapper(mapper)
        for ent in query._mapper_entities:
            if ent.corresponds_to(searchfor):
                return ent
        else:
            if raiseerr:
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

    @util.dependencies("sqlalchemy.orm.util")
    def _process_paths(self, orm_util, query, raiseerr):
        """reconcile the 'key' for this PropertyOption with
        the current path and entities of the query.

        Return a list of affected paths.

        """
        path = PathRegistry.root
        entity = None
        paths = []
        no_result = []

        # _current_path implies we're in a
        # secondary load with an existing path
        current_path = list(query._current_path.path)

        tokens = deque(self.key)
        while tokens:
            token = tokens.popleft()
            if isinstance(token, str):
                # wildcard token
                if token.endswith(':*'):
                    return [path.token(token)]
                sub_tokens = token.split(".", 1)
                token = sub_tokens[0]
                tokens.extendleft(sub_tokens[1:])

                # exhaust current_path before
                # matching tokens to entities
                if current_path:
                    if current_path[1].key == token:
                        current_path = current_path[2:]
                        continue
                    else:
                        return no_result

                if not entity:
                    entity = self._find_entity_basestring(
                                        query,
                                        token,
                                        raiseerr)
                    if entity is None:
                        return no_result
                    path_element = entity.entity_zero
                    mapper = entity.mapper

                if hasattr(mapper.class_, token):
                    prop = getattr(mapper.class_, token).property
                else:
                    if raiseerr:
                        raise sa_exc.ArgumentError(
                            "Can't find property named '%s' on the "
                            "mapped entity %s in this Query. " % (
                                token, mapper)
                        )
                    else:
                        return no_result
            elif isinstance(token, PropComparator):
                prop = token.property

                # exhaust current_path before
                # matching tokens to entities
                if current_path:
                    if current_path[0:2] == \
                            [token._parententity, prop]:
                        current_path = current_path[2:]
                        continue
                    else:
                        return no_result

                if not entity:
                    entity = self._find_entity_prop_comparator(
                                            query,
                                            prop.key,
                                            token._parententity,
                                            raiseerr)
                    if not entity:
                        return no_result

                    path_element = entity.entity_zero
                    mapper = entity.mapper
            else:
                raise sa_exc.ArgumentError(
                        "mapper option expects "
                        "string key or list of attributes")
            assert prop is not None
            if raiseerr and not prop.parent.common_parent(mapper):
                raise sa_exc.ArgumentError("Attribute '%s' does not "
                            "link from element '%s'" % (token, path_element))

            path = path[path_element][prop]

            paths.append(path)

            if getattr(token, '_of_type', None):
                ac = token._of_type
                ext_info = inspect(ac)
                path_element = mapper = ext_info.mapper
                if not ext_info.is_aliased_class:
                    ac = orm_util.with_polymorphic(
                                ext_info.mapper.base_mapper,
                                ext_info.mapper, aliased=True,
                                _use_mapper_path=True)
                    ext_info = inspect(ac)
                path.set(query._attributes, "path_with_polymorphic", ext_info)
            else:
                path_element = mapper = getattr(prop, 'mapper', None)
                if mapper is None and tokens:
                    raise sa_exc.ArgumentError(
                        "Attribute '%s' of entity '%s' does not "
                        "refer to a mapped entity" %
                        (token, entity)
                    )

        if current_path:
            # ran out of tokens before
            # current_path was exhausted.
            assert not tokens
            return no_result

        return paths


class StrategizedOption(PropertyOption):
    """A MapperOption that affects which LoaderStrategy will be used
    for an operation by a StrategizedProperty.
    """

    chained = False

    def process_query_property(self, query, paths):
        strategy = self.get_strategy_class()
        if self.chained:
            for path in paths:
                path.set(
                    query._attributes,
                    "loaderstrategy",
                    strategy
                )
        else:
            paths[-1].set(
                query._attributes,
                "loaderstrategy",
                strategy
            )

    def get_strategy_class(self):
        raise NotImplementedError()



class DeferredOption(StrategizedOption):
    propagate_to_loaders = True

    def __init__(self, key, defer=False):
        super(DeferredOption, self).__init__(key)
        self.defer = defer

    def get_strategy_class(self):
        if self.defer:
            return DeferredColumnLoader
        else:
            return ColumnLoader


class UndeferGroupOption(MapperOption):
    propagate_to_loaders = True

    def __init__(self, group):
        self.group = group

    def process_query(self, query):
        query._attributes[("undefer", self.group)] = True


class EagerLazyOption(StrategizedOption):
    def __init__(self, key, lazy=True, chained=False,
                    propagate_to_loaders=True
                    ):
        if isinstance(key[0], str) and key[0] == '*':
            if len(key) != 1:
                raise sa_exc.ArgumentError(
                        "Wildcard identifier '*' must "
                        "be specified alone.")
            key = ("relationship:*",)
            propagate_to_loaders = False
        super(EagerLazyOption, self).__init__(key)
        self.lazy = lazy
        self.chained = chained
        self.propagate_to_loaders = propagate_to_loaders
        self.strategy_cls = properties.RelationshipProperty._strategy_lookup(lazy=lazy)

    def get_strategy_class(self):
        return self.strategy_cls


class EagerJoinOption(PropertyOption):

    def __init__(self, key, innerjoin, chained=False):
        super(EagerJoinOption, self).__init__(key)
        self.innerjoin = innerjoin
        self.chained = chained

    def process_query_property(self, query, paths):
        if self.chained:
            for path in paths:
                path.set(query._attributes, "eager_join_type", self.innerjoin)
        else:
            paths[-1].set(query._attributes, "eager_join_type", self.innerjoin)


class LoadEagerFromAliasOption(PropertyOption):

    def __init__(self, key, alias=None, chained=False):
        super(LoadEagerFromAliasOption, self).__init__(key)
        if alias is not None:
            if not isinstance(alias, str):
                info = inspect(alias)
                alias = info.selectable
        self.alias = alias
        self.chained = chained

    def process_query_property(self, query, paths):
        if self.chained:
            for path in paths[0:-1]:
                (root_mapper, prop) = path.path[-2:]
                adapter = query._polymorphic_adapters.get(prop.mapper, None)
                path.setdefault(query._attributes,
                            "user_defined_eager_row_processor",
                            adapter)

        root_mapper, prop = paths[-1].path[-2:]
        if self.alias is not None:
            if isinstance(self.alias, str):
                self.alias = prop.target.alias(self.alias)
            paths[-1].set(query._attributes,
                    "user_defined_eager_row_processor",
                    sql_util.ColumnAdapter(self.alias,
                                equivalents=prop.mapper._equivalent_columns)
            )
        else:
            if paths[-1].contains(query._attributes, "path_with_polymorphic"):
                with_poly_info = paths[-1].get(query._attributes,
                                                "path_with_polymorphic")
                adapter = orm_util.ORMAdapter(
                            with_poly_info.entity,
                            equivalents=prop.mapper._equivalent_columns)
            else:
                adapter = query._polymorphic_adapters.get(prop.mapper, None)
            paths[-1].set(query._attributes,
                                "user_defined_eager_row_processor",
                                adapter)

