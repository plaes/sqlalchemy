# orm/strategy_options.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

"""

from .interfaces import MapperOption, PropComparator
from .. import util
from ..sql.base import _generative, Generative
from .. import exc as sa_exc, inspect
from .base import _is_aliased_class, _class_to_mapper

class Load(Generative, MapperOption):
    def __init__(self, entity):
        insp = inspect(entity)
        self.path = insp._path_registry
        self.context = {}
        self.local_opts = {}

    def _generate(self):
        cloned = super(Load, self)._generate()
        cloned.local_opts = {}
        return cloned

    strategy = None
    propagate_to_loaders = False

    @util.memoized_property
    def _is_chain_link(self):
        return "chain" in self.local_opts

    def process_query(self, query):
        self._process(query, True)

    def process_query_conditionally(self, query):
        self._process(query, False)

    def _process(self, query, raiseerr):
        Load._transfer_to_query(self.context, query)

    @classmethod
    def _transfer_to_query(cls, context, query):
        for key, loader in context.items():
            if loader._is_chain_link:
                query._attributes.setdefault(key, loader)
            else:
                query._attributes[key] = loader

    @util.dependencies("sqlalchemy.orm.util")
    def _generate_path(self, orm_util, path, attr, raiseerr=True):
        if isinstance(attr, util.string_types):
            try:
                attr = path.entity.attrs[attr]
            except KeyError:
                if raiseerr:
                    raise sa_exc.ArgumentError(
                        "Can't find property named '%s' on the "
                        "mapped entity %s in this Query. " % (
                            attr, path.entity)
                    )
                else:
                    return None

            path = path[attr]
        else:
            prop = attr.property
            if getattr(attr, '_of_type', None):
                ac = attr._of_type
                ext_info = inspect(ac)

                path_element = ext_info.mapper
                if not ext_info.is_aliased_class:
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

    @_generative
    def _set_strategy(self, attr, strategy):
        self.path = self._generate_path(self.path, attr)
        self.strategy = strategy
        if strategy is not None:
            self.path.parent.set(self.context, "loader", self)

    @_generative
    def _set_column_strategy(self, attrs, strategy):
        for attr in attrs:
            path = self._generate_path(self.path, attr)
            cloned = self._generate()
            cloned.strategy = strategy
            cloned.path = path
            path.set(self.context, "loader", cloned)

    def defer(self, *attrs):
        return self._set_column_strategy(
                    attrs,
                    (("deferred", True), ("instrument", True))
                )

    def default(self, attr):
        return self._set_strategy(
                    attr,
                    None
                )

    def joined(self, attr, innerjoin=None):
        loader = self._set_strategy(
                    attr,
                    (("lazy", "joined"),)
                )
        if innerjoin is not None:
            loader.local_opts['innerjoin'] = innerjoin
        return loader

    def contains_eager(self, attr, alias=None):
        if alias is not None:
            if not isinstance(alias, str):
                info = inspect(alias)
                alias = info.selectable

        loader = self._set_strategy(
                    attr,
                    (("lazy", "joined"),)
                )
        loader.local_opts['eager_from_alias'] = alias
        return loader


    @util.memoized_property
    def strategy_impl(self):
        if self.path.has_entity:
            return self.path.parent.prop._get_strategy(self.strategy)
        else:
            return self.path.prop._get_strategy(self.strategy)

class _UnboundLoad(Load):
    def __init__(self):
        self.path = ()
        self._to_bind = set()
        self.local_opts = {}

    def _process(self, query, raiseerr):
        context = {}
        for val in self._to_bind:
            val._bind_loader(query, context, raiseerr)
        Load._transfer_to_query(context, query)

    @classmethod
    def _from_keys(self, meth, keys, chained, kw):
        opt = _UnboundLoad()

        def _split_key(key):
            if isinstance(key, util.string_types):
                return key.split(".")
            else:
                return (key,)
        all_tokens = [token for key in keys for token in _split_key(key)]

        for token in all_tokens[0:-1]:
            if chained:
                opt = meth(opt, token, **kw)
            else:
                opt = opt.default(token)
            opt.local_opts['chain'] = True

        opt = meth(opt, all_tokens[-1], **kw)

        return opt


    @_generative
    def _set_strategy(self, attr, strategy):
        self.path = self._generate_path(self.path, attr)
        self.strategy = strategy
        if strategy is not None:
            self._to_bind.add(self)

    @_generative
    def _set_column_strategy(self, attrs, strategy):
        for attr in attrs:
            path = self._generate_path(self.path, attr)
            cloned = self._generate()
            cloned.strategy = strategy
            cloned.path = path
            self._to_bind.add(cloned)

    def _bind_loader(self, query, context, raiseerr):
        start_path = self.path
        # _current_path implies we're in a
        # secondary load with an existing path
        current_path = query._current_path
        if current_path:
            start_path = self._chop_path(start_path, current_path)
        if not start_path:
            return None

        token = start_path[0]
        if isinstance(token, util.string_types):
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

        if entity is None:
            return None

        path_element = entity.entity_zero

        # transfer our entity-less state into a Load() object
        # with a real entity path.
        loader = Load(path_element)
        loader.context = context
        loader.strategy = self.strategy
        for token in start_path:
            loader.path = path = loader._generate_path(loader.path, token, raiseerr)
            if path is None:
                return

        loader.local_opts.update(self.local_opts)
        if loader.path.has_entity:
            loader.path.parent.set(context, "loader", loader)
        else:
            loader.path.set(context, "loader", loader)


    def _generate_path(self, path, attr):
        return path + (attr, )

    def _chop_path(self, to_chop, path):
        i = -1
        for i, (c_token, (p_mapper, p_prop)) in enumerate(zip(to_chop, path.pairs())):

            if isinstance(c_token, util.string_types):
                if c_token != p_prop.key:
                    return None
            elif isinstance(c_token, PropComparator):
                if c_token.property is not p_prop:
                    return None
        else:
            i += 1

        return to_chop[i:]

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

    @classmethod
    def _joinedload(cls, *keys, **kw):
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
        return cls._from_keys(cls.joined, keys, False, kw)

    @classmethod
    def _joinedload_all(cls, *keys, **kw):
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
        return cls._from_keys(cls.joined, keys, True, kw)

    @classmethod
    def _contains_eager(cls, *keys, **kw):
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
        return cls._from_keys(cls.contains_eager, keys, True, kw)



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

