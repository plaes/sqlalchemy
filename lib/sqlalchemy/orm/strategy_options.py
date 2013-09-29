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
from . import util as orm_util
from .path_registry import PathRegistry

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

    def process_query(self, query):
        self._process(query, True)

    def process_query_conditionally(self, query):
        self._process(query, False)

    def _process(self, query, raiseerr):
        query._attributes.update(self.context)

    def _generate_path(self, path, attr, wildcard_key, raiseerr=True):
        if raiseerr and not path.has_entity:
            raise sa_exc.ArgumentError(
                "Attribute '%s' of entity '%s' does not "
                "refer to a mapped entity" %
                (path.key, path.parent.entity)
            )

        if isinstance(attr, util.string_types):
            if attr.endswith('*'):
                if wildcard_key:
                    attr = "%s:*" % wildcard_key
                return path.token(attr)

            try:
                # use getattr on the class to work around
                # synonyms, hybrids, etc.
                attr = getattr(path.entity.class_, attr)
            except AttributeError:
                if raiseerr:
                    raise sa_exc.ArgumentError(
                        "Can't find property named '%s' on the "
                        "mapped entity %s in this Query. " % (
                            attr, path.entity)
                    )
                else:
                    return None
            else:
                attr = attr.property

            path = path[attr]
        else:
            prop = attr.property

            if raiseerr and not prop.parent.common_parent(path.mapper):
                raise sa_exc.ArgumentError("Attribute '%s' does not "
                            "link from element '%s'" % (attr, path.entity))

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
    def _set_strategy(self, attr, strategy, propagate_to_loaders=True):
        self.path = self._generate_path(self.path, attr, "relationship")
        self.strategy = strategy
        self.propagate_to_loaders = propagate_to_loaders
        if strategy is not None:
            self._set_path_strategy()

    @_generative
    def _set_column_strategy(self, attrs, strategy, opts=None):
        for attr in attrs:
            path = self._generate_path(self.path, attr, "column")
            cloned = self._generate()
            cloned.strategy = strategy
            cloned.path = path
            cloned.propagate_to_loaders = True
            if opts:
                cloned.local_opts.update(opts)
            cloned._set_path_strategy()

    def _set_path_strategy(self):
        if self.path.has_entity:
            self.path.parent.set(self.context, "loader", self)
        else:
            self.path.set(self.context, "loader", self)

    def __getstate__(self):
        d = self.__dict__.copy()
        d["path"] = self.path.serialize()
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.path = PathRegistry.deserialize(self.path)

    def defer(self, *attrs):
        return self._set_column_strategy(
                    attrs,
                    (("deferred", True), ("instrument", True))
                )

    def undefer(self, *attrs):
        return self._set_column_strategy(
                    attrs,
                    (("deferred", False), ("instrument", True))
                )

    def default(self, attr):
        return self._set_strategy(
                    attr,
                    None
                )

    def load_only(self, *attrs):
        cloned = self._set_column_strategy(
                    attrs,
                    (("deferred", False), ("instrument", True))
                )
        cloned._set_column_strategy("*",
                        (("deferred", True), ("instrument", True)))
        return cloned

    def undefer_group(self, name):
        return self._set_column_strategy(
                                "*",
                                None,
                                {"undefer_group": name}
                        )

    def joinedload(self, attr, innerjoin=None):
        loader = self._set_strategy(
                    attr,
                    (("lazy", "joined"),)
                )
        if innerjoin is not None:
            loader.local_opts['innerjoin'] = innerjoin
        return loader

    def subqueryload(self, attr):
        loader = self._set_strategy(
                    attr,
                    (("lazy", "subquery"),)
                )
        return loader

    def lazyload(self, attr):
        loader = self._set_strategy(
                    attr,
                    (("lazy", "select"),)
                )
        return loader

    def immediateload(self, attr):
        loader = self._set_strategy(
                    attr,
                    (("lazy", "immediate"),)
                )
        return loader

    def noload(self, attr):
        loader = self._set_strategy(
                    attr,
                    (("lazy", "noload"),)
                )
        return loader

    def contains_eager(self, attr, alias=None):
        if alias is not None:
            if not isinstance(alias, str):
                info = inspect(alias)
                alias = info.selectable

        loader = self._set_strategy(
                    attr,
                    (("lazy", "joined"),),
                    propagate_to_loaders=False
                )
        loader.local_opts['eager_from_alias'] = alias
        return loader


class _UnboundLoad(Load):
    """Represent a loader option that isn't tied to a root entity.

    The loader option will produce an entity-linked :class:`.Load`
    object when it is passed :meth:`.Query.options`.

    This provides compatibility with the traditional system
    of freestanding options, e.g. ``joinedload('x.y.z')``.

    """
    def __init__(self):
        self.path = ()
        self._to_bind = set()
        self.local_opts = {}

    _is_chain_link = False

    def _set_path_strategy(self):
        self._to_bind.add(self)

    def _generate_path(self, path, attr, wildcard_key):
        if wildcard_key and isinstance(attr, util.string_types) and attr == '*':
            attr = "%s:*" % wildcard_key

        return path + (attr, )

    def __getstate__(self):
        d = self.__dict__.copy()
        d['path'] = ret = []
        for token in util.to_list(self.path):
            if isinstance(token, PropComparator):
                ret.append((token._parentmapper.class_, token.key))
            else:
                ret.append(token)
        return d

    def __setstate__(self, state):
        ret = []
        for key in state['path']:
            if isinstance(key, tuple):
                cls, propkey = key
                ret.append(getattr(cls, propkey))
            else:
                ret.append(key)
        state['path'] = tuple(ret)
        self.__dict__ = state

    def _process(self, query, raiseerr):
        for val in self._to_bind:
            val._bind_loader(query, query._attributes, raiseerr)

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
            opt._is_chain_link = True

        opt = meth(opt, all_tokens[-1], **kw)
        opt._is_chain_link = False

        return opt


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

        if not entity:
            return

        path_element = entity.entity_zero

        # transfer our entity-less state into a Load() object
        # with a real entity path.
        loader = Load(path_element)
        loader.context = context
        loader.strategy = self.strategy

        path = loader.path
        for token in start_path:
            loader.path = path = loader._generate_path(
                                        loader.path, token, None, raiseerr)
            if path is None:
                return

        loader.local_opts.update(self.local_opts)

        if loader.path.has_entity:
            effective_path = loader.path.parent
        else:
            effective_path = loader.path

        # prioritize "first class" options over those
        # that were "links in the chain", e.g. "x" and "y" in someload("x.y.z")
        # versus someload("x") / someload("x.y")
        if self._is_chain_link:
            effective_path.setdefault(context, "loader", loader)
        else:
            effective_path.set(context, "loader", loader)

    def _chop_path(self, to_chop, path):
        i = -1
        for i, (c_token, (p_mapper, p_prop)) in enumerate(zip(to_chop, path.pairs())):
            if isinstance(c_token, util.string_types):
                if c_token != 'relationship:*' and c_token != p_prop.key:
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
        if token.endswith(':*'):
            if len(list(query._mapper_entities)) != 1:
                if raiseerr:
                    raise sa_exc.ArgumentError(
                            "Wildcard loader can only be used with exactly "
                            "one entity.  Use Load(ent) to specify "
                            "specific entities.")

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
        return cls._from_keys(cls.joinedload, keys, False, kw)

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
        return cls._from_keys(cls.joinedload, keys, True, kw)

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

    @classmethod
    def _lazyload(cls, *keys):
        """Return a ``MapperOption`` that will convert the property of the given
        name or series of mapped attributes into a lazy load.

        Used with :meth:`~sqlalchemy.orm.query.Query.options`.

        See also:  :func:`eagerload`, :func:`subqueryload`, :func:`immediateload`

        """
        return cls._from_keys(cls.lazyload, keys, False, {})

    @classmethod
    def _lazyload_all(cls, *keys):
        """Return a ``MapperOption`` that will convert all the properties
        along the given dot-separated path or series of mapped attributes
        into a lazy load.

        Used with :meth:`~sqlalchemy.orm.query.Query.options`.

        See also:  :func:`eagerload`, :func:`subqueryload`, :func:`immediateload`

        """
        return cls._from_keys(cls.lazyload, keys, True, {})

    @classmethod
    def _load_only(cls, *attrs):
        """Load only column attributes *attrs; all other column attributes
        are set as deferred."""
        return _UnboundLoad().load_only(*attrs)

    @classmethod
    def _undefer_group(cls, name):
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
        return _UnboundLoad().undefer_group(name)


    @classmethod
    def _defer(cls, *key):
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
        return cls._from_keys(cls.defer, key, False, {})

    @classmethod
    def _undefer(cls, *key):
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
        return cls._from_keys(cls.undefer, key, False, {})


    @classmethod
    def _subqueryload(cls, *keys):
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
        return cls._from_keys(cls.subqueryload, keys, False, {})


    @classmethod
    def _subqueryload_all(cls, *keys):
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
        return cls._from_keys(cls.subqueryload, keys, True, {})

    @classmethod
    def _noload(cls, *keys):
        """Return a ``MapperOption`` that will convert the property of the
        given name or series of mapped attributes into a non-load.

        Used with :meth:`~sqlalchemy.orm.query.Query.options`.

        See also:  :func:`lazyload`, :func:`eagerload`,
        :func:`subqueryload`, :func:`immediateload`

        """
        return cls._from_keys(cls.noload, keys, False, {})


    @classmethod
    def _immediateload(cls, *keys):
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
        return cls._from_keys(cls.immediateload, keys, False, {})

