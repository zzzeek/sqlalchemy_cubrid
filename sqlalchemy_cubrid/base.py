# cubrid/base.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import datetime, inspect, re, sys
from sqlalchemy import schema as sa_schema
from sqlalchemy import exc, log, sql, util
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy.sql import functions as sql_functions
from sqlalchemy.sql import compiler
from array import array as _array

from sqlalchemy.engine import reflection
from sqlalchemy.engine import base as engine_base, default
from sqlalchemy import types as sqltypes
from sqlalchemy.util import topological
from sqlalchemy.types import BOOLEAN, CLOB, DATE, DATETIME, INTEGER, TEXT, TIME, TIMESTAMP

#CUBRID Reserved Keywords (http://www.cubrid.org/manual/841/en/Reserved%20Words)
RESERVED_WORDS = set(
    ['absolute','action','add','add_months','after','alias','all','allocate',
    'alter','and','any','are','as','asc','assertion','async','at','attach',
    'attribute','avg','before','between','bigint','bit','bit_length','blob',
    'boolean','both','breadth','by','call','cascade','cascaded','case','cast',
    'catalog','change','char','character','check','class','classes','clob',
    'close','cluster','coalesce','collate','collation','column','commit',
    'completion','connect','connect_by_iscycle','connect_by_isleaf',
    'connect_by_root','connection','constraint','constraints','continue',
    'convert','corresponding','count','create','cross','current',
    'current_date','current_datetime','current_time','current_timestamp',
    'current_user','cursor','cycle','data','data_type','database','date',
    'datetime','day','day_hour','day_millisecond','day_minute','day_second',
    'deallocate','dec','decimal','declare','default','deferrable','deferred',
    'delete','depth','desc','describe','descriptor','diagnostics','dictionary',
    'difference','disconnect','distinct','distinctrow','div','do','domain',
    'double','duplicate','drop','each','else','elseif','end','equals','escape',
    'evaluate','except','exception','exclude','exec','execute','exists',
    'external','extract','false','fetch','file','first','float','for',
    'foreign','found','from','full','function','general','get','global','go',
    'goto','grant','group','having','hour','hour_millisecond','hour_minute',
    'hour_second','identity','if','ignore','immediate','in','index',
    'indicator','inherit','initially','inner','inout','input','insert','int',
    'integer','intersect','intersection','interval','into','is','isolation',
    'join','key','language','last','ldb','leading','leave','left','less',
    'level','like','limit','list','local','local_transaction_id','localtime',
    'localtimestamp','loop','lower','match','max','method','millisecond','min',
    'minute','minute_millisecond','minute_second','mod','modify','module',
    'monetary','month','multiset','multiset_of','na','names','national',
    'natural','nchar','next','no','none','not','null','nullif','numeric',
    'object','octet_length','of','off','oid','on','only','open','operation',
    'operators','optimization','option','or','order','others','out','outer',
    'output','overlaps','parameters','partial','pendant','position',
    'precision','preorder','prepare','preserve','primary','prior','private',
    'privileges','procedure','protected','proxy','query','read','real',
    'recursive','ref','references','referencing','register','relative',
    'rename','replace','resignal','restrict','return','returns','revoke',
    'right','role','rollback','rollup','routine','row','rownum','rows',
    'savepoint','schema','scope','scroll','search','second',
    'second_millisecond','section','select','sensitive','sequence',
    'sequence_of','serializable','session','session_user','set','set_of',
    'seteq','shared','siblings','signal','similar','size','smallint','some',
    'sql','sqlcode','sqlerror','sqlexception','sqlstate','sqlwarning',
    'statistics','string','structure','subclass','subset','subseteq',
    'substring','sum','superclass','superset','superseteq',
    'sys_connect_by_path','sys_date','sys_datetime','sys_time','sys_timestamp',
    'sys_user','sysdate','sysdatetime','system_user','systime','table',
    'temporary','test','then','there','time','timestamp','timezone_hour',
    'timezone_minute','to','trailing','transaction','translate','translation',
    'trigger','trim','true','truncate','type','under','union','unique',
    'unknown','update','upper','usage','use','user','using','utime','value',
    'values','varchar','variable','varying','vclass','view','virtual',
    'visible','wait','when','whenever','where','while','with','without','work',
    'write','xor','year','year_month','zone'
     ])

AUTOCOMMIT_RE = re.compile(r'\s*(?:UPDATE|INSERT|CREATE|DELETE|DROP|ALTER|LOAD +DATA|REPLACE)',re.I | re.UNICODE)

class _NumericType(object):
    """Base for CUBRID numeric types."""
    def __init__(self, **kw):
        super(_NumericType, self).__init__(**kw)

class _FloatType(_NumericType, sqltypes.Float):
    def __init__(self, precision=None, **kw):
        super(_FloatType, self).__init__(precision=precision, **kw)

class _IntegerType(_NumericType, sqltypes.Integer):
    def __init__(self, display_width=None, **kw):
        self.display_width = display_width
        super(_IntegerType, self).__init__(**kw)

class _StringType(sqltypes.String):
    """Base for CUBRID string types."""
    def __init__(self, national=False, values=None, **kw):
        self.national = national
        self.values = values
        super(_StringType, self).__init__(**kw)

    def __repr__(self):
        attributes = inspect.getargspec(self.__init__)[0][1:]
        attributes.extend(inspect.getargspec(_StringType.__init__)[0][1:])

        params = {}
        for attr in attributes:
            val = getattr(self, attr)
            if val is not None and val is not False:
                params[attr] = val

        return "%s(%s)" % (self.__class__.__name__,', '.join(['%s=%r' % (k, params[k]) for k in params]))

class NUMERIC(_NumericType, sqltypes.NUMERIC):
    """CUBRID NUMERIC type.
    Default value is NUMERIC(15,0)
    """
    __visit_name__ = 'NUMERIC'
    def __init__(self, precision=None, scale=None, **kw):
        super(NUMERIC, self).__init__(precision=precision, scale=scale, **kw)

class BLOB(sqltypes.LargeBinary):
    __visit_name__ = 'BLOB'

class DECIMAL(_NumericType, sqltypes.DECIMAL):
    __visit_name__ = 'DECIMAL'
    """CUBRID DECIMAL type."""
    def __init__(self, precision=None, scale=None, **kw):
        super(DECIMAL, self).__init__(precision=precision, scale=scale, **kw)

class DOUBLE(_FloatType):
    """CUBRID DOUBLE type."""
    __visit_name__ = 'DOUBLE'

class FLOAT(_FloatType, sqltypes.FLOAT):
    """CUBRID FLOAT type."""
    __visit_name__ = 'FLOAT'
    def __init__(self, precision=None, **kw):
        super(FLOAT, self).__init__(precision=precision, **kw)
    def bind_processor(self, dialect):
        return None

class SMALLINT(_IntegerType, sqltypes.SMALLINT):
    __visit_name__ = 'SMALLINT'

class BIGINT(_IntegerType, sqltypes.BIGINT):
    __visit_name__ = 'BIGINT'

class BIT(sqltypes.TypeEngine):
    __visit_name__ = 'BIT'
    def __init__(self, length=None, varying=False):
        if not varying:
            self.length = length or 1   # BIT without VARYING defaults to length 1
        else:
            self.length = length        # but BIT VARYING can be unlimited-length, so no default
        self.varying = varying

class VARCHAR(_StringType, sqltypes.VARCHAR):
    """CUBRID VARCHAR type, for variable-length character data."""
    __visit_name__ = 'VARCHAR'

    def __init__(self, length=None, **kwargs):
        super(VARCHAR, self).__init__(length=length, **kwargs)

class CHAR(_StringType, sqltypes.CHAR):
    """CUBRID CHAR type, for fixed-length character data."""

    __visit_name__ = 'CHAR'

    def __init__(self, length=None, **kwargs):
        super(CHAR, self).__init__(length=length, **kwargs)

class NVARCHAR(_StringType, sqltypes.NVARCHAR):
    """CUBRID NVARCHAR type.
    For variable-length character data in the server's configured national
    character set.
    """
    __visit_name__ = 'NVARCHAR'

    def __init__(self, length=None, **kwargs):
        kwargs['national'] = True
        super(NVARCHAR, self).__init__(length=length, **kwargs)

class NCHAR(_StringType, sqltypes.NCHAR):
    """CUBRID NCHAR type.
    For fixed-length character data in the server's configured national
    character set.
    """
    __visit_name__ = 'NCHAR'

    def __init__(self, length=None, **kwargs):
        kwargs['national'] = True
        super(NCHAR, self).__init__(length=length, **kwargs)

class STRING(_StringType):
    """CUBRID STRING type
    STRING is a variable-length character string data type.
    STRING is the same as the VARCHAR with the length specified to the maximum value.
    That is, STRING and VARCHAR(1,073,741,823) have the same value.
    """
    __visit_name__ = 'STRING'

    def __init__(self, length=None, national=False, **kwargs):
        super(STRING, self).__init__(length=length, **kwargs)

class MONETARY(_FloatType):
    """CUBRID MONETARY type"""
    __visit_name__ = 'MONETARY'

class OBJECT(_StringType):
    """CUBRID OBJECT type."""
    __visit_name__ = 'OBJECT'

class SET(_StringType):
    """CUBRID SET type."""
    __visit_name__ = 'SET'

    def __init__(self, *values, **kw):
        self._ddl_values = values
        super(SET, self).__init__(**kw)

class MULTISET(_StringType):
    """CUBRID MULTISET type."""
    __visit_name__ = 'MULTISET'

    def __init__(self, *values, **kw):
        self._ddl_values = values
        super(MULTISET, self).__init__(**kw)

class SEQUENCE(_StringType):
    """CUBRID SEQUENCE type."""
    __visit_name__ = 'SEQUENCE'

    def __init__(self, *values, **kw):
        self._ddl_values = values
        super(SEQUENCE, self).__init__(**kw)

colspecs = {
    sqltypes.Numeric: NUMERIC,
    sqltypes.Float: FLOAT,
    sqltypes.Time: TIME,
}

ischema_names = {
    'bigint': BIGINT,
    'bit': BIT,
    'bit varying': BIT,
    'blob': BLOB,
    'char': CHAR,
    'character varying' : VARCHAR,
    'clob': CLOB,
    'date': DATE,
    'datetime': DATETIME,
    'decimal': DECIMAL,
    'double': DOUBLE,
    'float': FLOAT,
    'integer': INTEGER,
    'list': SEQUENCE,
    'monetary': MONETARY,
    'multiset': MULTISET,
    'nchar': NCHAR,
    'nvarchar': NVARCHAR,
    'numeric': NUMERIC,
    'object': OBJECT,
    'sequence': SEQUENCE,
    'set': SET,
    'smallint': SMALLINT,
	'short': SMALLINT,
    'string': STRING,
    'time': TIME,
    'timestamp': TIMESTAMP,
    'varbit': BIT,
    'varchar': VARCHAR,
    'varnchar': NVARCHAR,
}

class CUBRIDExecutionContext(default.DefaultExecutionContext):
    def should_autocommit_text(self, statement):
        return AUTOCOMMIT_RE.match(statement)

class CUBRIDCompiler(compiler.SQLCompiler):
    def visit_release_savepoint(self, savepoint_stmt):
        return ""

    def visit_utc_timestamp_func(self, fn, **kw):
        return "UTC_TIME()"

    def visit_sysdate_func(self, fn, **kw):
        return "SYSDATE"

    def visit_concat_op(self, binary, **kw):
        return "concat(%s, %s)" % (self.process(binary.left), self.process(binary.right))

    def visit_match_op(self, binary, **kw):
        return "(LOCATE(LOWER(%s), LOWER(%s)) > 0)" % (self.process(binary.right), self.process(binary.left))

    def visit_typeclause(self, typeclause):
        type_ = typeclause.type.dialect_impl(self.dialect)
        if isinstance(type_, sqltypes.Integer):
            return 'INTEGER'
        elif isinstance(type_, sqltypes.TIMESTAMP):
            return 'DATETIME'
        elif isinstance(type_, (sqltypes.DECIMAL, sqltypes.DateTime, sqltypes.Date, sqltypes.Time)):
            return self.dialect.type_compiler.process(type_)
        elif isinstance(type_, sqltypes.Text):
            return 'CHAR'
        elif (isinstance(type_, sqltypes.String) and not isinstance(type_, (SET, MULTISET, SEQUENCE))):
            if getattr(type_, 'length'):
                return 'CHAR(%s)' % type_.length
            else:
                return 'CHAR'
        elif isinstance(type_, sqltypes.NUMERIC):
            return self.dialect.type_compiler.process(type_).replace('NUMERIC', 'DECIMAL')
        else:
            return None

    def visit_cast(self, cast, **kwargs):
        type_ = self.process(cast.typeclause)
        if type_ is None:
            return self.process(cast.clause.self_group())

        return 'CAST(%s AS %s)' % (self.process(cast.clause), type_)

    def render_literal_value(self, value, type_):
        value = super(CUBRIDCompiler, self).render_literal_value(value, type_)
        value = value.replace('\\', '\\\\')
        return value

    def get_select_precolumns(self, select):
        if select._distinct:
            return "DISTINCT "
        else:
            return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        return ''.join(
            (self.process(join.left, asfrom=True, **kwargs),
             (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN "),
             self.process(join.right, asfrom=True, **kwargs),
             " ON ",
             self.process(join.onclause, **kwargs)))

    def limit_clause(self, select):
        # CUBRID supports: LIMIT <limit> and LIMIT <offset>, <limit>
        limit, offset = select._limit, select._offset
        if (limit, offset) == (None, None):
            return ''
        elif limit is None and offset is not None:
             return ' \n LIMIT %s, 1073741823' % (self.process(sql.literal(offset)))
        elif offset is not None:
            return ' \n LIMIT %s, %s' % (self.process(sql.literal(offset)), self.process(sql.literal(limit)))
        else:
            return ' \n LIMIT %s' % (self.process(sql.literal(limit)),)

    def for_update_clause(self, select):
        """FOR UPDATE is not supported by CUBRID; silently ignore"""
        return ''

    def update_limit_clause(self, update_stmt):
        limit = update_stmt.kwargs.get('%s_limit' % self.dialect.name, None)
        if limit:
            return "LIMIT %s" % limit
        else:
            return None

    def update_tables_clause(self, update_stmt, from_table, extra_froms, **kw):
        return ', '.join(t._compiler_dispatch(self, asfrom=True, **kw)
                    for t in [from_table] + list(extra_froms))

    def update_from_clause(self, update_stmt, from_table,
                                extra_froms, from_hints, **kw):
        return None

class CUBRIDDDLCompiler(compiler.DDLCompiler):
    def define_constraint_cascades(self, constraint):
        text = ""

        #You can use either NO ACTION, RESTRICT, CASCADE, or SET NULL
        #Set the default to "ON DELETE CASCADE"
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete
        else:
            text += " ON DELETE CASCADE"

        #You can use either NO ACTION, RESTRICT, or SET NULL
        #The default is "ON UPDATE RESTRICT"
        if constraint.onupdate is not None:
            text += " ON UPDATE %s" % constraint.onupdate

        return text

    def get_column_specification(self, column, **kw):
        """Builds column DDL."""

        colspec = [self.preparer.format_column(column),self.dialect.type_compiler.process(column.type)]
        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append('DEFAULT ' + default)

        is_timestamp = isinstance(column.type, sqltypes.TIMESTAMP)
        if not column.nullable and not is_timestamp:
            colspec.append('NOT NULL')

        elif column.nullable and is_timestamp and default is None:
            colspec.append('NULL')

        if column is column.table._autoincrement_column and column.server_default is None:
            colspec.append('AUTO_INCREMENT')

        return ' '.join(colspec)

    def visit_create_index(self, create):
        index = create.element
        if "primary_key=True" in index.__repr__():
            return None
        preparer = self.preparer
        table = preparer.format_table(index.table)
        columns = [preparer.quote(c.name, c.quote) for c in index.columns]
        name = preparer.quote(self._index_identifier(index.name), index.quote)

        text = "CREATE "
        if 'reverse' in index.kwargs and index.kwargs['reverse']:
            text += "REVERSE "
        if index.unique:
            text += "UNIQUE "
        text += "INDEX %s ON %s " % (name, table)

        columns = ', '.join(columns)
        if 'length' in index.kwargs:
            length = index.kwargs['length']
            text += "(%s(%d))" % (columns, length)
        else:
            text += "(%s)" % (columns)

        #By default, the index is sorted ASCending
        if 'desc' in index.kwargs and index.kwargs['desc']:
            text += " DESC"

        return text

    def visit_primary_key_constraint(self, constraint):
        return super(CUBRIDDDLCompiler, self).visit_primary_key_constraint(constraint)

    def visit_drop_index(self, drop):
        index = drop.element
        text = "\nDROP "
        if 'cubrid_reverse' in index.kwargs and index.kwargs['cubrid_reverse']:
            text += "REVERSE "
        if index.unique:
            text += "UNIQUE "
        return text+"INDEX %s ON %s" % \
                    (self.preparer.quote(self._index_identifier(index.name), index.quote),
                     self.preparer.format_table(index.table))

    def visit_drop_constraint(self, drop):
        constraint = drop.element
        if isinstance(constraint, sa_schema.ForeignKeyConstraint):
            qual = "FOREIGN KEY "
            const = self.preparer.format_constraint(constraint)
        elif isinstance(constraint, sa_schema.PrimaryKeyConstraint):
            qual = "PRIMARY KEY "
            const = ""
        elif isinstance(constraint, sa_schema.UniqueConstraint):
            qual = ""
            if constraint.reverse:
                qual += "REVERSE "
            if constraint.unique:
                qual += "UNIQUE "
            qual += "INDEX "
            const = self.preparer.format_constraint(constraint)
        else:
            qual = ""
            const = self.preparer.format_constraint(constraint)
        return "ALTER TABLE %s DROP %s%s" % \
                    (self.preparer.format_table(constraint.table),qual, const)

class CUBRIDTypeCompiler(compiler.GenericTypeCompiler):
    def _cubrid_type(self, type_):
        return isinstance(type_, (_StringType, _NumericType))

    def visit_boolean(self, type_):
        return self.visit_SMALLINT(type_)

    def visit_NUMERIC(self, type_):
        if type_.precision is None:
            return "NUMERIC"
        elif type_.scale is None:
            return "NUMERIC(%(precision)s)" % {'precision': type_.precision}
        else:
            return "NUMERIC(%(precision)s, %(scale)s)" % {'precision': type_.precision, 'scale' : type_.scale}

    def visit_DECIMAL(self, type_):
        if type_.precision is None:
            return "DECIMAL"
        elif type_.scale is None:
            return "DECIMAL(%(precision)s)" % {'precision': type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {'precision': type_.precision, 'scale' : type_.scale}

    def visit_FLOAT(self, type_):
        if type_.precision is not None:
            return "FLOAT(%s)" % (type_.precision)
        else:
            return "FLOAT"

    def visit_DOUBLE(self, type_):
        return "DOUBLE"

    def visit_MONETARY(self, type_):
        return "MONETARY"

    def visit_SMALLINT(self, type_):
        return "SMALLINT"

    def visit_BIGINT(self, type_):
        return "BIGINT"

    def visit_BIT(self, type_):
        if type_.varying:
            compiled = "BIT VARYING"
            if type_.length is not None:
                compiled += "(%d)" % type_.length
        else:
            compiled = "BIT(%d)" % type_.length
        return compiled

    def visit_datetime(self, type_):
        return "DATETIME"

    def visit_DATETIME(self, type_):
        return "DATETIME"

    def visit_DATE(self, type_):
        return "DATE"

    def visit_TIME(self, type_):
        return "TIME"

    def visit_TIMESTAMP(self, type_):
        return "TIMESTAMP"

    def visit_VARCHAR(self, type_):
        if hasattr(type_, 'national') and type_.national:
            return self.visit_NVARCHAR(type_)
        elif type_.length:
            return "VARCHAR(%d)" % type_.length
        else:
            return "VARCHAR(4096)"

    def visit_CHAR(self, type_):
        if hasattr(type_, 'national') and type_.national:
            return self.visit_NCHAR(type_)
        elif type_.length:
            return "CHAR(%(length)s)" % {'length' : type_.length}
        else:
            return "CHAR"

    def visit_NVARCHAR(self, type_):
        if type_.length:
            return "NCHAR VARYING(%(length)s)" % {'length': type_.length}
        else:
            return "NCHAR VARYING(4096)"

    def visit_NCHAR(self, type_):
        if type_.length:
            return "NCHAR(%(length)s)" % {'length': type_.length}
        else:
            return "NCHAR"

    def visit_OBJECT(self, type_):
        return "OBJECT"

    def visit_large_binary(self, type_):
        return self.visit_BLOB(type_)

    def visit_text(self, type_):
        return self.visit_STRING(type_)

    def visit_BLOB(self, type_):
        return "BLOB"

    def visit_CLOB(self, type_):
        return "CLOB"

    def visit_STRING(self, type_):
        return "STRING"

    def visit_SET(self, type_):
        return self.visit_list(type_, "SET")

    def visit_MULTISET(self, type_):
        return self.visit_list(type_, "MULTISET")

    def visit_SEQUENCE(self, type_):
        return self.visit_list(type_, "SEQUENCE")

    def visit_list(self, type_, list_type):
        """CUBRID supports SET, MULTISET and LIST/SEQUENCE"""
        first = True
        compiled = list_type+"("
        for value in type_._ddl_values:
            if not first :
                compiled += ","
            if isinstance(value,basestring):
                compiled += value
            else:
                compiled += value.__visit_name__
            first = False
        compiled += ")"
        return compiled

class CUBRIDIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def __init__(self, dialect):
        super(CUBRIDIdentifierPreparer, self).__init__(dialect, initial_quote='[', final_quote=']')

    def _quote_free_identifiers(self, *ids):
        """Unilaterally identifier-quote any number of strings."""
        return tuple([self.quote_identifier(i) for i in ids if i is not None])

class CUBRIDDialect(default.DefaultDialect):
    name = 'cubrid'

    max_identifier_length = 255
    max_index_name_length = 255

    supports_alter = True
    supports_native_enum = False        #CUBRID will support ENUM starting CUBRID Apricot
    supports_native_boolean = False     #CUBRID does not have the BOOLEAN type
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_unicode_binds = True       #CUBRID support Unicode
    implicit_returning = False          #CUBRID does not support implicit returning

    default_paramstyle = 'format'
    ischema_names = ischema_names
    colspecs = colspecs

    statement_compiler = CUBRIDCompiler
    ddl_compiler = CUBRIDDDLCompiler
    type_compiler = CUBRIDTypeCompiler
    preparer = CUBRIDIdentifierPreparer

    def __init__(self, use_ansiquotes=None, isolation_level=None, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)
        self.isolation_level = isolation_level

    def on_connect(self):
        if self.isolation_level is not None:
            def connect(conn):
                self.set_isolation_level(conn, self.isolation_level)
            return connect
        else:
            return None

    _isolation_lookup = set(['READ COMMITTED SCHEMA, READ UNCOMMITTED INSTANCES',
         'READ COMMITTED SCHEMA, READ COMMITTED INSTANCES',
         'REPEATABLE READ SCHEMA, READ UNCOMMITTED INSTANCES',
		 'REPEATABLE READ SCHEMA, READ COMMITTED INSTANCES',
         'REPEATABLE READ SCHEMA, REPEATABLE READ INSTANCES', 'SERIALIZABLE'])

    def set_isolation_level(self, connection, level):
        level = level.replace('_', ' ')
        if level not in self._isolation_lookup:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s" %
                (level, self.name, ", ".join(self._isolation_lookup))
                )
        cursor = connection.cursor()
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL %s" % level)
        cursor.execute("COMMIT")
        cursor.close()

    def get_isolation_level(self, connection):
        """CUBRID has a small issue of retrieving the isolation level.
		It works in CSQL, but does not work in python driver.
		Currently, it returns the default isolation level.

		"""
        return "REPEATABLE READ SCHEMA, READ UNCOMMITTED INSTANCES"

    def do_commit(self, connection):
        """Execute a COMMIT."""
        try:
            connection.commit()
        except:
            raise

    def do_rollback(self, connection):
        """Execute a ROLLBACK."""
        try:
            connection.rollback()
        except:
            raise

    def _get_default_schema_name(self, connection):
        return connection.execute('SELECT DATABASE()').scalar()

    def has_table(self, connection, table_name, schema=None):
        #Check if table name exists within _db_class system table
        st = "select * from _db_class WHERE class_name = '%s'" % table_name.lower()
        rs = None
        try:
            try:
                rs = connection.execute(st)
                have = rs.rowcount > 0
                rs.close()
                return have
            except exc.DBAPIError:
                raise
        finally:
            if rs:
                rs.close()

    def initialize(self, connection):
        default.DefaultDialect.initialize(self, connection)
        self._server_casing = 1       # CUBRID is not case sensitive when it comes to table names

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        rp = connection.execute("SELECT * FROM db_class")
        return [row[0] for row in rp.fetchall() if row[2] == 'CLASS' and row[3] == 'NO']

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        rp = connection.execute("SELECT * FROM db_class")
        return [row[0] for row in rp.fetchall() if row[2] == 'VCLASS']

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        rp = connection.execute("SHOW CREATE VIEW %s" % view_name)
        return rp.fetchone()[1]

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        columns = []
        rp = connection.execute("SHOW COLUMNS IN %s" % table_name)
        for row in rp.fetchall():
            name = row[0]

            if row[1] is not None:
                format_type = row[1].lower()

                ##strip (5) from character varying(5), timestamp(5)
                attype = re.sub(r'\([\d,]+\)', '', format_type)

                nullable = (row[2] == "YES")
                autoincrement = "auto_increment" in row[5]
                default = row[4]

                is_array = format_type.endswith('[]')
                charlen = re.search('\(([\d,]+)\)', format_type)
                if charlen:
                    charlen = charlen.group(1)
                kwargs = {}
                args = None

                set = format_type.split(' ')
                if set[0] in ('set', 'multiset', 'sequence', 'list'):
                    attype = set[0]
                    args = set[2].split(',')
                elif attype == 'string':
                    if charlen == '1073741823':
                        args = ()
                    else:
                        args = (int(charlen),)
                        attype = 'varchar'
                elif attype == 'numeric':
                    if charlen:
                        prec, scale = charlen.split(',')
                        args = (int(prec), int(scale))
                    else:
                        args = (15, 0)
                elif attype in ('integer', 'short', 'bigint', 'float',
                                    'double', 'monetary', 'object'):
                    args = ()
                elif attype in ('bit varying','varbit'):
                    kwargs['varying'] = True
                    if charlen:
                        args = (int(charlen),)
                    else:
                        args = ()
                elif charlen:
                    args = (int(charlen),)
                else:
                    args = ()

                try:
                    coltype = self.ischema_names[attype]
                    if coltype:
                        coltype = coltype(*args, **kwargs)
                        column_info = dict(name=name, type=coltype,
                                            nullable=nullable, default=default,
                                            autoincrement=autoincrement)
                        columns.append(column_info)
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (attype, name))
                    coltype = sqltypes.NullType()
        return columns

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        pkeys = []
        rp = connection.execute("SHOW COLUMNS IN %s" % table_name)
        for row in rp.fetchall():
            if row[3] == "PRI":
                pkeys.append(row[0])
        return pkeys

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Cannot get referred table or columns using SQL in CUBRID"""
        fkeys = []
        rp = connection.execute("SHOW INDEX IN %s" % table_name)

        prev_name = ""
        fkey_d = None

        for row in rp.fetchall():
            rp = connection.execute(
                    "SELECT * from _db_index WHERE index_name = '%s'" % row[2])
            data = rp.fetchone()
            if data[7] == 1:
                if row[2] != prev_name:
                    if fkey_d is not None:
                        fkeys.append(fkey_d)
                    fkey_d = {
                        'name': row[2],
                        'constrained_columns': [row[4]],
                    }
                    prev_name = row[2]
                else:
                    fkey_d['constrained_columns'].append(row[4])
        if fkey_d is not None:
            fkeys.append(fkey_d)
        return fkeys

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        indexes = []
        rp = connection.execute("SHOW INDEX IN %s" % table_name)
        prev_name = ""
        index_d = None
        for row in rp.fetchall():
            rp = connection.execute(
                    "SELECT * from _db_index WHERE index_name = '%s'" % row[2])
            data = rp.fetchone()

            #exclude indexes on primary keys
            if data[6] == 0:
                if row[2] != prev_name:
                    if index_d is not None:
                        indexes.append(index_d)
                    index_d = {}
                    index_d['name'] = row[2]
                    index_d['column_names'] = [row[4]]
                    index_d['unique'] = (row[1] == 0)
                    index_d['type'] = row[10]
                    prev_name = row[2]
                else:
                    index_d['column_names'].append(row[4])
        if index_d is not None:
            indexes.append(index_d)
        return indexes

    def _describe_table(self, connection, table, full_name=None):
        """Run DESCRIBE for a ``Table`` and return processed rows."""
        if full_name is None:
            full_name = self.identifier_preparer.format_table(table)
        st = "DESCRIBE %s" % full_name

        rp, rows = None, None
        try:
            try:
                rp = connection.execute(st)
            except exc.DBAPIError:
                raise
            rows = rp.fetchall()
        finally:
            if rp:
                rp.close()
        return rows