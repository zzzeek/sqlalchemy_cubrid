# coding: utf-8
from test.lib.testing import eq_, assert_raises


from sqlalchemy import *
from sqlalchemy import sql, exc, schema
from sqlalchemy.dialects.cubrid import base as cubrid

from test.lib import *

class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = cubrid.dialect()

    def test_reserved_words(self):
        table = Table("cubrid_table", MetaData(),
            Column("col1", Integer),
            Column("hour_millisecond", Integer))
        x = select([table.c.col1, table.c.hour_millisecond])
        self.assert_compile(x, '''SELECT cubrid_table.col1,'''
                '''cubrid_table.[hour_millisecond] FROM cubrid_table''')

    def test_create_index_simple(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', String(255)))
        idx = Index('test_idx1', tbl.c.data)

        self.assert_compile(schema.CreateIndex(idx),
            'CREATE INDEX test_idx1 ON testtbl ([data])',
            dialect=cubrid.dialect())

    def test_create_index_with_length(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', String(255)))
        idx1 = Index('test_idx1', tbl.c.data, length=10)
        idx2 = Index('test_idx2', tbl.c.data, length=5)

        self.assert_compile(schema.CreateIndex(idx1),
            'CREATE INDEX test_idx1 ON testtbl ([data](10))',
            dialect=cubrid.dialect())
        self.assert_compile(schema.CreateIndex(idx2),
            'CREATE INDEX test_idx2 ON testtbl ([data](5))',
            dialect=cubrid.dialect())

    def test_create_index_with_reverse(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', String(255)))
        idx1 = Index('test_idx1', tbl.c.data, reverse=True)
        idx2 = Index('test_idx2', tbl.c.data, reverse=False)

        self.assert_compile(schema.CreateIndex(idx1),
            'CREATE REVERSE INDEX test_idx1 ON testtbl ([data])',
            dialect=cubrid.dialect())
        self.assert_compile(schema.CreateIndex(idx2),
            'CREATE INDEX test_idx2 ON testtbl ([data])',
            dialect=cubrid.dialect())

    def test_create_index_with_desc(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', String(255)))
        idx1 = Index('test_idx1', tbl.c.data, desc=True)
        idx2 = Index('test_idx2', tbl.c.data, desc=False)

        self.assert_compile(schema.CreateIndex(idx1),
            'CREATE INDEX test_idx1 ON testtbl ([data]) DESC',
            dialect=cubrid.dialect())
        self.assert_compile(schema.CreateIndex(idx2),
            'CREATE INDEX test_idx2 ON testtbl ([data])',
            dialect=cubrid.dialect())

    def test_create_pk_plain(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('pk_data', String(255)),
            PrimaryKeyConstraint('pk_data'))

        self.assert_compile(schema.CreateTable(tbl),
            "CREATE TABLE testtbl (pk_data VARCHAR(255), PRIMARY KEY (pk_data))",
            dialect=cubrid.dialect())

class TypesTest(fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL):
    "Test CUBRID column types"
    __dialect__ = cubrid.dialect()

    def test_basic(self):
        meta1 = MetaData(testing.db)
        table = Table(
            'cubrid_types', meta1,
            Column('id', Integer, primary_key=True),
            Column('text1', cubrid.CHAR),
            Column('text2', cubrid.CHAR(50)),
            Column('text3', cubrid.VARCHAR),
            Column('text4', cubrid.VARCHAR(50)),
            Column('text5', cubrid.NCHAR),
            Column('text6', cubrid.NCHAR(50)),
            Column('text7', cubrid.NVARCHAR),
            Column('text8', cubrid.NVARCHAR(50)),
            Column('num1', cubrid.SMALLINT),
            Column('num2', cubrid.BIGINT),
            Column('num3', cubrid.FLOAT),
            Column('num4', cubrid.FLOAT(25)),
            Column('num5', cubrid.DOUBLE),
            Column('num6', cubrid.MONETARY),
            Column('text9', cubrid.STRING),
            Column('obj1', cubrid.OBJECT),
            Column('obj2', cubrid.BIT(10)),
            Column('obj3', cubrid.BIT(10, varying=True)),
            Column('set1', cubrid.SET(Integer, cubrid.CHAR(1), cubrid.STRING)),
            Column('set2', cubrid.MULTISET(Integer, cubrid.CHAR(1), cubrid.STRING)),
            Column('set3', cubrid.SEQUENCE(Integer, cubrid.CHAR(1), cubrid.STRING)),
            )
        try:
            table.drop(checkfirst=True)
            table.create()
            meta2 = MetaData(testing.db)
            t2 = Table('cubrid_types', meta2, autoload=True)
            assert isinstance(t2.c.num1.type, cubrid.SMALLINT)
            assert isinstance(t2.c.num2.type, cubrid.BIGINT)
            assert isinstance(t2.c.num3.type, cubrid.FLOAT)
            assert isinstance(t2.c.num4.type, cubrid.DOUBLE)
            assert isinstance(t2.c.num5.type, cubrid.DOUBLE)
            assert isinstance(t2.c.num6.type, cubrid.MONETARY)
            assert isinstance(t2.c.text1.type, cubrid.CHAR)
            assert isinstance(t2.c.text2.type, cubrid.CHAR)
            assert isinstance(t2.c.text3.type, cubrid.VARCHAR)
            assert isinstance(t2.c.text4.type, cubrid.VARCHAR)
            assert isinstance(t2.c.text5.type, cubrid.NCHAR)
            assert isinstance(t2.c.text6.type, cubrid.NCHAR)
            assert isinstance(t2.c.text7.type, cubrid.NVARCHAR)
            assert isinstance(t2.c.text8.type, cubrid.NVARCHAR)
            assert isinstance(t2.c.text9.type, cubrid.STRING)
            assert isinstance(t2.c.obj1.type, cubrid.OBJECT)
            assert isinstance(t2.c.obj2.type, cubrid.BIT)
            assert isinstance(t2.c.obj3.type, cubrid.BIT)
            assert isinstance(t2.c.set1.type, cubrid.SET)
            assert isinstance(t2.c.set2.type, cubrid.MULTISET)
            assert isinstance(t2.c.set3.type, cubrid.SEQUENCE)
            t2.drop()
            t2.create()
        finally:
            meta1.drop_all()

    def test_numeric(self):
        columns = [
            (cubrid.NUMERIC, [], {},
             'NUMERIC'),
            (cubrid.NUMERIC, [None], {},
             'NUMERIC'),
            (cubrid.NUMERIC, [12], {},
             'NUMERIC(12)'),
            (cubrid.NUMERIC, [12, 4], {},
             'NUMERIC(12, 4)'),

            (cubrid.DECIMAL, [], {},
             'DECIMAL'),
            (cubrid.DECIMAL, [None], {},
             'DECIMAL'),
            (cubrid.DECIMAL, [12], {},
             'DECIMAL(12)'),
            (cubrid.DECIMAL, [12, None], {},
             'DECIMAL(12)'),
            (cubrid.DECIMAL, [12, 4], {},
             'DECIMAL(12, 4)'),

            (cubrid.FLOAT, [], {},
             'FLOAT'),
            (cubrid.FLOAT, [None], {},
             'FLOAT'),
            (cubrid.FLOAT, [12], {},
             'FLOAT(12)'),
           ]

        table_args = ['test_cubrid_numeric', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw)))

        numeric_table = Table(*table_args)
        gen = testing.db.dialect.ddl_compiler(testing.db.dialect, None)

        for col in numeric_table.c:
            index = int(col.name[1:])
            eq_(gen.get_column_specification(col),
                        "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            numeric_table.create(checkfirst=True)
            assert True
        except:
            raise
        numeric_table.drop()

    def test_charset(self):
        columns = [
            (cubrid.CHAR, [1], {},
             'CHAR(1)'),
             (cubrid.NCHAR, [1], {},
              'NCHAR(1)'),
            (cubrid.CHAR, [1], {'national':True},
             'NCHAR(1)'),
            (cubrid.VARCHAR, [], {},
             'VARCHAR(4096)'),
            (cubrid.VARCHAR, [None], {},
             'VARCHAR(4096)'),
            (cubrid.VARCHAR, [2048], {},
             'VARCHAR(2048)'),
            (cubrid.NVARCHAR, [], {},
             'NCHAR VARYING(4096)'),
            (cubrid.NVARCHAR, [None], {},
             'NCHAR VARYING(4096)'),
            (cubrid.NVARCHAR, [2048], {},
             'NCHAR VARYING(2048)'),
            (cubrid.VARCHAR, [], {'national':True},
             'NCHAR VARYING(4096)'),
            (cubrid.STRING, [1], {},
             'STRING'),
           ]

        table_args = ['test_cubrid_charset', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw)))

        charset_table = Table(*table_args)
        gen = testing.db.dialect.ddl_compiler(testing.db.dialect, None)

        for col in charset_table.c:
            index = int(col.name[1:])
            eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            charset_table.create(checkfirst=True)
            assert True
        except:
            raise
        charset_table.drop()

    @testing.provide_metadata
    def test_bit_50(self):
        metadata = self.metadata
        bit_table = Table('cubrid_bits', meta,
                          Column('b1', cubrid.BIT),
                          Column('b2', cubrid.BIT()),
                          Column('b3', cubrid.BIT(), nullable=False),
                          Column('b4', cubrid.BIT(1)),
                          Column('b5', cubrid.BIT(8)),
                          Column('b6', cubrid.BIT(32)),
                          Column('b7', cubrid.BIT(63)),
                          Column('b8', cubrid.BIT(64)))

        eq_(colspec(bit_table.c.b1), 'b1 BIT(1)')
        eq_(colspec(bit_table.c.b2), 'b2 BIT(1)')
        eq_(colspec(bit_table.c.b3), 'b3 BIT(1) NOT NULL')
        eq_(colspec(bit_table.c.b4), 'b4 BIT(1)')
        eq_(colspec(bit_table.c.b5), 'b5 BIT(8)')
        eq_(colspec(bit_table.c.b6), 'b6 BIT(32)')
        eq_(colspec(bit_table.c.b7), 'b7 BIT(63)')
        eq_(colspec(bit_table.c.b8), 'b8 BIT(64)')

        for col in bit_table.c:
            self.assert_(repr(col))
        meta.create_all()

        meta2 = MetaData(testing.db)
        reflected = Table('cubrid_bits', meta2, autoload=True)

        for table in bit_table, reflected:

            def roundtrip(store, expected=None):
                expected = expected or store
                table.insert(store).execute()
                row = table.select().execute().first()
                try:
                    self.assert_(list(row) == expected)
                except:
                    print "Storing %s" % store
                    print "Expected %s" % expected
                    print "Found %s" % list(row)
                    raise
                table.delete().execute().close()

            roundtrip([0] * 8, ['00', '00', '00', '00', '00', '00000000',
                                '0000000000000000', '0000000000000000'])
            roundtrip([1] * 8, ['00', '00', '00', '00', '10', '10000000',
                                '1000000000000000', '1000000000000000'])
            roundtrip(['1'] * 8, ['00', '00', '00', '00', '10', '10000000',
                                '1000000000000000', '1000000000000000'])

            i = 255
            roundtrip([0, 0, 0, 0, i, i, i, i], ['00', '00', '00', '00', '25',
                        '25500000', '2550000000000000', '2550000000000000'])
            i = 2 ** 32 - 1
            roundtrip([0, 0, 0, 0, 0, i, i, i], ['00', '00', '00', '00', '00',
                        '42949672', '4294967295000000', '4294967295000000'])
            i = 2 ** 63 - 1
            roundtrip([0, 0, 0, 0, 0, 0, i, i], ['00', '00', '00', '00',
                        '00', '00000000', '9223372036854774',
                        '9223372036854775'])
            i = 2 ** 64 - 1
            roundtrip([0, 0, 0, 0, 0, 0, 0, i], ['00', '00', '00', '00',
                        '00', '00000000', '0000000000000000',
                        '1844674407370955'])

    @testing.provide_metadata
    def test_timestamp(self):
        meta = self.metadata
        columns = [
            ([TIMESTAMP], 'TIMESTAMP NULL'),
            ([cubrid.TIMESTAMP], 'TIMESTAMP NULL'),
            ([cubrid.TIMESTAMP, DefaultClause(sql.text('CURRENT_TIMESTAMP'))],
                                    "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            ([cubrid.TIMESTAMP, DefaultClause(sql.text("'1999-09-09 09:09:09'"))],
                                    "TIMESTAMP DEFAULT '1999-09-09 09:09:09'"),
            ]
        for idx, (spec, expected) in enumerate(columns):
            t = Table('cubrid_ts%s' % idx, meta,
                      Column('id', Integer, primary_key=True),
                      Column('t', *spec))
            eq_(colspec(t.c.t), "t %s" % expected)
            self.assert_(repr(t.c.t))
            t.create()
            r = Table('cubrid_ts%s' % idx, MetaData(testing.db),
                      autoload=True)
            if len(spec) > 1:
                self.assert_(r.c.t is not None)

    def test_timestamp_nullable(self):
        meta = MetaData(testing.db)
        ts_table = Table('cubrid_timestamp', meta,
                            Column('t1', TIMESTAMP),
                            Column('t2', TIMESTAMP, nullable=False),
                    )
        meta.create_all()
        try:
            now = testing.db.execute("select CURRENT_TIMESTAMP").scalar()

            ts_table.insert().execute({'t1':now})

            eq_(
                ts_table.select().execute().fetchall(),
                [(now, None)]
            )
        finally:
            meta.drop_all()

    @testing.provide_metadata
    def test_set(self):
        meta = self.metadata
        set_table = Table('cubrid_set', meta,
                            Column('s1', cubrid.SET(cubrid.INTEGER, cubrid.CHAR(1))),
                            Column('s2', cubrid.MULTISET(cubrid.INTEGER, cubrid.CHAR(1))),
                            Column('s3', cubrid.SEQUENCE(cubrid.INTEGER, cubrid.CHAR(1)))
                            )
        eq_(colspec(set_table.c.s1), "s1 SET(INTEGER,CHAR)")
        eq_(colspec(set_table.c.s2), "s2 MULTISET(INTEGER,CHAR)")
        eq_(colspec(set_table.c.s3), "s3 SEQUENCE(INTEGER,CHAR)")
        for col in set_table.c:
            self.assert_(repr(col))
        set_table.create()
        reflected = Table('cubrid_set', MetaData(testing.db),
                          autoload=True)
        for table in set_table, reflected:

            def roundtrip(store, expected=None):
                expected = expected or store
                table.insert(store).execute()
                row = table.select().execute().first()
                try:
                    self.assert_(list(row) == expected)
                except:
                    print 'Storing %s' % store
                    print 'Expected %s' % expected
                    print 'Found %s' % list(row)
                    raise
                table.delete().execute()

            roundtrip([['1', '2', '3', '2', '4', '1'],
                            ['1', '2', '3', '2', '4', '1'],
                            ['1', '2', '3', '2', '4', '1']],
                            [['1', '2', '3', '4'],
                            ['1', '1', '2', '2', '3', '4'],
                            ['1', '2', '3', '2', '4', '1']])
            roundtrip([[], [], []], [[], [], []])

class ReflectionTest(fixtures.TestBase, AssertsExecutionResults):
    def test_default_reflection(self):
        """Test reflection of column defaults."""
        from sqlalchemy.dialects.cubrid import VARCHAR
        def_table = Table(
            'cubrid_def',
            MetaData(testing.db),
            Column('c1', VARCHAR(10), DefaultClause(''), nullable=False),
            Column('c2', VARCHAR(10), DefaultClause('0')),
            Column('c3', VARCHAR(10), DefaultClause('abc')),
            Column('c4', TIMESTAMP, DefaultClause('2009-04-05 12:00:00')),
            Column('c5', TIMESTAMP),
            )
        def_table.create()
        try:
            reflected = Table('cubrid_def', MetaData(testing.db), autoload=True)
        finally:
            def_table.drop()
        assert def_table.c.c1.server_default.arg == ''
        assert def_table.c.c2.server_default.arg == '0'
        assert def_table.c.c3.server_default.arg == 'abc'
        assert def_table.c.c4.server_default.arg == '2009-04-05 12:00:00'
        assert str(reflected.c.c1.server_default.arg) == "''"
        assert str(reflected.c.c2.server_default.arg) == "'0'"
        assert str(reflected.c.c3.server_default.arg) == "'abc'"
        assert str(reflected.c.c4.server_default.arg) == "timestamp '12:00:00 PM 04/05/2009'"
        assert reflected.c.c5.default is None
        assert reflected.c.c5.server_default is None
        reflected.create()
        try:
            reflected2 = Table('cubrid_def', MetaData(testing.db),autoload=True)
        finally:
            reflected.drop()
        assert str(reflected2.c.c1.server_default.arg) == "''"
        assert str(reflected2.c.c2.server_default.arg) == "'0'"
        assert str(reflected2.c.c3.server_default.arg) == "'abc'"
        assert str(reflected2.c.c4.server_default.arg) == "timestamp '12:00:00 PM 04/05/2009'"
        assert reflected.c.c5.default is None
        assert reflected.c.c5.server_default is None

    def test_type_reflection(self):
        specs = [( String(1), cubrid.VARCHAR(1), ),
                 ( String(3), cubrid.VARCHAR(3), ),
                 ( cubrid.CHAR(1), ),
                 ( cubrid.CHAR(3), ),
                 ( cubrid.NCHAR(2), cubrid.NCHAR(2), ),
                 ( cubrid.NVARCHAR(22), cubrid.NVARCHAR(22), ),
                 ( SmallInteger(), cubrid.SMALLINT(), ),
                 ( SmallInteger(), cubrid.SMALLINT(4), ),
                 ( cubrid.SMALLINT(), ),
                 ( cubrid.SMALLINT(4), cubrid.SMALLINT(4), ),
                 ( LargeBinary(), cubrid.BLOB() ),
                 ( cubrid.BLOB(),),
                 ( cubrid.BLOB(1234), cubrid.BLOB()),
                 ]

        columns = [Column('c%i' % (i + 1), t[0]) for i, t in enumerate(specs)]

        db = testing.db
        m = MetaData(db)
        t_table = Table('cubrid_types', m, *columns)
        try:
            m.create_all()

            m2 = MetaData(db)
            rt = Table('cubrid_types', m2, autoload=True)
            try:
                db.execute('CREATE OR REPLACE VIEW cubrid_types_v '
                           'AS SELECT * from cubrid_types')
                rv = Table('cubrid_types_v', m2, autoload=True)

                expected = [len(c) > 1 and c[1] or c[0] for c in specs]

                tables = rt, rv

                for table in tables:
                    for i, reflected in enumerate(table.c):
                        assert isinstance(reflected.type,
                                type(expected[i])), \
                            'element %d: %r not instance of %r' % (i,
                                reflected.type, type(expected[i]))
            finally:
                db.execute('DROP VIEW cubrid_types_v')
        finally:
            m.drop_all()

    def test_system_views(self):
        dialect = testing.db.dialect
        connection = testing.db.connect()
        view_names = dialect.get_view_names(connection)
        self.assert_('db_attr_setdomain_elm' in view_names)
        self.assert_('db_attribute' in view_names)
        self.assert_('db_auth' in view_names)
        self.assert_('db_class' in view_names)
        self.assert_('db_direct_super_class' in view_names)
        self.assert_('db_index' in view_names)
        self.assert_('db_index_key' in view_names)
        self.assert_('db_partition' in view_names)
        self.assert_('db_trig' in view_names)
        self.assert_('db_vclass' in view_names)

class SQLTest(fixtures.TestBase, AssertsCompiledSQL):
    """Tests CUBRID-dialect specific compilation."""

    __dialect__ = cubrid.dialect()

    def test_precolumns(self):
        dialect = self.__dialect__

        def gen(distinct=None, prefixes=None):
            kw = {}
            if distinct is not None:
                kw['distinct'] = distinct
            if prefixes is not None:
                kw['prefixes'] = prefixes
            return str(select(['q'], **kw).compile(dialect=dialect))

        eq_(gen(None), 'SELECT q')
        eq_(gen(True), 'SELECT DISTINCT q')

        assert_raises(
            exc.SADeprecationWarning,
            gen, 'DISTINCT'
        )

        eq_(gen(prefixes=['ALL']), 'SELECT ALL q')
        eq_(gen(prefixes=['DISTINCTROW']),
                'SELECT DISTINCTROW q')

        # Interaction with CUBRID prefix extensions
        eq_(
            gen(None, ['straight_join']),
            'SELECT straight_join q')
        eq_(
            gen(False, ['HIGH_PRIORITY', 'SQL_SMALL_RESULT', 'ALL']),
            'SELECT HIGH_PRIORITY SQL_SMALL_RESULT ALL q')
        eq_(
            gen(True, ['high_priority', sql.text('sql_cache')]),
            'SELECT high_priority sql_cache DISTINCT q')

    def test_limit(self):
        t = sql.table('t', sql.column('col1'), sql.column('col2'))

        self.assert_compile(
            select([t]).limit(10).offset(20),
            "SELECT t.col1, t.col2 FROM t  LIMIT %s, %s",
            {'param_1':10, 'param_2':20}
            )
        self.assert_compile(
            select([t]).limit(10),
            "SELECT t.col1, t.col2 FROM t  LIMIT %s",
            {'param_1':10})

    def test_update_limit(self):
        t = sql.table('t', sql.column('col1'), sql.column('col2'))

        self.assert_compile(
            t.update(values={'col1':123}),
            "UPDATE t SET col1=%s"
            )
        self.assert_compile(
            t.update(values={'col1':123}, cubrid_limit=5),
            "UPDATE t SET col1=%s LIMIT 5"
            )
        self.assert_compile(
            t.update(values={'col1':123}, cubrid_limit=None),
            "UPDATE t SET col1=%s"
            )
        self.assert_compile(
            t.update(t.c.col2==456, values={'col1':123}, cubrid_limit=1),
            "UPDATE t SET col1=%s WHERE t.col2 = %s LIMIT 1"
            )

    def test_utc_timestamp(self):
        self.assert_compile(func.utc_timestamp(), "UTC_TIME()")

    def test_sysdate(self):
        self.assert_compile(func.sysdate(), "SYSDATE")

    def test_cast(self):
        t = sql.table('t', sql.column('col'))
        m = cubrid

        specs = [
            (Integer, "CAST(t.col AS INTEGER)"),
            (INT, "CAST(t.col AS INTEGER)"),
            (m.INTEGER, "CAST(t.col AS INTEGER)"),
            (SmallInteger, "CAST(t.col AS INTEGER)"),
            (m.SMALLINT, "CAST(t.col AS INTEGER)"),
            (m.BIGINT, "CAST(t.col AS INTEGER)"),
            (m.BIT, "t.col"),
            (NUMERIC, "CAST(t.col AS DECIMAL)"),
            (DECIMAL, "CAST(t.col AS DECIMAL)"),
            (Numeric, "CAST(t.col AS DECIMAL)"),
            (m.NUMERIC, "CAST(t.col AS DECIMAL)"),
            (m.DECIMAL, "CAST(t.col AS DECIMAL)"),

            (FLOAT, "t.col"),
            (Float, "t.col"),
            (m.FLOAT, "t.col"),
            (m.DOUBLE, "t.col"),

            (TIMESTAMP, "CAST(t.col AS DATETIME)"),
            (DATETIME, "CAST(t.col AS DATETIME)"),
            (DATE, "CAST(t.col AS DATE)"),
            (TIME, "CAST(t.col AS TIME)"),
            (DateTime, "CAST(t.col AS DATETIME)"),
            (Date, "CAST(t.col AS DATE)"),
            (Time, "CAST(t.col AS TIME)"),
            (DateTime, "CAST(t.col AS DATETIME)"),
            (Date, "CAST(t.col AS DATE)"),
            (m.TIME, "CAST(t.col AS TIME)"),
            (m.TIMESTAMP, "CAST(t.col AS DATETIME)"),
            (String, "CAST(t.col AS CHAR)"),
            (VARCHAR, "CAST(t.col AS CHAR)"),
            (NCHAR, "CAST(t.col AS CHAR)"),
            (CHAR, "CAST(t.col AS CHAR)"),
            (String(32), "CAST(t.col AS CHAR(32))"),
            (CHAR(32), "CAST(t.col AS CHAR(32))"),
            (m.STRING, "CAST(t.col AS CHAR)"),
            (m.NCHAR, "CAST(t.col AS CHAR)"),
            (m.NVARCHAR, "CAST(t.col AS CHAR)"),
            (BLOB, "t.col"),
            (m.BLOB, "t.col"),
            (m.BLOB(32), "t.col"),
            (m.SET, "t.col"),
            (m.SET(m.INTEGER,m.CHAR), "t.col"),
            ]

        for type_, expected in specs:
            self.assert_compile(cast(t.c.col, type_), expected)

    def test_cast_grouped_expression_non_castable(self):
        self.assert_compile(
            cast(sql.column('x') + sql.column('y'), Float),
            "(x + y)"
        )

    def test_extract(self):
        t = sql.table('t', sql.column('col1'))

        for field in 'year', 'month', 'day':
            self.assert_compile(
                select([extract(field, t.c.col1)]),
                "SELECT EXTRACT(%s FROM t.col1) AS anon_1 FROM t" % field)

        self.assert_compile(
            select([extract('milliseconds', t.c.col1)]),
            "SELECT EXTRACT(milliseconds FROM t.col1) AS anon_1 FROM t")

class MatchTest(fixtures.TestBase, AssertsCompiledSQL):
    @classmethod
    def setup_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)

        cattable = Table('cattable', metadata,
            Column('id', Integer, primary_key=True),
            Column('description', String(50))
        )
        matchtable = Table('matchtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('title', String(200)),
            Column('category_id', Integer, ForeignKey('cattable.id'))
        )
        metadata.create_all()

        cattable.insert().execute([
            {'id': 1, 'description': 'Python'},
            {'id': 2, 'description': 'Ruby'},
        ])
        matchtable.insert().execute([
            {'id': 1,
             'title': 'Agile Web Development with Rails',
             'category_id': 2},
            {'id': 2,
             'title': 'Dive Into Python',
             'category_id': 1},
            {'id': 3,
             'title': "Programming Matz's Ruby",
             'category_id': 2},
            {'id': 4,
             'title': 'The Definitive Guide to Django',
             'category_id': 1},
            {'id': 5,
             'title': 'Python in a Nutshell',
             'category_id': 1}
        ])

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_simple_match(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match('python')).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([2, 5], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match("Matz's")).
                   execute().
                   fetchall())
        eq_([3], [r.id for r in results])

    def test_or_match(self):
        results1 = (matchtable.select().
                    where(or_(matchtable.c.title.match('nutshell'),
                              matchtable.c.title.match('ruby'))).
                    order_by(matchtable.c.id).
                    execute().
                    fetchall())
        eq_([3, 5], [r.id for r in results1])

    def test_and_match(self):
        results1 = (matchtable.select().
                    where(and_(matchtable.c.title.match('python'),
                               matchtable.c.title.match('nutshell'))).
                    execute().
                    fetchall())
        eq_([5], [r.id for r in results1])

    def test_match_across_joins(self):
        results = (matchtable.select().
                   where(and_(cattable.c.id==matchtable.c.category_id,
                              or_(cattable.c.description.match('Ruby'),
                                  matchtable.c.title.match('nutshell')))).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([1, 3, 5], [r.id for r in results])


def colspec(c):
    return testing.db.dialect.ddl_compiler(testing.db.dialect, None).get_column_specification(c)

