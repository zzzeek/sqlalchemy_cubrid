from sqlalchemy.dialects import registry

registry.register("cubrid", "sqlalchemy_cubrid.cubriddb", "CubridDialect_cubriddb")
registry.register("cubrid.cubriddb", "sqlalchemy_cubrid.cubriddb", "CubridDialect_cubriddb")

from sqlalchemy.testing import runner

runner.main()
