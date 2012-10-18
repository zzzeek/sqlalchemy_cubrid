# cubrid/cubriddb.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.dialects.cubrid.base import (CUBRIDDialect, CUBRIDExecutionContext,
                                            CUBRIDCompiler, CUBRIDIdentifierPreparer)
from sqlalchemy.connectors.cubriddb import (
                        CUBRIDDBExecutionContext, 
                        CUBRIDDBCompiler, 
                        CUBRIDDBIdentifierPreparer, 
                        CUBRIDDBConnector
                    )

class CUBRIDExecutionContext_cubriddb(CUBRIDDBExecutionContext, CUBRIDExecutionContext):
    pass


class CUBRIDCompiler_cubriddb(CUBRIDDBCompiler, CUBRIDCompiler):
    pass


class CUBRIDIdentifierPreparer_cubriddb(CUBRIDDBIdentifierPreparer, CUBRIDIdentifierPreparer):
    pass

class CUBRIDDialect_cubriddb(CUBRIDDBConnector, CUBRIDDialect):
    execution_ctx_cls = CUBRIDExecutionContext_cubriddb
    statement_compiler = CUBRIDCompiler_cubriddb
    preparer = CUBRIDIdentifierPreparer_cubriddb

dialect = CUBRIDDialect_cubriddb
