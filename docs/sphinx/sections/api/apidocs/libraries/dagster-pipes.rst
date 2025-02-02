Pipes (dagster-pipes)
-----------------------

This library is intended for inclusion in an external process that integrates
with Dagster using teh Pipes protocol.

.. currentmodule:: dagster_pipes

.. autofunction:: init_dagster_pipes

.. autoclass:: PipesContext

.. autoclass:: DagsterPipesError

.. autoclass:: DagsterPipesWarning

.. autofunction:: encode_env_var

.. autofunction:: decode_env_var

.. autofunction:: is_dagster_pipes_process

.. autoclass:: PipesContextLoader

.. autoclass:: PipesMessageWriter

.. autoclass:: PipesMessageWriterChannel

.. autoclass:: PipesParamsLoader

.. autoclass:: PipesBlobStoreMessageWriter

.. autoclass:: PipesBlobStoreMessageWriterChannel

.. autoclass:: PipesBufferedFilesystemMessageWriterChannel

.. autoclass:: PipesDefaultContextLoader

.. autoclass:: PipesDefaultMessageWriter

.. autoclass:: PipesFileMessageWriterChannel

.. autoclass:: PipesStreamMessageWriterChannel

.. autoclass:: PipesEnvVarParamsLoader

.. autoclass:: PipesS3MessageWriter

.. autoclass:: PipesS3MessageWriterChannel

.. autoclass:: PipesDbfsContextLoader

.. autoclass:: PipesDbfsMessageWriter
