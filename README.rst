pgsrchcorrupt
=============


>> **Portugues**

`Script Python` para encontrar registros corrompidos no PostgreSQL
------------------------------------------------------------------

O script é multiprocessado, e usa um `loop` para executar "queries" `SQL` na tabela , testando registro a registro.

O registro é selecionado utilizando a tupla `CTID (page, id)` , uma vez que não era possível executar nenhuma "query" utilizando  `limit` ou `offset` na tabela.

outra opção ...

.. tip:: Caso seja possível acesso ao sistema de arquivos, uma opção que talvez seja mais eificienteseria acessar diretamente os cabeçalhos dos arquivos de dados do PostgreSQL encontrados através do `OID` da tabela.

Atenção...

.. warning::
    Este código, tal como está sendo publicado nos commits iniciais,
    não é um bom exemplo de padrões de projetos Python e nem deve ser seguido como
    boas práticas, contudo espero que auxilie de outras maneiras *devs* e *dbas* que
    necessitem  executar queries investigativas em um banco de dados,
    ou ainda tornar um script "tosco" em um script "tosco" multiprocessado. ;)
    ... e tinha que ser em Python 2.7 porque o servidor era velho!

    **O script foi publicado porque é parte de uma palestra na PgConf 2022 (conferência sobre PostgreSQL)**

--------------------------------------------------------------------------------



>> **English**

A `Pyton Script` to Search Corrut records on PostgreSQL
-----------------------------------------------------

The script is a multiproc script which execute queries onside a `loop` (bad pratice), checking each record one by one.

To read the record, the query uses the `CTID` tuple

It uses a *SQL* `select`s inside a **loop**  8o ...

another option ...

.. tip::

    IF you have acces to server file system, you may check corruption on postgre `relations` accessing the data files diretly trought relations OID (Files) checking the files head. ;)


Attention ...

.. warning::
    this code, as it has been publish and initial commits, is not a good
    example to follow as a pattern for Python projects,
    and should not be seen as a good pratice.
    However, I hope it helps any *dev* or *dba*,
    even not having a good knowledge about Postgresql,
    it may help to know how to execute queries on database or
    became a gross Python script into gross multiproc script. ;)

    **The script has been published because it was used on PgConf Brasil 2022 (PostgreSQL Conference)**

--------------------------------------------------------------------------------

