import argparse
import datetime
import logging
import multiprocessing
import os
import psycopg2
import sys
import time

from distutils.util import strtobool

# getattr(logging, loglevel.upper())

DESCRIPTION = u"""
(en) Identify corrupted records on a postgresql relation (table) by reading one by one.
throught CTID - Offset & limit are prohibited!

It checks *pg_class* table to get estimate number of pages & records.
----

"""


def get_commandline_parser():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('database', help='database name')
    parser.add_argument('schema', help='database schema')
    parser.add_argument('table', help='table name')
    parser.add_argument('--cpus', default=2, type=int, help='number of cpus')
    parser.add_argument('--blocks', default=0, type=int, help='number of blocks')
    parser.add_argument('--host', help='hostname or ip')
    parser.add_argument('--port', help='postgresql port')
    parser.add_argument('--user', help='postgresql user name')
    parser.add_argument('--password', help='postgresql user name')
    parser.add_argument(
        '--islogtofile',
        default=False,
        type=strtobool,
        help=u'gravar arquivo de log - default (default: %(default)s) - se não, log baseado no sistema'
    )
    parser.add_argument(
        '--loglevel',
        default='error',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        help=u'define nível de log  '
    )
    return parser


def set_logconf(loglevel=None, islogtofile=None):
    '''
    set log configs

    args:
        loglevel (None, optional): nivel de log ()
        islogtofile (None, optional): se False then log do sistema oeracional.

    Returns:
        TYPE: string  nome do arquivo de log gerado
    '''

    # define logfile name if islogfile
    log_file_name = islogtofile  # in ase its False
    if islogtofile:
        date_now = datetime.datetime.now()
        log_file_name = '%s.log' % (date_now.strftime('%Y%m%d_%H%M_pgsearchcorrupt'))

    logging.basicConfig(
        filename=log_file_name,
        level=loglevel or logging.DEBUG,
        format='%(asctime)s|%(name)s|%(levelname)s|%(message)s', datefmt='%Y%m%d %H:%M:%S'
    )
    return log_file_name


def message_info(msg, flush=False):
    msg = '\n\t%s\n' % (msg)
    sys.stdout.write(msg)
    if flush:
        sys.stdout.flush()


def message_error(msg, flush=False):
    msg = '\n\t%s\n' % (msg)
    sys.stderr.write(msg)
    if flush:
        sys.stderr.flush()


def get_connection(dbname, host='127.0.0.1', port=5432, user=None, password=None):
    try:
        conn = psycopg2.connect(
            dbname=dbname, host=host, port=port,
            user=user, password=password,
        )
    except Exception as e:
        logging.critical('erro na conexão')
        raise e
    return conn


def get_table_statistics(schema, table, conn):
    '''
    Query pg_class for table information Blocks/Pages and estimate record number.

    Returns:
        tuple   (p, r)  where `p` is Page/Blocks, r is Ettimate number of total records
    '''
    cur = conn.cursor()
    query = '''SELECT relpages::bigint as pages, reltuples::bigint AS estimate
        FROM   pg_class
        WHERE  oid = '%s.%s'::regclass;''' % (schema, table)
    try:
        cur.execute(query)
        res = cur.fetchall()

    except psycopg2.errors.UndefinedTable:
        logging.error('relation (tabela) inexistente %(schema)s.%(table)s' % (
            {'schema': schema, 'table': table}))
        raise psycopg2.errors.UndefinedTable(
            'Provavelmente a tabela %(schema)s.%(table)s não existe -' % (
                {'schema': schema, 'table': table}
            )
        )
    finally:
        cur.close()

    if res:
        return res
    else:
        return (0, 0)


def run_query(conn, query):
    '''
    conexão não pode ficar aqu porque será uma conexão por query - Nao eficiente
    '''
    cur = conn.cursor()

    cur.execute(query)

    # con.commit()
    return cur.fetchall()


def run_atomic_query(conn, query):
    '''

    '''
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    # cur.close()
    return 1


def get_records_per_worker(cpus=2, blocks=100):
    """calculate total records distributed per cpus equaly

    essa rotina define a estrategia de busca na tabela.

    Já que, ao não usar `offset` e `limit`, e sim CTID tuple (0,1)
    não temos uma condição de parada. É necessário definir um limite do par
    (block, page) para parar a busca.

    temos:
        total de pages
        Estimativa do total de registros
        O total de blocos será distribuído igualmente entre os processadores
        [(0, 250), (250, 500), (500, 750), (750, 1000)]

    Args:
        cpus (int, optional): Description
        recs (int, optional): Description

    returns:
        tuple containing start, stop offsets

    ex.:
        4 cpus and 1000 records
        [(0, 250), (250, 500), (500, 750), (750, 1000)]

    ranges = [(n, min(n + step, stop)) for n in xrange(start, stop, step)]
    """
    start = 0
    stop = blocks

    step = blocks // (cpus**2)

    if step < 1:
        step = 1

    pages_small_ranges = [(n, min(n + step, stop)) for n in range(start, stop, step)]

    step = len(pages_small_ranges) // cpus  # num of ranges per cpu
    if step < 1:
        step = 1
    start = 0
    stop = len(pages_small_ranges)
    # agrupar os ranges para 4 processadores

    ranges_indexes = [(n, min(n + step, stop)) for n in range(start, stop, step)]

    borrow = None
    if len(ranges_indexes) > cpus:
        borrow = ranges_indexes.pop()

    processes_ranges_queue = [list() for i in range(cpus)]
    for t in ranges_indexes:
        # print 't=', t
        index = 0
        for i in range(*t):
            # print 'i=', i
            processes_ranges_queue[index].append(pages_small_ranges[i])
            index += 1
    if borrow:
        pass

    return processes_ranges_queue


def get_ctid_query(table='', block=0, record=1):
    ctid_tuple = '(%s, %s)' % (block, record)
    query = 'SELECT ctid,* FROM %s WHERE ctid = \'%s\';' % (table, ctid_tuple)
    return query


def get_insert_query(table, rec_id):
    query = 'INSERT INTO newtable_safedata SELECT * FROM %s WHERE id = %s;' % (table, rec_id)  # todo: parameter relation name
    return query


def check_records(conn, table, block, records):
    '''executa selects em todas as linhas de um unico bloco.

    Args:
        block (int): bloco
        records (int): numero estimado de regitros no bloco

    '''
    process_name = multiprocessing.current_process().name
    ctid_exists = True  # if CTID tuple exists (select down)
    number_ctid_exists = 0  # qtt of CTIDs read
    record_index = 0  # record count records
    zero_recs_count = 0  # number of queries return 0 recs
    errors = 0  # number of erros (select fail)
    inserts = 0
    # calcula 10% dos registros...
    # se 10% for menor que 0 (zero) minimo de 10
    # O valor é usado para ler N registros a mais após a primeira query CTID que
    # retornar 0 (nada)
    estimate_recs_perc = ((10 * records) / 100.0)  # 10 % da qtd de registros esperado
    if estimate_recs_perc < 10:
        estimate_recs_perc = 10

    while True:
        record_index += 1  # incrementa contador. Records starts in 1 (blocks in 0)

        try:
            qresult = run_query(conn, get_ctid_query(table, block, record_index))
        except Exception as e:
            qresult = None
            errors += 1
            conn.rollback()
            # raise e  # todo: not raise , log it !!
            try:
                # esta query
                query = 'SELECT ctid, id FROM %(table)s WHERE CTID=\'%(ctid_tuple)s\'' % (
                    {'table': table, 'ctid_tuple': '(%s, %s)' % (block, record_index)})
                qresult = run_query(conn, query)
                # logging.debug(f'Query: {query}')
                # logging.error(f' ERROR Query: {query}')
                # logging.warning('%(process_name)s|  Bl:%(block)s Re:%(record)s ID:%(record_id)s ErrorMessage:%(erros_msg)s ' % (
                #     {'process_name': process_name, 'block': block, 'record': record_index, 'record_id': qresult[0][1], 'erros_msg': e})
                # )
            except Exception as e:
                conn.rollback()
                logging.error('%(process_name)s| CORRUPTED Fail get ID Bl:%(block)s Re:%(record)s ErrorMessage %(erros_msg)s' % (
                    {'process_name': process_name, 'block': block, 'record': record_index, 'erros_msg': e}
                ))
                qresult = None

        # informativo para cada linha
        logging.debug('%s|Bl:%s Re:%s D: %s' % (process_name, block, record_index, bool(qresult)))

        if bool(qresult):
            try:
                i_id = qresult[0][1]  # id
            except Exception as e:
                logging.error('%s| Erro no id %s' % (process_name, qresult))  # loga resul #todo: Precisa melhorar isso aqui
                i_id = None

            # if i_id:
            #     try:
            #         run_atomic_query(conn, get_insert_query(table, i_id))  # INSERT INTO
            #         inserts += 1
            #     except Exception as e:
            #         inserts -= 1
            #         conn.rollback()
            #         logging.error('%s| INSERT FAIL Bl:%s Re:%s id:%s ErrorMessage :%s' % (process_name, block, record_index, i_id, e))
            #         # raise e  # todo: not raise , log it !!

    # 7353533 20201104 09:20:05|root|ERROR|CORRUPTED Bl:12070712 Re:36 D: True  ErrorMessage missing chunk number 8 for to        ast value 51071556 in pg_toast_24735
    # 7353534
    # ...
    # 7353541 20201104 09:20:05|root|ERROR|INSERT FAIL Bl:12070712 Re:36 id:39074282 ErrorMessage :current transaction is         aborted, commands ignored until end of transaction block
    # 7353542

        ctid_exists = bool(qresult)  # se lista vazia, ctid_exists = False
        if ctid_exists:
            number_ctid_exists += 1
        else:
            zero_recs_count += 1

        # STOP conditions
        # STOP se o numero de registros NAO encontrados for > 10% do estimado
        if zero_recs_count > estimate_recs_perc:
            break

    logging.info('%s|Blk:%s Recs:%s: Q0:%s CTIDs:%s Erros:%s Insert:%s' % (
        process_name, block, record_index - 1, zero_recs_count, number_ctid_exists, errors, inserts))
    return record_index - 1


def check_blocks(table, ranges, records, database, host, port, user, password):
    conn = get_connection(database, host=host, port=port, user=user, password=password)
    # [ ...
    # [(0, 1147405L), (4589620, 5737025L), (9179240, 10326645L), (13768860, 14916265L)],
    # ... ]

    ranges.reverse()  # para iniciar do ultimo bloco.
    for rang in ranges:
        for block in range(*rang):
            try:
                check_records(conn, table, block, records)
            except Exception as e:
                logging.error('%s|ERROR check_blocks - %s' % (
                    multiprocessing.current_process().name, e)
                )
    conn.close()


if __name__ == "__main__":

    # todo:  import multiprocessing as mp
    # usar o nume de cpus automatico print("Number of processors: ", mp.cpu_count())

    # ARGs definitions
    parser = get_commandline_parser()
    args = parser.parse_args()

    # LOG configs
    log_file_name = set_logconf(args.loglevel.upper(), args.islogtofile)  # set loglevel
    # set header for logs  ...  if in a file its pretty imho !
    logging.debug('Debug Mode ON')
    logging.debug('ARGS passed:%s' % args)

    # get DB connection
    conn = get_connection(args.database, host='127.0.0.1', port=args.port, user=None, password=None)
    table_stats = get_table_statistics(args.schema, args.table, conn)
    conn.close()

    # print log file name
    sys.stdout.write('schema..................: %s\n' % args.schema)
    sys.stdout.write('database................: %s\n' % args.database)
    sys.stdout.write('table...................: %s\n' % args.table)

    sys.stdout.write(f'Blocks/Pages ...........: {table_stats[0][0]}\n' )
    sys.stdout.write('Postgre Estimate Records: %s\n' % table_stats[0][1])
    sys.stdout.write('Estimate Records/Pages..: %s\n' % (table_stats[0][1] / table_stats[0][0]))

    logging.info('INICIO ..................: %s' % (datetime.datetime.now().strftime('%Y%m%d %H:%M')))
    logging.info('schema: %s' % args.schema)
    logging.info('database: %s' % args.database)
    logging.info('table: %s' % args.table)
    logging.info('Blocks/Pages: %s' % table_stats[0][0])
    logging.info('Postgre Estimate Records: %s' % table_stats[0][1])
    logging.info('Estimate Records/Pages: %s' % (table_stats[0][1] / table_stats[0][0]))

    # if blocks not informed it is taken from pg_class
    if not args.blocks:
        args.blocks = table_stats[0][0]

    ranges_list_per_cpu = get_records_per_worker(cpus=args.cpus, blocks=args.blocks)

    logging.info(f'ranges: {ranges_list_per_cpu}')

    for proc_num, proc_range in enumerate(ranges_list_per_cpu):

        service = multiprocessing.Process(
            name='CB%s'%(proc_num),
            target=check_blocks,
            args=(args.table, proc_range, 7, args.database, args.host, args.port, args.user, args.password)
        )
        service.start()

    # at the end
    log_file_name = 'log File ............... : %s \n' % (log_file_name)
    sys.stdout.write(log_file_name)
    sys.stdout.flush()
