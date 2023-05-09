

from datetime import datetime

import mysql.connector
from mysql.connector import cursor
import pytz
from mysql.connector import Error


R = '\033[31m'  # red
G = '\033[32m'  # green
C = '\033[36m'  # cyan
W = '\033[0m'  # white
P = '\033[35m'  #morado
Y = '\033[33m'  #amarrilo

now = datetime.now()
today = now.strftime('%Y-%m-%d')



def conect_to_wpsie_db(credentials):
    """
    Initiates conection with WECPSIE database.

    Parameters
    ----------
    credentials : list, A list that contains the credentials needed to connect to the database.
    Must have: ``[host, database_name, user, password]``

    Returns
    -------
    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    """
    print(C+"[DB_CON] Connecting to {} database...".format(credentials[1]))
    try:
        connection = mysql.connector.connect(host=credentials[0],
                                             database=credentials[1],
                                             user=credentials[2],
                                             password=credentials[3])
        if connection.is_connected():
            print(G+"[DB_CON] Connected to {}!".format(credentials[1]))
            return connection
    except Error as e:
        print(R+"[DB_CON] Error while connecting to {}".format(credentials[1]), e)

# index_level DB methods

def save_index_search_table(search_table_list, connection, mode="insert"):
    """
    Performs the INSERT/UPDATE transaction for the information scraped from: Búsqueda de Procesos de Contratación.

    Parameters
    ----------
    search_table_list : list,
        A list containing the dictionaries that represent the information scrapped from the records in the current page.
        Please refer to the method *get_main_page_table_info* in the extract module for the dictionary structure.

    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    mode : str, default='insert',
        A string that represents the mode this function is going to store the data into database (insert, update)

    Notes
    -----
    Any error, warning, info encountered during the function operations will be logged in WECPSIE database.
    """

    print(C+"[DB_SIST] Saving index result table records on WECPSIE database...")
    mode = mode.upper()

    for pcp_dict in search_table_list:

        base_query = ''
        final_query = ''
        process_id_key = pcp_dict['Código']

        if mode == 'INSERT':

            base_query = """INSERT INTO index_search_result
            (id_proceso, p_link, entidad_contratante,
             objeto_proceso, estado, provincia_canton, presupuesto, fecha_publi, opciones)
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}') """

            final_query = base_query.format(
                pcp_dict['Código'],
                pcp_dict['link'],
                pcp_dict['Entidad Contratante'],
                pcp_dict['Objeto del Proceso'],
                pcp_dict['Estado del Proceso'],
                pcp_dict['Provincia/Cantón'],
                pcp_dict['Presupuesto Referencial Total(sin iva)'],
                pcp_dict['Fecha de Publicación'],
                pcp_dict['Opciones']
            )

        elif mode == 'UPDATE':

            base_query = '''UPDATE index_search_result
                        SET id_proceso = '{}', p_link = '{}', entidad_contratante = '{}',
                        objeto_proceso = '{}', estado = '{}', provincia_canton = '{}', presupuesto = '{}', fecha_publi = '{}', opciones = '{}'
                        WHERE (id_proceso = '{}') and (entidad_contratante = '{}') '''

            final_query = base_query.format(
                pcp_dict['Código'],
                pcp_dict['link'],
                pcp_dict['Entidad Contratante'],
                pcp_dict['Objeto del Proceso'],
                pcp_dict['Estado del Proceso'],
                pcp_dict['Provincia/Cantón'],
                pcp_dict['Presupuesto Referencial Total(sin iva)'],
                pcp_dict['Fecha de Publicación'],
                pcp_dict['Opciones'],
                # where pks
                pcp_dict['Código'],
                pcp_dict['Entidad Contratante']
            )

        try:

            cursor = connection.cursor()
            cursor.execute(final_query)
            connection.commit()
            print(G+"[DB_SIST] |{}| Successfully record {} into index_search_result table!".format(
                process_id_key, mode))
            cursor.close()

            # LOG area
            add_entry_log(connection, process_id_key, pcp_dict['Entidad Contratante'],
                pcp_dict['Fecha de Publicación'], 'info', 'index', mode,today)

        except Error as e:

            entry_message = R+"[DB_SIST] Error while inserting data to index_search_result table:\n  PID: |{}|\n  Error message: {}".format(
                process_id_key, e)
            print(entry_message)

            # LOG area
            add_entry_log(connection, process_id_key, pcp_dict['Entidad Contratante'],
                pcp_dict['Fecha de Publicación'], 'error', 'index', mode, entry_message,today)


def get_triad_pks(table, connection):
    """
    Retrieves a tuple list (id_proceso, entidad_contratante) of the primary
    keys used in the tables index_search_results, and info_sie.

    Parameters
    ----------
    table : str,
        A string representing the table from which the method will extract the pks.

    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    Returns
    -------
    triad_pks : list,
        A list of tuples which that represents the PK combination for every entry in the selected table.
    """

    triad_pk_query = '''SELECT id_proceso, entidad_contratante
                        FROM wecpsie.{}
                        '''.format(table)

    cursor = connection.cursor()
    cursor.execute(triad_pk_query)
    triad_pks = cursor.fetchall()
    print(G+"[DB_TRIPK] Done! Total number of TRIAD PKS retrieved: ", cursor.rowcount)
    return triad_pks

# level1 DB methods

def get_procesos_page_links(connection):
    """
    Retrieves the links and the id for all the stored PCPSIE in database, ordered by latest date.

    Parameters
    ----------
    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    Returns
    -------
    p_links : list, A list of tuples containing the id and the link from the fetched records in database.
    """

    print(P+"[DB_PLINKS] Fetching links of procesos stored at the database")
    select_links_query = """SELECT id_proceso, p_link  FROM index_search_result ORDER BY fecha_publi DESC"""

    cursor = connection.cursor()
    cursor.execute(select_links_query)
    p_links = cursor.fetchall()
    print(G+"[DB_PLINKS] Done! Total number of links retrieved: ", cursor.rowcount)
    return p_links

def save_SIE_info(sie_dict, list_autoridades,list_comision, connection, mode='insert'):
    """
    Performs the INSERT/UPDATE transaction for the information scraped from: Información Proceso Contratación.
    This includes the information under the tables "Descripción del Proceso de Contratación" and "Fechas de Control del Proceso"

    Parameters
    ----------
    sie_dict : dict, A dictionary containing the information scrapped from the tables "Información Proceso Contratación" and "Fechas de Control."
    list_autoridades : list, A list containing the dictionaries with the information of the Autorities of each process
    list_comision : list, A list containing the dictionaries with the information of the Comision Tecnica if the case of each process

    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    Notes
    -----
    The only mandatory keys in sie_dict are ``['Codigo','Entidad Contratante']``.
    The rest of the keys in the dictionary could not exist. It depends on what information is displayed on the page:
    Información Proceso Contratación.
    """


    print(C+"  |--[DB_SIE] |{}| Saving PSIE info in WECPSIE database...".format(
        sie_dict['Codigo']))

    mode = mode.upper()
    cursor = connection.cursor()

    if mode == 'INSERT':
        sie_query = """INSERT INTO info_sie
                    (id_proceso, entidad_contratante, objeto_proceso, compra, presupuesto,
                    funcionario, estado, tipo_Adj, plazo_entrega, f_publicacion,
                    f_inicio_puja, f_adjudicacion, inv_link, cont_link)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s)"""

        autoridad_query = """INSERT INTO autoridades
                          (cedula,nombre,cargo,origen,id_proceso,entidad_contratante)
                          VALUES(%s,%s,%s,%s,%s,%s)"""

        comision_query = """INSERT INTO comision_tecnica
                         (cedula_ct,nombre_ct,funcion_ct,id_proceso,entidad_contratante)
                         VALUES(%s,%s,%s,%s,%s)"""

        try:
            # statements without if condition cant be empty
            cursor.execute(sie_query, (
                sie_dict['Codigo'],
                sie_dict['Entidad'],
                sie_dict['Objeto'],
                sie_dict['Compra'],
                sie_dict['Presupuesto'] if 'Presupuesto' in sie_dict else None,
                sie_dict['Funcionario'] if 'Funcionario' in sie_dict else None,
                sie_dict['Estado'] if 'Estado' in sie_dict else None,
                sie_dict['Tipo'] if 'Tipo' in sie_dict else None,
                sie_dict['Plazo'] if 'Plazo' in sie_dict else None,
                sie_dict['Fecha_publicacion'] if 'Fecha_publicacion' in sie_dict else None,
                sie_dict['Fecha_puja'],
                sie_dict['Fecha_adjudicacion'] if 'Fecha_adjudicacion' in sie_dict else None,
                sie_dict['Invitacion'],
                sie_dict['Contrato']
            ))

            for i in range(len(list_autoridades)):

                dic_autoridad = list_autoridades[i]

                cursor.execute(autoridad_query,(
                    dic_autoridad['Cedula'],
                    dic_autoridad['Nombre'],
                    dic_autoridad['Cargo'],
                    dic_autoridad['Origen'],
                    sie_dict['Codigo'],
                    sie_dict['Entidad']
                ))
            if len(list_comision) > 0:
                for i in range(len(list_comision)):

                    dic_comision = list_comision[i]

                    cursor.execute(comision_query,(

                        dic_comision['Cedula'],
                        dic_comision['Nombre'],
                        dic_comision['Funcion'],
                        sie_dict['Codigo'],
                        sie_dict['Entidad']
                    ))
            else:
                pass

            connection.commit()
            print(G+"  |--[DB_SIE] |{}| PSIE info INSERTED successfully into info_sie table!".format(
                sie_dict['Codigo']))

            # # LOG zone
            add_entry_log(connection, sie_dict['Codigo'], sie_dict['Entidad'],
                sie_dict['Fecha_publicacion'], 'info', 'lvl_1',mode,today)
            # cursor.close()

        except Error as e:
            entry_message = "[DB_SIE] Error while inserting SIE data to info_sie:\n  PID: |{}|\n  Error message: {}".format(
                sie_dict['Codigo'], e)

            print(R+"  |--{}".format(entry_message))

            # LOG area
            add_entry_log(connection, sie_dict['Codigo'], sie_dict['Entidad'],
                sie_dict['Fecha_publicacion'], 'error', 'lvl_1',mode,today, entry_message)

    elif mode == 'UPDATE':
        sie_query = '''UPDATE info_sie
                        SET id_proceso = %s, entidad_contratante = %s, objeto_proceso = %s, compra = %s,
                            presupuesto = %s, funcionario = %s, estado = %s, tipo_adj = %s,
                            plazo_entrega = %s, f_publicacion = %s, f_inicio_puja = %s, f_adjudicacion = %s,
                            inv_link = %s, cont_link = %s
                        WHERE (id_proceso = %s) and (entidad_contratante = %s)'''

        autoridad_query = '''UPDATE autoridades
                                SET cedula = %s, nombre = %s, cargo = %s, origen = %s,
                                id_proceso = %s,entidad_contratante = %s
                            WHERE (id_proceso = %s) and (entidad_contratante = %s) and (cedula = %s)'''

        comision_query = '''UPDATE comision_tecnica
                                SET cedula_ct = %s, nombre_ct = %s, funcion_ct = %s,
                                    id_proceso = %s, entidad_contratante = %s
                                WHERE(id_proceso = %s) and (entidad_contratante = %s) and (cedula_ct = %s)'''


        try:
            # statements without if condition cant be empty
            cursor.execute(sie_query, (
                sie_dict['Codigo'],
                sie_dict['Entidad'],
                sie_dict['Objeto'],
                sie_dict['Compra'],
                sie_dict['Presupuesto'] if 'Presupuesto' in sie_dict else None,
                sie_dict['Funcionario'] if 'Funcionario' in sie_dict else None,
                sie_dict['Estado'] if 'Estado' in sie_dict else None,
                sie_dict['Tipo'] if 'Tipo' in sie_dict else None,
                sie_dict['Plazo'] if 'Plazo' in sie_dict else None,
                sie_dict['Fecha_publicacion'] if 'Fecha_publicacion' in sie_dict else None,
                sie_dict['Fecha_puja'],
                sie_dict['Fecha_adjudicacion'] if 'Fecha_adjudicacion' in sie_dict else None,
                sie_dict['Invitacion'],
                sie_dict['Contrato'],
                # where clause pks
                sie_dict['Codigo'],
                sie_dict['Entidad'],

            ))

            for i in range(len(list_autoridades)):

                dic_autoridad = list_autoridades[i]

                cursor.execute(autoridad_query,(
                    dic_autoridad['Cedula'],
                    dic_autoridad['Nombre'],
                    dic_autoridad['Cargo'],
                    dic_autoridad['Origen'],
                    sie_dict['Codigo'],
                    sie_dict['Entidad'],
                    # where clause pks
                    sie_dict['Codigo'],
                    sie_dict['Entidad'],
                    dic_autoridad['Cedula']
                ))
            if len(list_comision) > 0:
                for i in range(len(list_comision)):

                    dic_comision = list_comision[i]

                    cursor.execute(comision_query,(

                        dic_comision['Cedula'],
                        dic_comision['Nombre'],
                        dic_comision['Funcion'],
                        sie_dict['Codigo'],
                        sie_dict['Entidad'],
                        # where clause pks
                        sie_dict['Codigo'],
                        sie_dict['Entidad'],
                        dic_comision['Cedula']
                    ))
            else:
                pass

            connection.commit()
            print(G+"  |--[DB_SIE] |{}| PSIE info UPDATED successfully into info_sie table!".format(
                sie_dict['Codigo']))

            # # LOG zone
            add_entry_log(connection, sie_dict['Codigo'], sie_dict['Entidad'],
                sie_dict['Fecha_publicacion'], 'info', 'lvl_1',mode,today)
            # cursor.close()

        except Error as e:
            entry_message = "[DB_SIE] Error while UPDATING PSIE data to info_sie:\n  PID: |{}|\n  Error message: {}".format(
                sie_dict['Codigo'], e)

            print(R+"  |--{}".format(entry_message))

            # # LOG area
            add_entry_log(connection, sie_dict['Codigo'], sie_dict['Entidad'],
                sie_dict['Fecha_publicacion'], 'error', 'lvl_1',mode, today,entry_message)

def get_contrato_links(connection):
    """
    Retrieves the links of contract if exist and the id for all the stored PCPSIE  in database, ordered by latest date.

    Parameters
    ----------
    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    Returns
    -------
    p_links : list, A list of tuples containing the id and the link from the fetched records in database.
    """

    print(P+"[DB_PLINKS] Fetching links of contrato stored at the database")
    select_links_query = """SELECT id_proceso, entidad_contratante, cont_link  FROM info_sie"""

    cursor = connection.cursor()
    cursor.execute(select_links_query)
    p_links = cursor.fetchall()

    counter = 0

    for c_index, contrato_link in enumerate(p_links):
        if contrato_link[2] != '':
            counter+=1

    print(G+"[DB_PLINKS] Done! Total number of links retrieved: ", counter)
    return p_links

def get_invitacion_links(connection):
    """
    Retrieves the links of invitation page if exist and the id for all the stored PCPSIE  in database, ordered by latest date.

    Parameters
    ----------
    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    Returns
    -------
    p_links : list, A list of tuples containing the id and the link from the fetched records in database.

    Notes
    -----
    All processes have a link to the invitations page but in some cases there are no companies invited to the process.
    """

    print(P+"[DB_PLINKS] Fetching links of invitation page stored at the database")
    select_links_query = """SELECT id_proceso, entidad_contratante, inv_link  FROM info_sie"""

    cursor = connection.cursor()
    cursor.execute(select_links_query)
    p_links = cursor.fetchall()
    print(G+"[DB_PLINKS] Done! Total number of links retrieved: ", cursor.rowcount)
    return p_links


def save_contract_SIE_info(proceso_id, entidad_contratante,contrato_dic, connection, mode='insert'):
    """
    Performs the INSERT/UPDATE transaction for the information scraped from: Resumen Información Esencial del Contrato.
    This includes the information under the tables "Información de Adjudicación" and "Información del Contrato"

    Parameters
    ----------
    proceso_id : str, The identification code of the process
    entidad_contratante : str, The entity of the process
    contrato_dic : dict, A dictionary containing the information scrapped from the tables "Información de Adjudicación" and "Información del Contrato"

    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    Notes
    -----


    """


    print(C+"  |--[DB_SIE] |{}| Saving contract PSIE info in WECPSIE database...".format(proceso_id))
    mode = mode.upper()
    cursor = connection.cursor()

    if mode == 'INSERT':
        sie_query = """INSERT INTO info_contrato
                    (ruc, nombre_adj, fecha_adj, monto, admin_contrato,
                    id_proceso, entidad_contratante)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)"""

        try:
            # statements without if condition cant be empty
            cursor.execute(sie_query, (
                contrato_dic['Ruc'],
                contrato_dic['Nombre'],
                contrato_dic['Fecha_adj'],
                contrato_dic['Monto'],
                contrato_dic['Admin'],
                proceso_id,
                entidad_contratante
            ))

            connection.commit()
            print(G+"  |--[DB_SIE] |{}| contract PSIE info INSERTED successfully into info_contrato table!".format(proceso_id))

            # # LOG zone
            add_entry_log(connection, proceso_id, entidad_contratante,'', 'info', 'lvl_2',mode,today)
            # cursor.close()

        except Error as e:
            entry_message = "[DB_SIE] Error while inserting contract SIE data to info_contrato:\n  PID: |{}|\n  Error message: {}".format(proceso_id, e)

            print(R+"  |--{}".format(entry_message))

            # LOG area
            add_entry_log(connection, proceso_id, entidad_contratante,'' ,'error', 'lvl_2', mode, today)

    elif mode == 'UPDATE':
        sie_query = '''UPDATE info_contrato
                           SET ruc = %s, nombre_adj = %s, fecha_adj = %s, monto = %s,
                               admin_contrato = %s, id_proceso = %s, entidad_contratante = %s
                           WHERE (id_proceso = %s) and (entidad_contratante = %s)'''
        try:
            # statements without if condition cant be empty
            cursor.execute(sie_query, (
                contrato_dic['Ruc'],
                contrato_dic['Nombre'],
                contrato_dic['Fecha_adj'],
                contrato_dic['Monto'],
                contrato_dic['Admin'],
                proceso_id,
                entidad_contratante,
                # where clause pks
                proceso_id,
                entidad_contratante
            ))

            connection.commit()
            print(G + "  |--[DB_SIE] |{}| contract SIE info UPDATED successfully into info_contrato table!".format(
                proceso_id))
            add_entry_log(connection, proceso_id, entidad_contratante, '', 'error', 'lvl_2', mode, today)

        except Error as e:
            entry_message = "[DB_SIE] Error while UPDATING contract SIE data to info_contrato:\n  PID: |{}|\n  Error message: {}".format(proceso_id, e)

            print(R+"  |--{}".format(entry_message))

            # LOG area
            add_entry_log(connection, proceso_id, entidad_contratante, '','error', 'lvl_2',mode,today,entry_message)


def save_invitation_SIE_info(proceso_id, entidad_contratante, invitacion_list, connection, mode='insert'):
    """
    Performs the INSERT/UPDATE transaction for the information scraped from: Invitacion a proveedores.


    Parameters
    ----------
    proceso_id : str, The identification code of the process
    entidad_contratante : str, The entity of the process
    invitacion_list : list, A list containing various dictionarys each one with the information scrapped from the table
     "Invitacion a proveedores"

    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    Notes
    -----


    """

    print(C+"  |--[DB_SIE] |{}| Saving invitation SIE info in WECPSIE database...".format(proceso_id))
    mode = mode.upper()
    cursor = connection.cursor()

    for invi in range(len(invitacion_list)):

        if mode == 'INSERT':
            sie_query = """INSERT INTO invitaciones
                        (razon_social, fecha_inv, prov_cant, estado,
                        id_proceso, entidad_contratante)
                        VALUES (%s,%s,%s,%s,%s,%s)"""

            try:
                # statements without if condition cant be empty
                cursor.execute(sie_query, (
                    invitacion_list[invi].get('Razón Social - Proveedor'),
                    invitacion_list[invi].get('Fecha de Invitación'),
                    invitacion_list[invi].get('Provincia - Cantón'),
                    invitacion_list[invi].get('Estado actual RUP'),
                    proceso_id,
                    entidad_contratante
                ))

                connection.commit()
                print(G+"  |--[DB_SIE] |{}| invitation SIE info INSERTED successfully into invitaciones table!".format(proceso_id))

                # # LOG zone
                add_entry_log(connection,proceso_id, entidad_contratante,'','info', 'lvl_2',mode,today)
                # cursor.close()

            except Error as e:
                entry_message = "[DB_SIE] Error while inserting invitation SIE data to invitaciones:\n  PID: |{}|\n  Error message: {}".format(proceso_id, e)

                print(R+"  |--{}".format(entry_message))

                # LOG area
                add_entry_log(connection,proceso_id, entidad_contratante,'','error', 'lvl_2',mode,today,entry_message)

        elif mode == 'UPDATE':
            sie_query = '''UPDATE invitaciones
                               SET razon_social = %s, fecha_inv = %s, prov_cant = %s, estado = %s,
                                   id_proceso = %s, entidad_contratante = %s
                               WHERE (id_proceso = %s) and (entidad_contratante = %s) and (razon_social = %s)'''
            try:
                # statements without if condition cant be empty
                cursor.execute(sie_query, (
                    invitacion_list[invi].get('Razón Social - Proveedor'),
                    invitacion_list[invi].get('Fecha de Invitación'),
                    invitacion_list[invi].get('Provincia - Cantón'),
                    invitacion_list[invi].get('Estado actual RUP'),
                    proceso_id,
                    entidad_contratante,
                    # where clause pks
                    proceso_id,
                    entidad_contratante,
                    invitacion_list[invi].get('Razón Social - Proveedor')

                ))

                connection.commit()
                print(G + "  |--[DB_SIE] |{}| invitation list of SIE info UPDATED successfully into invitaciones table!".format(
                    proceso_id))


            except Error as e:
                entry_message = "[DB_SIE] Error while UPDATING invitaciones SIE data to invitaciones:\n  PID: |{}|\n  Error message: {}".format(proceso_id, e)

                print(R+"  |--{}".format(entry_message))

                # LOG area
                add_entry_log(connection, proceso_id, entidad_contratante,'', 'error', 'lvl_2', mode, today,entry_message)

# other methods

def add_entry_log(connection, id_proceso, entidad_contratante, f_publicacion, entry_type, level, action,entry_date, entry_message=None):
    """
    Inserts ``LOG``entries related to any error, warning, info occurred during the execution of the
    modules in this project.

    Parameters
    ----------
    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.

    id_proceso : str,
        A string representing the id of a PSIE.

    entidad_contratante : str,
        A string representing the entidad contratante of a PSIE.

    f_publicacion : str,
        A string representing the date when a PSIE was published some cases is empty.

    entry_type : str, A string that describes the type of log entry: error, warning, info

    level : str, A string that describes the level where this log_entry was called.

    action : str, A string that describes the action that fired the log_entry, for example: navigation, extraction, update, store

    entry_message : str, default="None"
        A string that represents the message thrown by the system about the error or warning encountered.

    Notes
    -----
    This function also stores the ``DATETIME`` of the log entry in the database.
    """

    if 'lvl_0' in level:
        indent = '  |--'
    elif 'lvl_1' in level:
        indent = '  |--='
    elif 'lvl_2' in level:
        indent = '  |--<<'
    else:
        indent = ''

    print(G+"{}[DB_LOG] |{}| Adding log entry...".format(indent, id_proceso))

    entry_query = """INSERT INTO entry_log 
                (id_proceso, entidad_contratante, fecha_publicacion, entry_type, 
                entry_level, entry_action, entry_date, entry_message) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""


    try:
        cursor = connection.cursor()
        cursor.execute(entry_query, (
            id_proceso,
            entidad_contratante,
            f_publicacion,
            entry_type,
            level,
            action,
            entry_date,
            entry_message
        ))
        connection.commit()
        cursor.close()

        print(G+"{}[DB_LOG] |{}| Log entry added successfully into entry_log table!".format(indent, id_proceso))

    except Error as e:
        print(R+"{}[DB_LOG] Error while inserting entrylog to entry_log table:\n PID: |{}|".format(indent, id_proceso), e)


def disconnect_db(connection):
    """
    Disconects from WECPSIE database.

    Parameters
    ----------
    connection : CMySQLConnection | MySQLConnection, Connection object to perform further db transactions.
    """
    if connection.is_connected():
        connection.close()
        print(G+"[DB_OFF] WECPSIE DB connection closed")

    pass


# main program, testing only
if __name__ == "__main__":

    db_connection = conect_to_wpsie_db(['localhost', 'wecpsie', 'user', 'passs'])
    add_entry_log(db_connection,2,'sebas','2020-07-12','info','lv_0','insert','2021-05-16','hola')
    # db_connection = conect_to_wpsie_db(['localhost', 'wecpsie', 'user', 'passs'])
    # list = []
    # # list2 = []
    # test = {'Razón Social - Proveedor': 'ZAMBRANO SERRANO EMILIO JOSE', 'Fecha de Invitación': '2021-04-05 20:02', 'Provincia - Cantón': 'PICHINCHA - QUITO', 'Estado actual RUP': 'Habilitado en RUP'}
    # test2 = {'Razón Social - Proveedor': 'ZAMBRANO ZAMBRANO HOMERO GUILLERMO', 'Fecha de Invitación': '2021-04-05 20:02', 'Provincia - Cantón': 'ESMERALDAS - ESMERALDAS', 'Estado actual RUP': 'Habilitado en RUP'}
    # list.append(test)
    # list.append(test2)
    # save_invitation_SIE_info('SIE-002-2020-DD11D07','DIRECCION DISTRITAL 11D07-MACARA-SOZORANGA-EDUCACION',list,db_connection,mode='update')
    #
    #
    # # get_contrataciones_table_page_links(db_connection)
    #
    # # triads = get_rc_aas_pks(db_connection)
    # # print(triads, len(triads))
    # # print(type(triads[0]))
    # # print(len(triads))