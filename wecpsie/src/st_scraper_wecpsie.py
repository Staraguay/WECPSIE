import concurrent.futures
import re

import time
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.select import Select
import d_utilities as dutils
import st_wpse_db_connection as wpsedb


# begin information retrieval methods

R = '\033[31m'  # red
G = '\033[32m'  # green
C = '\033[36m'  # cyan
W = '\033[0m'  # white
P = '\033[35m'  #morado
Y = '\033[33m'  #amarrilo


now = datetime.now()
today = now.strftime('%Y-%m-%d')

def get_main_page_table_info(dcrawler, n_retrieve=20):
    """
    Information scraper for search result index table in: Búsqueda de Procesos de Contratación.
    This function is classified as Level_Index.

    Parameters
    ----------
    dcrawler : webdriver instance to perform crawling operations.

    n_retrieve : int, default="20"
        The number of rows to retrieve from the current page search table. The update module
        passes this argument in order to retrieve a certain number of records (<20) in the table.
        The spider module uses the default argument since its goal is to retrieve all the records.

    Returns
    -------
    swing_table_process_list : list,
        A list containing the information scrapped from the n_retrieve records in the current page.
        Every element in the list is a dictionary that represents a PCPSIE indexed.
        The dictionary has the following example structure:

            {'Código': 'GGE-CJU-2020-020'
             'Link': 'https://link-to-PSIE',
             'Entidad Contratante': 'Entidad1',
             'Objeto del proceso': 'Desc1',
             'Estado del proceso': 'Est1',
             'Provincia/Cantón': 'Guayas/Guayaquil',
             'Presupuesto Referencial': '$66,340.00',
             'Fecha de Publicación': '2020-10-10 13:00:00',
             'Opciones': 'Opc1'}

    current_last_element : int,
        The last element scrapped from the search result table. This number is found from the
        TABLE "Procesos del 1 al 20 de 870" at the bottom of the page.

    last_element : int,
        The last element (PCPSIE) from the search result table. This number is found from the
        TABLE "Procesos del 1 al 20 de 870" at the bottom of the page.

    Notes
    -----
    Any error, warning, info encountered during the function operations will be logged in WECPSIE database.

    Examples
    --------
    >>> from selenium import webdriver
    >>> dcrawler = webdriver.Chrome(Driver_PATH)
    >>> search_table_dict, current_last_element, last_element = get_main_page_table_info(dcrawler)
    >>> search_table_dict, current_last_element, last_element = get_main_page_table_info(dcrawler, 10)
    """

    page_label = ''

    try:

        info_table = WebDriverWait(dcrawler,3).until(EC.presence_of_element_located((By.ID,"divProcesos")))

        # extract index of the current page
        page_index_labels = dcrawler.find_element_by_xpath('//*[@id="divProcesos"]/table[2]/tbody/tr[1]/td').text.split()

        current_last_element = int(page_index_labels[4])
        last_element = int(page_index_labels[6])

        page_label = ' '.join(page_index_labels[:5])
        print(C+"[SCRAPER] Extranting table info for: {}".format(page_label))

        table_len = dcrawler.find_element_by_xpath('//*[@id="divProcesos"]/table[1]')

        table_rows = table_len.find_elements_by_tag_name("tr")
        swing_table_process_list = []
        table_headers = []

        row_index = 0

        for row in table_rows:

            process_dict = {}

            if row_index == 0:
                for header in row.find_elements_by_tag_name("td"):
                    table_headers.append(header.text)

            else:

                col_index = 0
                resolucion_id = ''

                for cell in row.find_elements_by_tag_name("td"):
                    if col_index == 0:

                        if cell.text not in process_dict:

                            resolucion_id = cell.text
                            process_dict[table_headers[col_index]] =  resolucion_id
                            process_dict['link'] = cell.find_element_by_tag_name("a").get_attribute("href")

                    else:
                        col_name = table_headers[col_index]

                        if col_name not in process_dict:
                            process_dict[col_name] = cell.text

                    col_index +=1

                swing_table_process_list.append(process_dict)

                if row_index == n_retrieve:
                    break

            row_index +=1

        print(G+"[SCRAPER] Information in table for {} retrieved succesfully.".format(
            page_label))
        # print(swing_table_process_list)


        return swing_table_process_list, current_last_element, last_element


    except Exception as e:

        # # LOG area
        entry_message = "[SCRAPER] Failed to retrieve information in index_table for {}\nError message: {}".format(
            page_label, e)
        #print(R+entry_message)

        db_error_connection = wpsedb.conect_to_dscp_db(['localhost', 'wecpsie', 'user', 'pass'])

        wpsedb.add_entry_log(db_error_connection, '[SCRAPER]', '[SCRAPER]','', 'error', 'index', 'extrc',today, entry_message)

        wpsedb.disconnect_db(db_error_connection)

        print(R+"[SCRAPER] Failed to retrieve information in index_table for {}\nError message: {}".format(
            page_label, e))
        #print(e)


def extract_SIE_lv1(infotable):
    """
    Information scraper for tables "Descripción del Proceso de Contratación" and "Fechas de Control" in:
    Información Proceso Contratacion. This function is classified as Level_1.

    Parameters
    ----------
    infotable : WebObjectInstance
        Web object that represents the table on which all the
        information in this page is displayed, including all visible sections.

    Returns
    -------
    info_SIE_dict : dict,
        A dictionary containing the information scrapped from the two table mentioned.
        The dictionary has the following example structure:

            {
             'Entidad':'Enti',
             'Objeto':'Desc1',
             'Código':'SIE-MREMH-001-2021',
             'Compra':'tipo1',
             'presupuesto':'Pres',
             'Tipo':'Estatus',
             'Plazo':'Estatus',
             'Funcionario:'Funci',
             'Estado':'Estatus',
             'Fecha_publicacion'2021-10-10 13:00:00',
             'Fecha_puja'2021-10-10 13:00:00',
             'Fecha_adjudicacion'2021-10-10 13:00:00'
             'Invitacion: https://link/invitaciones',
             'Contrato: https://link/contrato}

    autoridades_list : list,
        A list containing various dictionaries each one with the information scrapped from the table Autoridades.
        Each dictionary has the following example structure:

            {
             'Cedula':'172568566',
             'Nombre':'Nombre',
             'Cargo':'Autoridad',
             'Origen':'Nacional'}

    comision_list : list,
        A list containing various dictionaries each one with the information scrapped from the table Comision Tecnica if exist.
        Each dictionary has the following example structure:
            {
             'Cedula':'1726859645',
             'Nombre':'Nombre',
             'Funcion':'Gerente'}

    Notes
    -----

    Please refer to the documentation and implementation of the function *save_SIE_info* in the database module
    to see which fields the dictionary might not contain.

    Any error, warning, info encountered during the function operations will be logged in WECPSIE database.

    Examples
    --------
    >>> from selenium import webdriver
    >>> import st_scraper_wecpsie as dextract
    >>> pcrawler = webdriver.Chrome(Driver_PATH)
    >>> infotable = WebDriverWait(pcrawler, 5).until(EC.presence_of_element_located((By.ID, 'ladoDer')))
    >>> sie_dict,list_autoridades, list_comision = dextract.extract_SIE_lv1(infotable)
    """

    info_SIE_dict = {}
    autoridades_list = []
    comision_list = []
    titles = ["Entidad","Objeto","Codigo","Compra","Presupuesto","Tipo","Plazo","Funcionario","Estado"]
    titles_table2 = ["Cedula","Nombre","Cargo","Origen"]
    titles_table3 = ["Cedula","Nombre","Funcion"]
    excluidos = ["Tipo de Contratación:","Forma de Pago:","Autoridades:","Miembros Comisión Técnica","Vigencia de Oferta:","Descripción:","Variación mínima de la Oferta durante la Puja:","Estado en el cual finalizó el Proceso"]

    tabla = infotable.find_element_by_id('one-column-emphasis')

    filas = tabla.find_elements_by_tag_name("tr")
    count = 0
    for row in filas:

        try:
            titulo = row.find_element_by_tag_name("th")
            descripcion = row.find_element_by_tag_name('td')
            if titulo.text not in excluidos:
                if titulo.text != "Comisión Técnica:":
                    if titulo.text == 'Presupuesto Referencial Total (Sin Iva):':
                        if descripcion.text != 'NO DISPONIBLE':
                            presupuesto_str = descripcion.text.split()[1]
                            presupuesto_value = float(re.sub(',', '', presupuesto_str))
                            info_SIE_dict[titles[count]] = presupuesto_value
                            count +=1
                        else:
                            info_SIE_dict[titles[count]] = 0.0
                            count += 1
                    else:
                        #print(descripcion.text)
                        info_SIE_dict[titles[count]] = descripcion.text
                        count+=1
        except Exception as e:
            entry_message = "[SCRAPER] Failed to retrieve information in Information table\nError message: {}".format(e)
            #print(R + entry_message)

            db_error_connection = wpsedb.conect_to_dscp_db(['localhost', 'wecpsie', 'user', 'pass'])

            wpsedb.add_entry_log(db_error_connection, '[SCRAPER]', '[SCRAPER]', '', 'error', 'scraper', 'extrc', today,
                                 entry_message)

            wpsedb.disconnect_db(db_error_connection)

            print(R + "[SCRAPER] Failed to retrieve information in Information table\nError message: {}".format(e))
            pass

    #Obtencion de datos tabla Autoridades y Comision tecnica
    try:
        tabla_fun_comision =  infotable.find_elements_by_id('rounded-corner')
        aux = len(tabla_fun_comision)
        counter = 0
        if aux < 2:

            tabla_funcionarios = tabla_fun_comision[0]
            filas_fun = tabla_funcionarios.find_elements_by_tag_name("tr")
            for row in filas_fun:
                if counter != 0:
                    colum = row.find_elements_by_tag_name("td")
                    autoridades_dict = {}
                    for i,colums in enumerate(colum):
                        autoridades_dict[titles_table2[i]] = colums.text
                    autoridades_list.append(autoridades_dict)
                    counter+=1
                else:
                    counter += 1
                    pass
        elif aux == 2:

            tabla_funcionarios = tabla_fun_comision[0]
            tabla_comision = tabla_fun_comision[1]
            filas_fun = tabla_funcionarios.find_elements_by_tag_name("tr")
            filas_com = tabla_comision.find_elements_by_tag_name("tr")
            for row in filas_fun:
                if counter != 0:
                    colum = row.find_elements_by_tag_name("td")
                    autoridades_dict = {}
                    for i, colums in enumerate(colum):
                        autoridades_dict[titles_table2[i]] = colums.text
                    autoridades_list.append(autoridades_dict)
                    counter+=1
                else:
                    counter += 1
                    pass
            counter = 0
            for row in filas_com:
                if counter!= 0:
                    colum = row.find_elements_by_tag_name("td")
                    comision_dict = {}
                    for i, colums in enumerate(colum):
                        comision_dict[titles_table3[i]] = colums.text
                    comision_list.append(comision_dict)
                    counter += 1
                else:
                    counter += 1
                    pass

    except Exception as e:
        entry_message = "[SCRAPER] Failed to retrieve information in Information table\nError message: {}".format(e)
        #print(R + entry_message)

        db_error_connection = wpsedb.conect_to_dscp_db(['localhost', 'wecpsie', 'user', 'pass'])

        wpsedb.add_entry_log(db_error_connection, '[SCRAPER]', '[SCRAPER]', '', 'error', 'scraper', 'extrc', today,
                             entry_message)

        wpsedb.disconnect_db(db_error_connection)
        print(R+"[SCRAPER] Information in table retrieved fail.")
        pass


    #Obtencion de fechas

    try:
        tab_fechas = infotable.find_element_by_id("tab2")
        tab_fechas.click()
        time.sleep(2)

        fecha_publicacion = infotable.find_element_by_xpath('//*[@id="one-column-emphasis"]/tbody/tr[1]/td[1]')
        fecha_subasta = infotable.find_element_by_xpath('//*[@id="one-column-emphasis"]/tbody/tr[8]/td[1]')
        fecha_adjudicacion = infotable.find_element_by_xpath('//*[@id="one-column-emphasis"]/tbody/tr[10]/td[1]')

        info_SIE_dict["Fecha_publicacion"] = fecha_publicacion.text
        info_SIE_dict["Fecha_puja"] = fecha_subasta.text
        info_SIE_dict["Fecha_adjudicacion"] = fecha_adjudicacion.text
    except Exception as e:
        entry_message = "[SCRAPER] Failed to retrieve information in Information table\nError message: {}".format(e)
        #print(R + entry_message)

        db_error_connection = wpsedb.conect_to_dscp_db(['localhost', 'wecpsie', 'user', 'pass'])

        wpsedb.add_entry_log(db_error_connection, '[SCRAPER]', '[SCRAPER]', '', 'error', 'scraper', 'extrc', today,
                             entry_message)

        wpsedb.disconnect_db(db_error_connection)
        print(R+"[SCRAPER] Information in table retrieved fail.")
        pass

    #Obtencion de links extras

    try:

        info_SIE_dict["Invitacion"] = ""
        info_SIE_dict["Contrato"] = ""
        link_invitaciones = infotable.find_element_by_xpath('//*[@id="menu"]/li/ul/a').get_attribute("href")
        info_SIE_dict["Invitacion"] = link_invitaciones
        div_contrato = infotable.find_elements_by_xpath('//*[@id="menu"]')
        link_contrato = div_contrato[2].find_element_by_tag_name("a").get_attribute("href")
        info_SIE_dict["Contrato"] = link_contrato

    except  Exception as e:
        print(R+"[SCRAPER] Information in table retrieved fail No link to informacion contrato.")
        entry_message = "[SCRAPER] Failed to retrieve information in Information table\nError message: {}".format(e)
        #print(R + entry_message)

        db_error_connection = wpsedb.conect_to_dscp_db(['localhost', 'wecpsie', 'user', 'pass'])

        wpsedb.add_entry_log(db_error_connection, '[SCRAPER]', '[SCRAPER]', '', 'error', 'scraper', 'extrc', today,
                             entry_message)

        wpsedb.disconnect_db(db_error_connection)
        pass

    print(G+"[SCRAPER] Information in table retrieved succesfully.")

    # print(info_SIE_dict)
    # print(autoridades_list)
    # print(comision_list)

    return info_SIE_dict,autoridades_list,comision_list


#level2

def extract_SIE_lv2(infotable):
    """
    Information scraper for table "Informacion de Adjudicacion"
    This function is classified as Level_2.

    Parameters
    ----------
    infotable : WebObjectInstance
        Web object that represents the table on which all the
        information in this page is displayed, including all visible sections.

    Returns
    -------
    contrato_DIC : dict,
        A dictionary containing the information scrapped from the table mentioned.
        The dictionary has the following example structure:

            {
             'Ruc':'172689256001',
             'Nombre adjudicacion':'Nombre',
             'Fecha adjudicacion':'2020-06-20',
             'Monto':'62.03',
             'Administrador':'Nombre'
             }

    Notes
    -----

    Please refer to the documentation and implementation of the function *save_SIE_contrato_info* in the database module
    to see which fields the dictionary might not contain.
    Any error, warning, info encountered during the function operations will be logged in WECPSIE database.

    Examples
    --------
    >>> from selenium import webdriver
    >>> import st_scraper_wecpsie as dextract
    >>> pcrawler = webdriver.Chrome(Driver_PATH)
    >>> infotable = WebDriverWait(pcrawler, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="frminfoGeneralContrato"]/table')))
    >>> contratos_dict = dextract.extract_SIE_lv2(infotable)
    """

    contrato_DIC = {}

    try:
        ruc = infotable.find_element_by_xpath('//*[@id="frminfoGeneralContrato"]/table/tbody/tr[15]/td/table/tbody/tr[2]/td[2]')
        nombre = infotable.find_element_by_xpath('//*[@id="frminfoGeneralContrato"]/table/tbody/tr[15]/td/table/tbody/tr[2]/td[3]')
        fecha_adj = infotable.find_element_by_xpath('//*[@id="frminfoGeneralContrato"]/table/tbody/tr[15]/td/table/tbody/tr[2]/td[4]')
        monto = infotable.find_element_by_xpath('//*[@id="frminfoGeneralContrato"]/table/tbody/tr[15]/td/table/tbody/tr[2]/td[5]')
        admin = infotable.find_element_by_xpath('//*[@id="frminfoGeneralContrato"]/table/tbody/tr[18]/td/table/tbody/tr[3]/td[16]')

        monto_str = monto.text
        monto_value = float(re.sub(',', '', monto_str))


        contrato_DIC['Ruc'] = ruc.text
        contrato_DIC['Nombre'] = nombre.text
        contrato_DIC['Fecha_adj'] = fecha_adj.text
        contrato_DIC['Monto'] = monto_value
        contrato_DIC['Admin'] = admin.text
    except Exception as e:
        entry_message = "[SCRAPER] Failed to retrieve information in Contrato Information table\nError message: {}".format(e)
        print(R + entry_message)

        db_error_connection = wpsedb.conect_to_dscp_db(['localhost', 'wecpsie', 'user', 'pass'])

        wpsedb.add_entry_log(db_error_connection, '[SCRAPER]', '[SCRAPER]', '', 'error', 'scraper', 'extrc', today,
                             entry_message)

        wpsedb.disconnect_db(db_error_connection)


    #print(contrato_DIC)
    print(G+"[SCRAPER] Information in table retrieved succesfully.")
    return contrato_DIC


def extract_SIE_lv2_inv(infotable,dcrawler,n_inv=50):
    """
    Information scraper for table "Invitaciones a proveedores"
    This function is classified as Level_2.

    Parameters
    ----------
    infotable : WebObjectInstance
        Web object that represents the table on which all the
        information in this page is displayed, including all visible sections.

    dcrawler : webdriver instance to perform crawling operations.

    n_inv: int, default="50"
        The number of rows to retrieve from the current page search table. The crawler module
        passes this argument in order to retrieve a certain number of records in the table.

    Returns
    -------
     swing_table_invitation_list : list,
        A list containing various dictionarys with the information scrapped from the table mentioned.
        Each dictionary has the following example structure:

            {
             'Razon_social':'ABAD PARDO REYNALDO CESAR',
             'Fecha_invitacion':'2020-07-31 13:02',
             'Provincia':'GUAYAS - SIMON BOLIVAR',
             'Estado':' No Habilitado en RUP'
             }

    Notes
    -----

    Any error, warning, info encountered during the function operations will be logged in WECPSIE database.

    Examples
    --------
    >>> from selenium import webdriver
    >>> import st_scraper_wecpsie as dextract
    >>> pcrawler = webdriver.Chrome(Driver_PATH)
    >>> infotable = WebDriverWait(pcrawler, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="formPaginas"]/table')))
    >>> invitaciones_dict = dextract.extract_SIE_lv2_inv(infotable,pcrawler,50)
    """

    swing_table_invitation_list = []

    page_index_labels = dcrawler.find_element_by_xpath('//*[@id="divListaInv"]/table/tbody/tr[2]/td').text.split()

    current_last_element = int(page_index_labels[4])
    last_element = int(page_index_labels[6])

    page_label = ' '.join(page_index_labels[:5])
    #print(C + "[SCRAPER] Extranting table info for: {}".format(page_label))


    while current_last_element <= last_element:
        try:


            table_len = dcrawler.find_element_by_xpath('//*[@id="formPaginas"]/table')


            table_rows = table_len.find_elements_by_tag_name("tr")

            table_headers = []

            row_index = 0

            for row in table_rows:

                invitaciones_DIC = {}

                if row_index == 0:
                    counter = 0
                    for header in row.find_elements_by_tag_name("td"):
                        if counter != 0:
                            table_headers.append(header.text)
                        else:
                            counter+=1
                else:

                    col_index = 0

                    for cell in row.find_elements_by_tag_name("td"):
                        if col_index == 0:
                            pass
                        else:
                            col_name = table_headers[col_index-1]
                            if col_name not in invitaciones_DIC:
                                invitaciones_DIC[col_name] = cell.text

                        col_index +=1

                    swing_table_invitation_list.append(invitaciones_DIC)

                row_index +=1

            if current_last_element > n_inv:
                break
            if current_last_element == last_element:
                 break
            #Next page
            next_page = dcrawler.find_element_by_xpath('//*[@id="divListaInv"]/table/tbody/tr[1]/td[3]/a')

            next_page.click()

            # extract index of the current page
            page_index_labels = dcrawler.find_element_by_xpath('//*[@id="divListaInv"]/table/tbody/tr[2]/td').text.split()

            current_last_element = int(page_index_labels[4])
            last_element = int(page_index_labels[6])

            page_label = ' '.join(page_index_labels[:5])
            #print(C+"[SCRAPER] Extranting table info for: {}".format(page_label))

            # print(G+"[SCRAPER] Information in table for {} retrieved succesfully.".format(
            #     page_label))
            #print(swing_table_invitation_list)

        except Exception as e:

            # LOG area
            entry_message = "[SCRAPER] Failed to retrieve information in Invitation table\nError message: {}".format(e)
            #print(R + entry_message)

            db_error_connection = wpsedb.conect_to_dscp_db(['localhost', 'wecpsie', 'user', 'pass'])

            wpsedb.add_entry_log(db_error_connection, '[SCRAPER]', '[SCRAPER]', '', 'error', 'scraper', 'extrc', today,
                                 entry_message)

            wpsedb.disconnect_db(db_error_connection)

            print(R+"[SCRAPER] Failed to retrieve information in Invitation table {}\nError message: {}".format(e))
            #print(e)

    print(G+"[SCRAPER] Information in table Invitaciones a proveedores retrieved succesfully.")
    #print(swing_table_invitation_list)
    #print(len(swing_table_invitation_list))

    return swing_table_invitation_list




# main program, testing only
if __name__ == '__main__':


    driver = webdriver.Chrome(dutils.GDriver_PATH)
    #dd = driver.get("https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/PC/informacionProcesoContratacion2.cpe?idSoliCompra=Ob221JtHRXNXGMqxfHzL-fjsGTpYrKa8RFP8B22UKYg,")
    #dd = driver.get('https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/PC/informacionProcesoContratacion2.cpe?idSoliCompra=bgqAn1x_doMc1plEGfQyWwz44UbNkuQZbw5Pey7GCq0,')

    # dd = driver.get('https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/PC/informacionProcesoContratacion2.cpe?idSoliCompra=YSSRqurXwAj4o5L6TJYSev2_d1M0um8td8v9ILe_jt4,')
    #     # infotable = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, 'ladoDer')))
    #     # time.sleep(2)
    #     # extract_SIE_lv1(infotable)
    #     # driver.quit()

    #dd = driver.get('https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/EC/resumenContractual1.cpe?idSoliCompra=bbNmZrv2JzbHn-pID33beeYZ57v4QwtzQPow7MiHlrY,&cnt=928-hS6GSn6QbGyUV3hEORazuY6Gqs8Nu1cA2JY-Gus,&contratoId=JmI166t9rLTT8fqTPUgOB5HLAdD-bFiL3T1Hn1v2Hjc')
    dd = driver.get('https://www.compraspublicas.gob.ec/ProcesoContratacion/compras/IV/ReporteInvitaciones.cpe?solicitud=yA2AbIL142NPoOimM3387DHKV7hYvZvt51SJOEOzfWA,')
    infotable = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="formPaginas"]/table')))
    extract_SIE_lv2_inv(infotable,driver)

